"""
Movie analytics ETL pipeline.

Extracts MovieLens movies/ratings, enriches titles via the OMDb API,
transforms the data, and loads it into MySQL.

The load step is idempotent: re-running the script upserts rows instead of
creating duplicates. OMDb responses are cached on disk so later runs do not
repeat successful API calls.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import re
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus
from urllib.request import urlretrieve

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine

ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
DATASET_DIR = DATA_DIR / "ml-latest-small"
CACHE_PATH = DATA_DIR / "omdb_cache.json"
MOVIELENS_URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"

TITLE_YEAR_RE = re.compile(r"^(.*)\s+\((\d{4})\)\s*$")
N_A_VALUES = {"", "N/A", "NA", "null", "None"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("etl")


def get_config() -> dict[str, Any]:
    load_dotenv(ROOT / ".env")
    max_movies = os.getenv("OMDB_MAX_MOVIES", "").strip()
    return {
        "mysql_host": os.getenv("MYSQL_HOST", "localhost"),
        "mysql_port": int(os.getenv("MYSQL_PORT", "3306")),
        "mysql_user": os.getenv("MYSQL_USER", "root"),
        "mysql_password": os.getenv("MYSQL_PASSWORD", ""),
        "mysql_database": os.getenv("MYSQL_DATABASE", "movie_analytics"),
        "omdb_api_key": os.getenv("OMDB_API_KEY", "").strip(),
        "omdb_max_movies": int(max_movies) if max_movies else None,
    }


def mysql_engine(cfg: dict[str, Any], with_db: bool = True) -> Engine:
    db = f"/{cfg['mysql_database']}" if with_db else ""
    user = quote_plus(str(cfg["mysql_user"]))
    password = quote_plus(str(cfg["mysql_password"]))
    url = (
        f"mysql+pymysql://{user}:{password}"
        f"@{cfg['mysql_host']}:{cfg['mysql_port']}{db}?charset=utf8mb4"
    )
    return create_engine(url, pool_pre_ping=True)


def ensure_database(cfg: dict[str, Any]) -> None:
    """Create the database and tables from schema.sql if they do not exist."""
    schema_sql = (ROOT / "schema.sql").read_text(encoding="utf-8")
    server_engine = mysql_engine(cfg, with_db=False)
    with server_engine.begin() as conn:
        conn.execute(
            text(
                f"CREATE DATABASE IF NOT EXISTS `{cfg['mysql_database']}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        )

    statements = [
        stmt.strip()
        for stmt in schema_sql.split(";")
        if stmt.strip()
        and not stmt.strip().upper().startswith("CREATE DATABASE")
        and not stmt.strip().upper().startswith("USE ")
        and not stmt.strip().startswith("--")
    ]
    db_engine = mysql_engine(cfg, with_db=True)
    with db_engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))
    logger.info("MySQL schema is ready in database '%s'", cfg["mysql_database"])


def download_movielens() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    movies_csv = DATASET_DIR / "movies.csv"
    if movies_csv.exists():
        return DATASET_DIR

    zip_path = DATA_DIR / "ml-latest-small.zip"
    downloads_zip = Path.home() / "Downloads" / "ml-latest-small.zip"
    if not zip_path.exists() and downloads_zip.exists():
        logger.info("Using MovieLens zip already present in Downloads")
        zip_path.write_bytes(downloads_zip.read_bytes())
    if not zip_path.exists():
        logger.info("Downloading MovieLens small dataset...")
        urlretrieve(MOVIELENS_URL, zip_path)

    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(DATA_DIR)
    logger.info("Extracted dataset to %s", DATASET_DIR)
    return DATASET_DIR


def parse_title_year(raw_title: str) -> tuple[str, int | None]:
    match = TITLE_YEAR_RE.match(raw_title.strip())
    if not match:
        return raw_title.strip(), None
    return match.group(1).strip(), int(match.group(2))


def to_null(value: Any) -> Any:
    if value is None:
        return None
    text_value = str(value).strip()
    if text_value in N_A_VALUES:
        return None
    return text_value


def parse_runtime(value: Any) -> int | None:
    value = to_null(value)
    if value is None:
        return None
    match = re.search(r"(\d+)", value)
    return int(match.group(1)) if match else None


def parse_box_office(value: Any) -> int | None:
    value = to_null(value)
    if value is None:
        return None
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


def parse_float(value: Any) -> float | None:
    value = to_null(value)
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def parse_int(value: Any) -> int | None:
    value = to_null(value)
    if value is None:
        return None
    try:
        return int(float(value))
    except ValueError:
        return None


def load_cache() -> dict[str, Any]:
    if CACHE_PATH.exists():
        return json.loads(CACHE_PATH.read_text(encoding="utf-8"))
    return {}


def save_cache(cache: dict[str, Any]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(json.dumps(cache), encoding="utf-8")


def _omdb_error_message(payload: dict[str, Any]) -> str:
    return str(payload.get("Error") or "").strip()


def omdb_get(params: dict[str, str], retries: int = 3) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            response = requests.get("http://www.omdbapi.com/", params=params, timeout=20)
            if response.status_code in (401, 403):
                raise RuntimeError("OMDb API key is invalid or unauthorized")
            if response.status_code == 429:
                time.sleep(2 ** attempt)
                continue
            response.raise_for_status()
            payload = response.json()
            error = _omdb_error_message(payload).lower()
            if "invalid api key" in error or "invalid key" in error:
                raise RuntimeError("OMDb API key is invalid or unauthorized")
            if error == "request limit reached!":
                raise RuntimeError("OMDb daily request limit reached")
            return payload
        except RuntimeError:
            raise
        except Exception as exc:  # network / JSON issues
            last_error = exc
            time.sleep(1.5 * attempt)
    logger.warning("OMDb request failed after retries: %s", last_error)
    return {"Response": "False", "Error": str(last_error), "_transient": True}


def fetch_omdb(
    movie_id: int,
    title: str,
    year: int | None,
    imdb_id: str | None,
    api_key: str,
    cache: dict[str, Any],
) -> dict[str, Any]:
    cached = cache.get(str(movie_id))
    if cached is not None:
        return cached

    payload: dict[str, Any] = {"Response": "False"}
    attempts: list[dict[str, str]] = []
    if imdb_id:
        attempts.append({"i": imdb_id, "apikey": api_key})
    params = {"t": title, "apikey": api_key, "type": "movie"}
    if year:
        params["y"] = str(year)
    attempts.append(params)
    if year:
        attempts.append({"t": title, "apikey": api_key, "type": "movie"})

    for params in attempts:
        payload = omdb_get(params)
        if payload.get("Response") == "True":
            cache[str(movie_id)] = payload
            return payload
        # Do not persist transient HTTP/network failures; they should be retried later.
        if payload.get("_transient"):
            return payload
        time.sleep(0.2)

    cache[str(movie_id)] = payload
    return payload


def extract() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame | None]:
    dataset_dir = download_movielens()
    movies = pd.read_csv(dataset_dir / "movies.csv")
    ratings = pd.read_csv(dataset_dir / "ratings.csv")
    links_path = dataset_dir / "links.csv"
    links = pd.read_csv(links_path) if links_path.exists() else None
    logger.info(
        "Loaded %s movies and %s ratings%s",
        f"{len(movies):,}",
        f"{len(ratings):,}",
        " (links.csv present)" if links is not None else "",
    )
    return movies, ratings, links


def transform_movies(
    movies: pd.DataFrame,
    links: pd.DataFrame | None,
    cfg: dict[str, Any],
    skip_api: bool,
) -> tuple[pd.DataFrame, list[tuple[int, str]], list[tuple[int, str]], list[int]]:
    parsed = movies["title"].map(parse_title_year)
    movies = movies.copy()
    movies["clean_title"] = parsed.map(lambda x: x[0])
    movies["release_year"] = parsed.map(lambda x: x[1])
    movies["decade"] = movies["release_year"].map(
        lambda y: (int(y) // 10) * 10 if pd.notna(y) else None
    )

    imdb_map: dict[int, str] = {}
    if links is not None and "imdbId" in links.columns:
        for row in links.itertuples(index=False):
            if pd.notna(row.imdbId):
                imdb_map[int(row.movieId)] = f"tt{int(row.imdbId):07d}"

    cache = load_cache()
    api_key = cfg["omdb_api_key"]
    max_new_calls = cfg["omdb_max_movies"]
    movie_rows: list[dict[str, Any]] = []
    genre_pairs: list[tuple[int, str]] = []
    director_pairs: list[tuple[int, str]] = []
    # Only these movie_ids had OMDb applied this run (cache hit or HTTP).
    # Director junction rows are rebuilt for this set only, so --skip-api
    # and partial daily runs cannot wipe directors from earlier enrichments.
    attempted_ids: list[int] = []

    if skip_api:
        logger.info("Skipping OMDb calls (--skip-api)")
    elif not api_key:
        logger.warning("OMDB_API_KEY is empty; movies will load without API enrichment")
        skip_api = True
    elif max_new_calls is not None:
        logger.info(
            "OMDB_MAX_MOVIES=%s new uncached lookups this run; cache hits are free",
            max_new_calls,
        )

    processed = 0
    matched = 0
    new_calls = 0
    api_exhausted = skip_api

    for row in movies.itertuples(index=False):
        movie_id = int(row.movieId)
        imdb_id = imdb_map.get(movie_id)
        omdb: dict[str, Any] = {}
        cache_key = str(movie_id)
        was_cached = cache_key in cache
        should_call = False
        if not api_exhausted:
            if was_cached:
                should_call = True
            elif max_new_calls is None or new_calls < max_new_calls:
                should_call = True

        if should_call:
            try:
                omdb = fetch_omdb(
                    movie_id=movie_id,
                    title=row.clean_title,
                    year=int(row.release_year) if pd.notna(row.release_year) else None,
                    imdb_id=imdb_id,
                    api_key=api_key,
                    cache=cache,
                )
            except RuntimeError as exc:
                message = str(exc).lower()
                if "limit" not in message and "unauthorized" not in message and "invalid" not in message:
                    raise
                logger.warning(
                    "%s after %s new HTTP calls; remaining titles load without enrichment",
                    exc,
                    new_calls,
                )
                api_exhausted = True
                omdb = {}
                save_cache(cache)
            else:
                attempted_ids.append(movie_id)
                processed += 1
                if not was_cached:
                    new_calls += 1
                    time.sleep(0.15)
                if processed % 50 == 0:
                    save_cache(cache)
                    logger.info(
                        "OMDb progress: %s applied (%s new HTTP calls)",
                        processed,
                        new_calls,
                    )

        found = omdb.get("Response") == "True"
        if found:
            matched += 1
            imdb_id = to_null(omdb.get("imdbID")) or imdb_id

        movie_rows.append(
            {
                "movie_id": movie_id,
                "title": row.clean_title,
                "release_year": int(row.release_year) if pd.notna(row.release_year) else None,
                "decade": int(row.decade) if pd.notna(row.decade) else None,
                "imdb_id": imdb_id,
                "rated": to_null(omdb.get("Rated")) if found else None,
                "runtime_minutes": parse_runtime(omdb.get("Runtime")) if found else None,
                "plot": to_null(omdb.get("Plot")) if found else None,
                "language": to_null(omdb.get("Language")) if found else None,
                "country": to_null(omdb.get("Country")) if found else None,
                "box_office": parse_box_office(omdb.get("BoxOffice")) if found else None,
                "imdb_rating": parse_float(omdb.get("imdbRating")) if found else None,
                "metascore": parse_int(omdb.get("Metascore")) if found else None,
                "omdb_matched": 1 if found else 0,
            }
        )

        for genre in str(row.genres).split("|"):
            name = genre.strip()
            if name and name != "(no genres listed)":
                genre_pairs.append((movie_id, name))

        if found:
            for director in str(omdb.get("Director") or "").split(","):
                name = director.strip()
                if name and name not in N_A_VALUES:
                    director_pairs.append((movie_id, name))

    save_cache(cache)
    logger.info(
        "OMDb matches: %s / %s applied (%s new HTTP calls)",
        matched,
        processed,
        new_calls,
    )
    return pd.DataFrame(movie_rows), genre_pairs, director_pairs, attempted_ids


def transform_ratings(ratings: pd.DataFrame, valid_movie_ids: set[int]) -> pd.DataFrame:
    ratings = ratings.copy()
    ratings["user_id"] = ratings["userId"].astype(int)
    ratings["movie_id"] = ratings["movieId"].astype(int)
    ratings["rating"] = ratings["rating"].astype(float)
    ratings["rated_at"] = pd.to_datetime(ratings["timestamp"], unit="s", utc=True).dt.tz_localize(None)
    ratings = ratings[ratings["movie_id"].isin(valid_movie_ids)]
    ratings = ratings.drop_duplicates(subset=["user_id", "movie_id"], keep="last")
    return ratings[["user_id", "movie_id", "rating", "rated_at"]]


def mysql_value(value: Any) -> Any:
    """Convert pandas/numpy missing values to None so PyMySQL can bind them."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        if value.is_integer():
            return int(value)
    return value


def records_for_mysql(df: pd.DataFrame) -> list[dict[str, Any]]:
    records = df.to_dict(orient="records")
    for record in records:
        for key, value in record.items():
            record[key] = mysql_value(value)
    return records


def chunked(rows: list[Any], size: int = 2000):
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def load_all(
    engine: Engine,
    movies_df: pd.DataFrame,
    genre_pairs: list[tuple[int, str]],
    director_pairs: list[tuple[int, str]],
    ratings_df: pd.DataFrame,
    director_rebuild_ids: list[int],
) -> None:
    movie_records = records_for_mysql(movies_df)
    movie_sql = text(
        """
        INSERT INTO movies (
            movie_id, title, release_year, decade, imdb_id, rated,
            runtime_minutes, plot, language, country, box_office,
            imdb_rating, metascore, omdb_matched
        ) VALUES (
            :movie_id, :title, :release_year, :decade, :imdb_id, :rated,
            :runtime_minutes, :plot, :language, :country, :box_office,
            :imdb_rating, :metascore, :omdb_matched
        ) AS new
        ON DUPLICATE KEY UPDATE
            title = new.title,
            release_year = new.release_year,
            decade = new.decade,
            imdb_id = COALESCE(new.imdb_id, movies.imdb_id),
            rated = COALESCE(new.rated, movies.rated),
            runtime_minutes = COALESCE(new.runtime_minutes, movies.runtime_minutes),
            plot = COALESCE(new.plot, movies.plot),
            language = COALESCE(new.language, movies.language),
            country = COALESCE(new.country, movies.country),
            box_office = COALESCE(new.box_office, movies.box_office),
            imdb_rating = COALESCE(new.imdb_rating, movies.imdb_rating),
            metascore = COALESCE(new.metascore, movies.metascore),
            omdb_matched = GREATEST(new.omdb_matched, movies.omdb_matched)
        """
    )

    with engine.begin() as conn:
        for batch in chunked(movie_records, 1000):
            conn.execute(movie_sql, batch)
        logger.info("Upserted %s movies", len(movie_records))

        unique_genres = sorted({name for _, name in genre_pairs})
        if unique_genres:
            conn.execute(
                text("INSERT IGNORE INTO genres (genre_name) VALUES (:genre_name)"),
                [{"genre_name": name} for name in unique_genres],
            )
        unique_directors = sorted({name for _, name in director_pairs})
        if unique_directors:
            conn.execute(
                text("INSERT IGNORE INTO directors (director_name) VALUES (:director_name)"),
                [{"director_name": name} for name in unique_directors],
            )

        genre_ids = {
            row.genre_name: row.genre_id
            for row in conn.execute(text("SELECT genre_id, genre_name FROM genres"))
        }
        director_ids = {
            row.director_name: row.director_id
            for row in conn.execute(text("SELECT director_id, director_name FROM directors"))
        }

        movie_ids = [int(x) for x in movies_df["movie_id"].tolist()]
        delete_genres = text("DELETE FROM movie_genres WHERE movie_id IN :ids").bindparams(
            bindparam("ids", expanding=True)
        )
        for batch_ids in chunked(movie_ids, 1000):
            conn.execute(delete_genres, {"ids": batch_ids})

        # Genres always come from the CSV, so they are rebuilt for every movie.
        # Directors come from OMDb: only rebuild rows for titles we actually
        # enriched this run. --skip-api and mid-quota stops leave prior data.
        if director_rebuild_ids:
            delete_directors = text(
                "DELETE FROM movie_directors WHERE movie_id IN :ids"
            ).bindparams(bindparam("ids", expanding=True))
            for batch_ids in chunked(director_rebuild_ids, 1000):
                conn.execute(delete_directors, {"ids": batch_ids})
            logger.info(
                "Rebuilding movie_directors for %s enriched movies",
                len(director_rebuild_ids),
            )
        else:
            logger.info("No OMDb enrichment this run; leaving movie_directors unchanged")

        mg_rows = [
            {"movie_id": movie_id, "genre_id": genre_ids[name]}
            for movie_id, name in genre_pairs
            if name in genre_ids
        ]
        md_rows = [
            {"movie_id": movie_id, "director_id": director_ids[name]}
            for movie_id, name in director_pairs
            if name in director_ids
        ]
        if mg_rows:
            conn.execute(
                text(
                    "INSERT IGNORE INTO movie_genres (movie_id, genre_id) "
                    "VALUES (:movie_id, :genre_id)"
                ),
                mg_rows,
            )
        if md_rows:
            conn.execute(
                text(
                    "INSERT IGNORE INTO movie_directors (movie_id, director_id) "
                    "VALUES (:movie_id, :director_id)"
                ),
                md_rows,
            )

        rating_sql = text(
            """
            INSERT INTO ratings (user_id, movie_id, rating, rated_at)
            VALUES (:user_id, :movie_id, :rating, :rated_at) AS new
            ON DUPLICATE KEY UPDATE
                rating = new.rating,
                rated_at = new.rated_at
            """
        )
        rating_records = records_for_mysql(ratings_df)
        for batch in chunked(rating_records, 3000):
            conn.execute(rating_sql, batch)
        logger.info("Upserted %s ratings", len(rating_records))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="MovieLens + OMDb ETL into MySQL")
    parser.add_argument("--skip-api", action="store_true", help="Load CSVs only, skip OMDb")
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max new uncached OMDb HTTP calls this run (overrides OMDB_MAX_MOVIES)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    cfg = get_config()
    if args.limit is not None:
        cfg["omdb_max_movies"] = args.limit

    started = datetime.now(timezone.utc)
    logger.info("Starting ETL")
    ensure_database(cfg)
    movies, ratings, links = extract()
    movies_df, genre_pairs, director_pairs, attempted_ids = transform_movies(
        movies, links, cfg, skip_api=args.skip_api
    )
    ratings_df = transform_ratings(ratings, set(movies_df["movie_id"].tolist()))
    engine = mysql_engine(cfg, with_db=True)
    load_all(
        engine,
        movies_df,
        genre_pairs,
        director_pairs,
        ratings_df,
        director_rebuild_ids=attempted_ids,
    )
    elapsed = datetime.now(timezone.utc) - started
    logger.info("ETL finished in %s", elapsed)


if __name__ == "__main__":
    main()
