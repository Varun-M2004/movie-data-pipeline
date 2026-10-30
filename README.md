# Movie Data Pipeline (tsworks Data Engineering Assignment)

Python ETL that ingests the MovieLens small dataset, enriches each title with the OMDb API, and loads a normalized MySQL schema for analytics.

```
Extract (CSV + OMDb) → Transform (clean, parse genres/year/decade) → Load (MySQL upserts)
```

## Project structure

```
.
├── etl.py              # extract, transform, load
├── schema.sql          # MySQL DDL
├── queries.sql         # answers to the four analytical questions
├── requirements.txt
├── .env.example
└── README.md
```

## Data model

| Table | Role |
|---|---|
| `movies` | One row per MovieLens film, plus OMDb fields (`director` is normalized out) and a `decade` column |
| `genres` / `movie_genres` | Many-to-many: a film like *Toy Story* is Animation, Comedy, and Adventure |
| `directors` / `movie_directors` | Many-to-many: OMDb can return several names |
| `ratings` | Grain is `(user_id, movie_id)` |

Foreign keys keep ratings and junction rows attached to a real movie. Ratings are not stored on `movies` so averages always reflect the current fact table.

**Bonus feature:** `movies.decade` is derived from the year in the MovieLens title (`1995` → `1990`). Pipe-separated `genres` are split into rows instead of left as a string.

## Prerequisites

- Python 3.10+
- MySQL 8+ running locally (this project does **not** use SQLite)
- A free OMDb API key from [omdbapi.com](http://www.omdbapi.com/apikey.aspx)

Create a MySQL user/database you can write to, or let `etl.py` create `movie_analytics` for the configured user.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
```

Edit `.env`:

```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=movie_analytics
OMDB_API_KEY=your_omdb_api_key
```

Optional: set `OMDB_MAX_MOVIES=200` while testing. The free OMDb plan allows 1,000 calls per day; MovieLens small has ~9,700 titles. Responses are cached in `data/omdb_cache.json`, so later runs only call the API for titles not yet cached. Re-run `python etl.py` on later days to finish enrichment.

If the API key is invalid or the daily quota is exhausted, the pipeline still loads every MovieLens row and keeps previously cached matches.

## Run

```bash
python etl.py
```

Useful flags:

```bash
python etl.py --skip-api          # load movies.csv + ratings.csv only
python etl.py --limit 0           # apply the local OMDb cache, no new HTTP calls
python etl.py --limit 100         # up to 100 new uncached OMDb lookups this run
```

The script will:

1. Create the database/tables from `schema.sql` if needed
2. Download (or reuse) `ml-latest-small`
3. Call OMDb by IMDb id from `links.csv` when present, then fall back to title + year
4. Upsert movies, genres, directors, and ratings

Apply the analytics queries:

```bash
mysql -u root -p movie_analytics < queries.sql
```

## Verified results (this workspace)

After a full CSV load plus **1,100** OMDb-enriched titles:

| Check | Result |
|---|---|
| Q1 highest average rating (≥ 50 votes) | *The Shawshank Redemption* (1994), 4.429 from 317 ratings |
| Q2 top genres by average rating | Film-Noir, War, Documentary, Crime, Drama |
| Q3 director with the most movies | Alfred Hitchcock (12 titles among currently enriched films) |
| Q4 average rating by year | One row per `release_year` from 1902 onward |
| Warehouse counts | 9,742 movies, 100,836 ratings, 19 genres |

Q3 grows as more OMDb rows are cached. Q1, Q2, and Q4 use MovieLens ratings, so they are complete without full API coverage.

## Design choices

- **MySQL 8** as requested, via SQLAlchemy + PyMySQL.
- **Idempotent loads** use `INSERT ... ON DUPLICATE KEY UPDATE`. Junction tables are rebuilt per movie so genre/director lists stay accurate after a re-run.
- **Title matching:** MovieLens titles look like `Toy Story (1995)`. The year is stripped for the OMDb `t` parameter. If `links.csv` exists, the IMDb id (`tt0114709`) is tried first because it is more reliable than fuzzy titles.
- **Missing API rows:** unmatched movies still load; `omdb_matched = 0` and director/plot/box office stay `NULL`. "Movie not found" responses are cached. Transient HTTP errors and invalid-key / quota errors are **not** cached, and the run continues loading CSVs.
- **Average-rating queries** require at least 50 ratings. Otherwise a one-vote 5.0 would win.

## Assumptions

- The year in parentheses at the end of a MovieLens title is the release year.
- `(no genres listed)` is treated as no genre, not a real category.
- Box office strings such as `$292,576,195` are stored as integers (USD, no inflation adjustment).
- Director names are split on commas exactly as OMDb returns them.

## Challenges

| Issue | Approach |
|---|---|
| OMDb title mismatches (`Seven (a.k.a. Se7en) (1995)`) | Prefer IMDb id; retry without year; keep the CSV row if still missing |
| 1,000 API calls/day | Disk cache + optional `OMDB_MAX_MOVIES` / `--limit`; resume on later days |
| Invalid / unauthorized OMDb key | Stop new HTTP calls; keep CSV load and existing cache |
| Duplicate ETL runs | Natural keys + upserts; rebuild `movie_genres` / `movie_directors` |
| Sparse high ratings | `HAVING COUNT(*) >= 50` in Q1 and Q2 |

## How this would scale in production

- Land raw CSVs in object storage and run Spark/dbt instead of a single Python process.
- Replace per-title HTTP calls with a bulk metadata source or an async worker queue with rate limits.
- Partition `ratings` by `rated_at` (or `movie_id` hash) and add a nightly aggregate table for average rating / genre stats.
- Add tests around title parsing, OMDb `N/A` handling, and upsert idempotency.
- Track pipeline runs (Airflow/Dagster) with freshness and row-count checks.

## Presentation notes

Be ready to walk through: `schema.sql` relationships, the extract/transform/load functions in `etl.py`, unmatched OMDb titles, and the four queries in `queries.sql`.
