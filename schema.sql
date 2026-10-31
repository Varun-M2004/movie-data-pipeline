-- Movie analytics schema (MySQL 8+)
-- Normalized model: movies, genres, directors, ratings, and junction tables.

CREATE DATABASE IF NOT EXISTS movie_analytics
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE movie_analytics;

CREATE TABLE IF NOT EXISTS movies (
    movie_id         INT           NOT NULL,
    title            VARCHAR(512)  NOT NULL,
    release_year     SMALLINT      NULL,
    decade           SMALLINT      NULL,
    imdb_id          VARCHAR(20)   NULL,
    rated            VARCHAR(32)   NULL,
    runtime_minutes  INT           NULL,
    plot             TEXT          NULL,
    language         VARCHAR(255)  NULL,
    country          VARCHAR(255)  NULL,
    box_office       BIGINT        NULL,
    imdb_rating      DECIMAL(3, 1) NULL,
    metascore        SMALLINT      NULL,
    omdb_matched     TINYINT(1)    NOT NULL DEFAULT 0,
    updated_at       TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP
                                   ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (movie_id),
    KEY idx_movies_year (release_year),
    KEY idx_movies_decade (decade),
    KEY idx_movies_imdb (imdb_id)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS genres (
    genre_id    INT         NOT NULL AUTO_INCREMENT,
    genre_name  VARCHAR(64) NOT NULL,
    PRIMARY KEY (genre_id),
    UNIQUE KEY uq_genre_name (genre_name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id  INT NOT NULL,
    genre_id  INT NOT NULL,
    PRIMARY KEY (movie_id, genre_id),
    KEY idx_mg_genre (genre_id),
    CONSTRAINT fk_mg_movie FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_mg_genre FOREIGN KEY (genre_id) REFERENCES genres (genre_id)
        ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS directors (
    director_id    INT          NOT NULL AUTO_INCREMENT,
    director_name  VARCHAR(255) NOT NULL,
    PRIMARY KEY (director_id),
    UNIQUE KEY uq_director_name (director_name)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id     INT NOT NULL,
    director_id  INT NOT NULL,
    PRIMARY KEY (movie_id, director_id),
    KEY idx_md_director (director_id),
    CONSTRAINT fk_md_movie FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_md_director FOREIGN KEY (director_id) REFERENCES directors (director_id)
        ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS ratings (
    user_id   INT           NOT NULL,
    movie_id  INT           NOT NULL,
    rating    DECIMAL(2, 1) NOT NULL,
    rated_at  DATETIME      NOT NULL,
    PRIMARY KEY (user_id, movie_id),
    KEY idx_ratings_movie (movie_id),
    CONSTRAINT fk_ratings_movie FOREIGN KEY (movie_id) REFERENCES movies (movie_id)
        ON DELETE CASCADE
) ENGINE=InnoDB;
