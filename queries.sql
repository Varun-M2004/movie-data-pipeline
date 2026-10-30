-- Analytical queries for the movie analytics warehouse.
-- A movie with a single 5.0 rating would otherwise dominate averages,
-- so Q1 and Q2 require at least 50 ratings.

USE movie_analytics;

-- Q1. Which movie has the highest average rating?
SELECT
    m.movie_id,
    m.title,
    m.release_year,
    ROUND(AVG(r.rating), 3) AS avg_rating,
    COUNT(*) AS rating_count
FROM movies m
JOIN ratings r ON r.movie_id = m.movie_id
GROUP BY m.movie_id, m.title, m.release_year
HAVING COUNT(*) >= 50
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 1;


-- Q2. What are the top 5 movie genres that have the highest average rating?
SELECT
    g.genre_name,
    ROUND(AVG(r.rating), 3) AS avg_rating,
    COUNT(*) AS rating_count,
    COUNT(DISTINCT m.movie_id) AS movie_count
FROM genres g
JOIN movie_genres mg ON mg.genre_id = g.genre_id
JOIN movies m ON m.movie_id = mg.movie_id
JOIN ratings r ON r.movie_id = m.movie_id
GROUP BY g.genre_id, g.genre_name
HAVING COUNT(*) >= 50
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 5;


-- Q3. Who is the director with the most movies in this dataset?
-- Directors come from OMDb, so unmatched titles are excluded.
SELECT
    d.director_name,
    COUNT(*) AS movie_count
FROM directors d
JOIN movie_directors md ON md.director_id = d.director_id
GROUP BY d.director_id, d.director_name
ORDER BY movie_count DESC, d.director_name
LIMIT 1;


-- Q4. What is the average rating of movies released each year?
SELECT
    m.release_year,
    ROUND(AVG(r.rating), 3) AS avg_rating,
    COUNT(*) AS rating_count,
    COUNT(DISTINCT m.movie_id) AS movie_count
FROM movies m
JOIN ratings r ON r.movie_id = m.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year;
