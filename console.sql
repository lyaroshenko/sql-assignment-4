CREATE TABLE steam_games AS
SELECT *
FROM read_json('steam_games.json',
               columns={'metadata':'JSON','games':'JSON'},
               maximum_object_size=1000000000);

CREATE TABLE steam_reviews_raw AS
SELECT *
FROM read_json('steam_reviews.json',
               columns={'metadata':'JSON','reviews':'JSON'},
                maximum_object_size=1000000000);
DESCRIBE steam_games;
DESCRIBE steam_reviews;

CREATE TABLE flat_games AS
SELECT
    CAST(json_extract(g.value, '$.appid') AS BIGINT) AS appid,
    json_extract(g.value, '$.name_from_applist') AS name_from_applist,
    json_extract(g.value, '$.app_details.data') AS game_data
FROM steam_games,
     json_each(games) AS g;

CREATE TABLE steam_games_clean AS
SELECT
    appid,
    json_extract(game_data, '$.name') AS name,
    COALESCE(
            try_strptime(replace(json_extract(game_data, '$.release_date.date'), '"', ''), '%b %d, %Y'),
            try_strptime(replace(json_extract(game_data, '$.release_date.date'), '"', ''), '%Y-%m-%d'),
            try_strptime(replace(json_extract(game_data, '$.release_date.date'), '"', ''), '%Y')
    ) AS release_date,
    CAST(json_extract(game_data, '$.price_overview.final') AS DOUBLE) / 100 AS price,
    CAST(json_extract(game_data, '$.recommendations.total') AS INTEGER) AS review_count,
    json_extract(game_data, '$.genres') AS genres_json,
    json_extract(game_data, '$.categories') AS categories_json
FROM flat_games;

CREATE TABLE steam_reviews_clean AS
SELECT
    CAST(json_extract(r.value, '$.appid') AS BIGINT) AS appid,
    CAST(json_extract(r.value, '$.review_data.query_summary.total_reviews') AS INTEGER) AS review_count,
    CAST(json_extract(r.value, '$.review_data.query_summary.total_positive') AS INTEGER) AS positive_reviews,
    CAST(json_extract(r.value, '$.review_data.query_summary.total_negative') AS INTEGER) AS negative_reviews,
    json_extract(r.value, '$.review_data.query_summary.review_score_desc') AS review_score_desc
FROM steam_reviews_raw,
     json_each(reviews) AS r;

CREATE TABLE steam_genres AS
SELECT
    appid,
    json_extract(genre.value, '$.description') AS genre
FROM steam_games_clean,
     json_each(genres_json) AS genre;

CREATE TABLE steam_tags AS
SELECT
    appid,
    json_extract(tag.value, '$.description') AS tag
FROM steam_games_clean,
     json_each(categories_json) AS tag;


SELECT g.name, r.review_count
FROM steam_games_clean g
JOIN steam_reviews_clean r USING (appid)
ORDER BY r.review_count DESC
LIMIT 20;

SELECT EXTRACT(YEAR FROM release_date) AS year, COUNT(*) AS games
FROM steam_games_clean
WHERE release_date IS NOT NULL
GROUP BY year
ORDER BY year;

SELECT genre, AVG(price) AS avg_price
FROM steam_genres g
         JOIN steam_games_clean s USING (appid)
WHERE price IS NOT NULL
GROUP BY genre
ORDER BY avg_price DESC;

SELECT tag, COUNT(*) AS frequency
FROM steam_tags
GROUP BY tag
ORDER BY frequency DESC
    LIMIT 10;

SELECT EXTRACT(YEAR FROM g.release_date) AS year, AVG(r.review_count) AS avg_reviews
FROM steam_games_clean g
    JOIN steam_reviews_clean r USING (appid)
WHERE g.release_date IS NOT NULL
GROUP BY year
ORDER BY year;
