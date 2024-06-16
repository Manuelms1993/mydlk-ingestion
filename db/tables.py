
create_raw_films_schema = """
CREATE TABLE IF NOT EXISTS raw.films (
    id STRING,
    title STRING,
    type STRING,
    release_year INT,
    age_certification STRING,
    runtime INT,
    genres STRING,
    production_countries STRING,
    seasons INT
)
STORED AS PARQUET;
"""

create_raw_imdb_schema = """
CREATE TABLE IF NOT EXISTS raw.imdb (
    id STRING,
    title STRING,
    imdb_score FLOAT,
    imdb_votes INT,
    tmdb_popularity STRING,
    tmdb_score FLOAT
)
STORED AS PARQUET;
"""