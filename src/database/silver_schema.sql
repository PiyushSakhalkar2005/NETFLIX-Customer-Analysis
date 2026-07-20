-- PostgreSQL DDL for Netflix Medallion Architecture - Silver Layer Tables

-- Drop tables if they exist to support clean deployment rebuilds
DROP TABLE IF EXISTS silver.silver_cast CASCADE;
DROP TABLE IF EXISTS silver.silver_directors CASCADE;
DROP TABLE IF EXISTS silver.silver_genres CASCADE;
DROP TABLE IF EXISTS silver.silver_country CASCADE;
DROP TABLE IF EXISTS silver.silver_titles CASCADE;

-- 1. Fact/Dimensional Titles Table
CREATE TABLE silver.silver_titles (
    show_id VARCHAR(50) PRIMARY KEY,
    type VARCHAR(20) NOT NULL,
    title VARCHAR(255) NOT NULL,
    date_added DATE,
    release_year INT NOT NULL,
    rating VARCHAR(20),
    duration VARCHAR(50),
    description TEXT,
    content_age INT,
    duration_minutes INT,
    season_count INT,
    is_recent_release BOOLEAN,
    batch_id VARCHAR(50),
    pipeline_run_id VARCHAR(50),
    ingestion_timestamp TIMESTAMP
);

-- Indexing for BI querying speed
CREATE INDEX idx_silver_titles_year_type ON silver.silver_titles(release_year, type);

-- 2. Normalized Countries Map
CREATE TABLE silver.silver_country (
    show_id VARCHAR(50) REFERENCES silver.silver_titles(show_id) ON DELETE CASCADE,
    country VARCHAR(100) NOT NULL,
    PRIMARY KEY (show_id, country)
);

CREATE INDEX idx_silver_country_name ON silver.silver_country(country);

-- 3. Normalized Genres (listed_in) Map
CREATE TABLE silver.silver_genres (
    show_id VARCHAR(50) REFERENCES silver.silver_titles(show_id) ON DELETE CASCADE,
    genre VARCHAR(100) NOT NULL,
    PRIMARY KEY (show_id, genre)
);

CREATE INDEX idx_silver_genres_name ON silver.silver_genres(genre);

-- 4. Normalized Directors Map
CREATE TABLE silver.silver_directors (
    show_id VARCHAR(50) REFERENCES silver.silver_titles(show_id) ON DELETE CASCADE,
    director VARCHAR(255) NOT NULL,
    PRIMARY KEY (show_id, director)
);

-- 5. Normalized Cast Map
CREATE TABLE silver.silver_cast (
    show_id VARCHAR(50) REFERENCES silver.silver_titles(show_id) ON DELETE CASCADE,
    cast_member VARCHAR(255) NOT NULL,
    PRIMARY KEY (show_id, cast_member)
);
