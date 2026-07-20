-- PostgreSQL DDL for Netflix Medallion Architecture - Gold Layer Tables (Star Schema)

-- Drop tables if they exist to support clean rebuilds
DROP TABLE IF EXISTS gold.fact_content CASCADE;
DROP TABLE IF EXISTS gold.dim_date CASCADE;
DROP TABLE IF EXISTS gold.dim_type CASCADE;
DROP TABLE IF EXISTS gold.dim_rating CASCADE;
DROP TABLE IF EXISTS gold.dim_genre CASCADE;
DROP TABLE IF EXISTS gold.dim_country CASCADE;
DROP TABLE IF EXISTS gold.dim_director CASCADE;
DROP TABLE IF EXISTS gold.dim_title CASCADE;

-- 1. Dimension Tables
CREATE TABLE gold.dim_title (
    title_key BIGINT PRIMARY KEY,
    show_id VARCHAR(50) NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    is_classic BOOLEAN,
    content_category VARCHAR(50)
);

CREATE TABLE gold.dim_director (
    director_key BIGINT PRIMARY KEY,
    director VARCHAR(255) NOT NULL UNIQUE
);

CREATE TABLE gold.dim_country (
    country_key BIGINT PRIMARY KEY,
    country VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE gold.dim_genre (
    genre_key BIGINT PRIMARY KEY,
    genre VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE gold.dim_rating (
    rating_key BIGINT PRIMARY KEY,
    rating VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE gold.dim_type (
    type_key BIGINT PRIMARY KEY,
    type VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE gold.dim_date (
    date_key INT PRIMARY KEY,
    date_added DATE,
    year INT,
    month INT,
    day INT,
    quarter INT,
    day_of_week INT
);

-- 2. Fact Table
CREATE TABLE gold.fact_content (
    title_key BIGINT REFERENCES gold.dim_title(title_key),
    director_key BIGINT REFERENCES gold.dim_director(director_key),
    country_key BIGINT REFERENCES gold.dim_country(country_key),
    genre_key BIGINT REFERENCES gold.dim_genre(genre_key),
    rating_key BIGINT REFERENCES gold.dim_rating(rating_key),
    type_key BIGINT REFERENCES gold.dim_type(type_key),
    date_key INT REFERENCES gold.dim_date(date_key),
    duration_minutes INT,
    season_count INT,
    release_year INT NOT NULL,
    content_age INT,
    batch_id VARCHAR(50),
    pipeline_run_id VARCHAR(50),
    ingestion_timestamp TIMESTAMP,
    PRIMARY KEY (title_key, director_key, country_key, genre_key, rating_key, type_key, date_key)
);

-- Indexes for dimensional query speeds
CREATE INDEX idx_fact_release_year ON gold.fact_content(release_year);
CREATE INDEX idx_fact_keys ON gold.fact_content(title_key, director_key, country_key, genre_key);
