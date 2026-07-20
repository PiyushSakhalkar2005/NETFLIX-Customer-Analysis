-- Analytical Queries for Netflix Medallion Architecture - Business Layer

-- 1. KPI Summary (Total count, ratios, average durations)
SELECT 
    COUNT(*) as total_content,
    SUM(CASE WHEN type = 'Movie' THEN 1 ELSE 0 END) as total_movies,
    SUM(CASE WHEN type = 'TV Show' THEN 1 ELSE 0 END) as total_tv_shows,
    ROUND(AVG(duration_minutes), 2) as avg_movie_duration_mins,
    ROUND(AVG(season_count), 2) as avg_tv_seasons
FROM silver.silver_titles;

-- 2. Running Total of releases by release year
SELECT 
    release_year,
    COUNT(*) as yearly_releases,
    SUM(COUNT(*)) OVER (ORDER BY release_year) as running_total_releases
FROM silver.silver_titles
GROUP BY release_year
ORDER BY release_year;

-- 3. Top 3 Genres by country (Window Dense Rank)
WITH genre_country_counts AS (
    SELECT 
        c.country,
        g.genre,
        COUNT(*) as genre_count
    FROM silver.silver_titles t
    JOIN silver.silver_country c ON t.show_id = c.show_id
    JOIN silver.silver_genres g ON t.show_id = g.show_id
    GROUP BY c.country, g.genre
),
ranked_genre_country AS (
    SELECT 
        country,
        genre,
        genre_count,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY genre_count DESC) as genre_rank
    FROM genre_country_counts
)
SELECT 
    country,
    genre,
    genre_count,
    genre_rank
FROM ranked_genre_country
WHERE genre_rank <= 3
ORDER BY country ASC, genre_rank ASC;

-- 4. Director rankings by total titles (Window Row Number)
WITH director_counts AS (
    SELECT 
        director,
        COUNT(*) as title_count
    FROM silver.silver_directors
    WHERE director <> 'Unknown Director'
    GROUP BY director
)
SELECT 
    director,
    title_count,
    ROW_NUMBER() OVER (ORDER BY title_count DESC, director ASC) as director_rank
FROM director_counts
ORDER BY director_rank;
