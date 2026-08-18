-- Power BI Dashboard-Ready Views

-- 1. View for General Content Summary Metrics (Total Titles, Movies vs TV Shows, Average Age)
DROP VIEW IF EXISTS metadata.v_kpi_general_summary CASCADE;
CREATE VIEW metadata.v_kpi_general_summary AS
WITH distinct_titles AS (
    SELECT DISTINCT title_key, content_age
    FROM gold.fact_content
)
SELECT 
    COUNT(DISTINCT f.title_key) AS total_titles,
    COUNT(DISTINCT CASE WHEN t.type = 'Movie' THEN f.title_key END) AS total_movies,
    COUNT(DISTINCT CASE WHEN t.type = 'TV Show' THEN f.title_key END) AS total_tv_shows,
    ROUND((SELECT AVG(content_age) FROM distinct_titles), 1) AS average_content_age
FROM gold.fact_content f
JOIN gold.dim_type t
    ON f.type_key = t.type_key;



-- 2. View for Content by Country (Top Countries KPI Map/Bar)
DROP VIEW IF EXISTS metadata.v_kpi_country_distribution CASCADE;
CREATE VIEW metadata.v_kpi_country_distribution AS
SELECT 
    c.country AS country_name,
    COUNT(DISTINCT f.title_key) AS titles_count,
    ROUND(100.0 * COUNT(DISTINCT f.title_key) / SUM(COUNT(DISTINCT f.title_key)) OVER(), 2) AS percentage_share
FROM gold.fact_content f
JOIN gold.dim_country c ON f.country_key = c.country_key
WHERE c.country <> 'Unknown Country'
GROUP BY c.country;

-- 3. View for Content by Genre (Top Genres KPI Treemap)
DROP VIEW IF EXISTS metadata.v_kpi_genre_distribution CASCADE;
CREATE VIEW metadata.v_kpi_genre_distribution AS
SELECT 
    g.genre AS genre_name,
    COUNT(DISTINCT f.title_key) AS titles_count
FROM gold.fact_content f
JOIN gold.dim_genre g ON f.genre_key = g.genre_key
WHERE g.genre <> 'Unknown'
GROUP BY g.genre;

-- 4. View for Top Directors
DROP VIEW IF EXISTS metadata.v_kpi_top_directors CASCADE;
CREATE VIEW metadata.v_kpi_top_directors AS
SELECT 
    d.director AS director_name,
    COUNT(DISTINCT f.title_key) AS titles_count
FROM gold.fact_content f
JOIN gold.dim_director d ON f.director_key = d.director_key
WHERE d.director <> 'Unknown Director'
GROUP BY d.director;

-- 5. View for Rating Distribution KPI Chart
DROP VIEW IF EXISTS metadata.v_kpi_rating_distribution CASCADE;
CREATE VIEW metadata.v_kpi_rating_distribution AS
SELECT 
    r.rating AS rating_name,
    COUNT(DISTINCT f.title_key) AS titles_count
FROM gold.fact_content f
JOIN gold.dim_rating r ON f.rating_key = r.rating_key
GROUP BY r.rating;

-- 6. View for Content Releases trend by Year (Releases by Year KPI Line Chart)
DROP VIEW IF EXISTS metadata.v_kpi_release_trends CASCADE;
CREATE VIEW metadata.v_kpi_release_trends AS
SELECT 
    f.release_year,
    t.type AS type_name,
    COUNT(DISTINCT f.title_key) AS releases_count
FROM gold.fact_content f
JOIN gold.dim_type t ON f.type_key = t.type_key
GROUP BY f.release_year, t.type;
