import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.ai_backend.database import execute_read_query

print("=== CHECKING DURATION IN GOLD & SILVER ===")

# Movie Duration on silver.silver_titles excluding 0 min anomalies if any
sql_silver_movies = """
SELECT 
    COUNT(*) as total_movies,
    ROUND(AVG(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)), 1) as avg_movie_duration_min,
    ROUND(AVG(CASE WHEN CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) > 0 THEN CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) END), 1) as avg_movie_duration_gt_zero
FROM silver.silver_titles
WHERE type = 'Movie' AND duration LIKE '%min%';
"""
print("Silver Movies Duration:", execute_read_query(sql_silver_movies)[0])

# TV Show Seasons on silver.silver_titles
sql_silver_tv = """
SELECT 
    COUNT(*) as total_tv_shows,
    ROUND(AVG(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)), 2) as avg_tv_seasons,
    MIN(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)) as min_seasons,
    MAX(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)) as max_seasons
FROM silver.silver_titles
WHERE type = 'TV Show' AND (duration LIKE '%Season%' OR duration LIKE '%Seasons%');
"""
print("Silver TV Shows Seasons:", execute_read_query(sql_silver_tv)[0])

# Gold Distinct Title Duration
sql_gold_distinct_movie_duration = """
SELECT 
    COUNT(DISTINCT f.title_key) as total_movies,
    ROUND(AVG(t_distinct.duration_minutes), 1) as avg_movie_duration_min
FROM gold.fact_content f
JOIN gold.dim_type t ON f.type_key = t.type_key
JOIN (
    SELECT DISTINCT title_key, duration_minutes
    FROM gold.fact_content
    WHERE duration_minutes > 0
) t_distinct ON f.title_key = t_distinct.title_key
WHERE t.type = 'Movie';
"""
print("Gold Distinct Title Movie Duration:", execute_read_query(sql_gold_distinct_movie_duration)[0])

# Gold Distinct TV Show Seasons
sql_gold_distinct_tv_seasons = """
SELECT 
    COUNT(DISTINCT f.title_key) as total_tv_shows,
    ROUND(AVG(t_distinct.season_count), 2) as avg_tv_seasons
FROM gold.fact_content f
JOIN gold.dim_type t ON f.type_key = t.type_key
JOIN (
    SELECT DISTINCT title_key, season_count
    FROM gold.fact_content
    WHERE season_count > 0
) t_distinct ON f.title_key = t_distinct.title_key
WHERE t.type = 'TV Show';
"""
print("Gold Distinct Title TV Seasons:", execute_read_query(sql_gold_distinct_tv_seasons)[0])
