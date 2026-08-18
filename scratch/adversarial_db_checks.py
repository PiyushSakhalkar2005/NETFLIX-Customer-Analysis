import sys
import os
import json

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.ai_backend.database import execute_read_query

print("==========================================================")
print("INDEPENDENT ADVERSARIAL DATABASE & DENOMINATOR AUDIT")
print("==========================================================")

# 1. Movie Duration Denominator & Stats
sql_movie_denom = """
SELECT 
    COUNT(*) as total_distinct_movies,
    COUNT(CASE WHEN duration LIKE '%min%' THEN 1 END) as movies_with_valid_duration,
    COUNT(CASE WHEN duration IS NULL OR duration NOT LIKE '%min%' THEN 1 END) as movies_with_missing_duration,
    ROUND(AVG(CASE WHEN duration LIKE '%min%' THEN CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) END), 2) as avg_duration_exact,
    ROUND(AVG(CASE WHEN duration LIKE '%min%' THEN CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) END), 1) as avg_duration_rounded,
    MIN(CASE WHEN duration LIKE '%min%' THEN CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) END) as min_duration,
    MAX(CASE WHEN duration LIKE '%min%' THEN CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) END) as max_duration
FROM silver.silver_titles
WHERE type = 'Movie';
"""
row_m_denom = execute_read_query(sql_movie_denom)[0]
print("\n--- 1. MOVIE DURATION DENOMINATOR ---")
print(row_m_denom)

# 2. Top 10 Longest Movies
sql_top10_longest = """
SELECT show_id, title, duration, release_year,
       CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) as duration_min
FROM silver.silver_titles
WHERE type = 'Movie' AND duration LIKE '%min%'
ORDER BY duration_min DESC
LIMIT 10;
"""
print("\n--- 2. TOP 10 LONGEST MOVIES ---")
for r in execute_read_query(sql_top10_longest):
    print(" ", dict(r))

# 3. Top 10 Shortest Movies (Non-zero)
sql_top10_shortest = """
SELECT show_id, title, duration, release_year,
       CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) as duration_min
FROM silver.silver_titles
WHERE type = 'Movie' AND duration LIKE '%min%' AND CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) > 0
ORDER BY duration_min ASC
LIMIT 10;
"""
print("\n--- 3. TOP 10 SHORTEST MOVIES (NON-ZERO) ---")
for r in execute_read_query(sql_top10_shortest):
    print(" ", dict(r))

# 4. TV Show Seasons Denominator & Stats
sql_tv_denom = """
SELECT 
    COUNT(*) as total_distinct_tv_shows,
    COUNT(CASE WHEN duration LIKE '%Season%' OR duration LIKE '%Seasons%' THEN 1 END) as tv_with_valid_seasons,
    COUNT(CASE WHEN duration IS NULL OR (duration NOT LIKE '%Season%' AND duration NOT LIKE '%Seasons%') THEN 1 END) as tv_with_missing_seasons,
    ROUND(AVG(CASE WHEN duration LIKE '%Season%' OR duration LIKE '%Seasons%' THEN CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) END), 2) as avg_seasons_exact,
    MIN(CASE WHEN duration LIKE '%Season%' OR duration LIKE '%Seasons%' THEN CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) END) as min_seasons,
    MAX(CASE WHEN duration LIKE '%Season%' OR duration LIKE '%Seasons%' THEN CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) END) as max_seasons
FROM silver.silver_titles
WHERE type = 'TV Show';
"""
row_tv_denom = execute_read_query(sql_tv_denom)[0]
print("\n--- 4. TV SHOW SEASONS DENOMINATOR ---")
print(row_tv_denom)

# 5. Country Denominators & Shares
sql_country_shares = """
SELECT 
    country_name,
    titles_count,
    ROUND((100.0 * titles_count / 8807.0), 2) as catalog_share,
    percentage_share as assignment_share
FROM metadata.v_kpi_country_distribution
ORDER BY titles_count DESC
LIMIT 5;
"""
print("\n--- 5. COUNTRY DENOMINATORS & SHARES ---")
for r in execute_read_query(sql_country_shares):
    print(" ", dict(r))
