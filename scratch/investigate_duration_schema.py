import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.ai_backend.database import execute_read_query, get_database_schema_info

print("==========================================================")
print("INVESTIGATION: DURATION COLUMN & METRIC LOCATION")
print("==========================================================")

# 1. Inspect silver.silver_titles schema and sample duration values
sql_silver_duration = """
SELECT duration, type, COUNT(*) 
FROM silver.silver_titles 
GROUP BY duration, type 
LIMIT 20;
"""
print("\n--- 1. SAMPLE DURATION VALUES IN silver.silver_titles ---")
for r in execute_read_query(sql_silver_duration):
    print(" ", dict(r))

# 2. Inspect gold.fact_content columns containing duration
sql_gold_fact_cols = """
SELECT column_name, data_type 
FROM information_schema.columns 
WHERE table_schema = 'gold' AND table_name = 'fact_content';
"""
print("\n--- 2. COLUMNS IN gold.fact_content ---")
for r in execute_read_query(sql_gold_fact_cols):
    print(" ", dict(r))

# 3. Check duration calculation on silver.silver_titles (parsing 'min')
sql_movie_duration_silver = """
SELECT 
    COUNT(*) as total_movies,
    AVG(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)) as avg_duration_minutes,
    MIN(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)) as min_duration_minutes,
    MAX(CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER)) as max_duration_minutes
FROM silver.silver_titles
WHERE type = 'Movie' AND duration LIKE '%min%';
"""
print("\n--- 3. MOVIE DURATION METRICS FROM silver.silver_titles ---")
print(execute_read_query(sql_movie_duration_silver)[0])

# 4. Longest and Shortest Movie in silver.silver_titles
sql_longest_movie = """
SELECT title, type, duration, release_year
FROM silver.silver_titles
WHERE type = 'Movie' AND duration LIKE '%min%'
ORDER BY CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) DESC
LIMIT 3;
"""
print("\n--- 4. LONGEST MOVIES (silver.silver_titles) ---")
for r in execute_read_query(sql_longest_movie):
    print(" ", dict(r))

sql_shortest_movie = """
SELECT title, type, duration, release_year
FROM silver.silver_titles
WHERE type = 'Movie' AND duration LIKE '%min%'
ORDER BY CAST(SPLIT_PART(duration, ' ', 1) AS INTEGER) ASC
LIMIT 3;
"""
print("\n--- 5. SHORTEST MOVIES (silver.silver_titles) ---")
for r in execute_read_query(sql_shortest_movie):
    print(" ", dict(r))

# 5. Inspect duration in gold.fact_content (duration_minutes column)
sql_gold_duration = """
SELECT 
    COUNT(DISTINCT f.title_key) as distinct_movies,
    AVG(f.duration_minutes) as gold_fact_avg_duration_exploded,
    AVG(t_distinct.duration_minutes) as gold_distinct_movie_avg_duration
FROM gold.fact_content f
JOIN gold.dim_type t ON f.type_key = t.type_key
JOIN (
    SELECT DISTINCT title_key, duration_minutes 
    FROM gold.fact_content
) t_distinct ON f.title_key = t_distinct.title_key
WHERE t.type = 'Movie';
"""
print("\n--- 6. MOVIE DURATION IN gold.fact_content (duration_minutes) ---")
print(execute_read_query(sql_gold_duration)[0])
