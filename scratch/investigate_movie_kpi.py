import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.ai_backend.database import execute_read_query

print("==========================================================")
print("CRITICAL DATA CONSISTENCY INVESTIGATION: MOVIE KPI (PART 2)")
print("==========================================================")

# A. Silver layer distinct title counts
sql_silver = """
SELECT 
    COUNT(*) as silver_total_rows,
    COUNT(DISTINCT show_id) as silver_distinct_show_ids,
    COUNT(CASE WHEN type = 'Movie' THEN 1 END) as silver_movies,
    COUNT(CASE WHEN type = 'TV Show' THEN 1 END) as silver_tv_shows
FROM silver.silver_titles;
"""
print("\n--- 1. SILVER LAYER (silver.silver_titles) ---")
print(execute_read_query(sql_silver)[0])

# B. Gold Dim Title distinct count
sql_gold_dim = """
SELECT 
    COUNT(*) as dim_title_rows,
    COUNT(DISTINCT show_id) as dim_title_distinct_shows
FROM gold.dim_title;
"""
print("\n--- 2. GOLD DIM TITLE (gold.dim_title) ---")
print(execute_read_query(sql_gold_dim)[0])

# C. Gold Fact Table Total (gold.fact_content)
sql_gold_fact = """
SELECT 
    COUNT(*) as gold_fact_total_rows,
    COUNT(DISTINCT title_key) as gold_fact_distinct_titles
FROM gold.fact_content;
"""
print("\n--- 3. GOLD FACT CONTENT TOTAL (gold.fact_content) ---")
print(execute_read_query(sql_gold_fact)[0])

# D. Gold Fact Row Count joined with dim_type (DISTINCT vs NON-DISTINCT)
sql_gold_by_type = """
SELECT 
    t.type,
    COUNT(*) as non_distinct_fact_rows,
    COUNT(DISTINCT f.title_key) as distinct_title_keys
FROM gold.fact_content f
JOIN gold.dim_type t ON f.type_key = t.type_key
GROUP BY t.type;
"""
print("\n--- 4. GOLD FACT CONTENT BY TYPE (Non-Distinct vs Distinct) ---")
for r in execute_read_query(sql_gold_by_type):
    print(" ", dict(r))

# E. Metadata View: v_kpi_general_summary
sql_meta_general = "SELECT * FROM metadata.v_kpi_general_summary;"
print("\n--- 5. METADATA VIEW (metadata.v_kpi_general_summary) ---")
print(execute_read_query(sql_meta_general)[0])

# F. Check Power BI SQL Views in src/database/powerbi_views.sql
print("\n--- 6. CHECKING OTHER METADATA VIEWS ---")
sql_country_dist = "SELECT SUM(titles_count) as total_distinct_country_titles FROM metadata.v_kpi_country_distribution;"
print("Sum of titles_count in v_kpi_country_distribution:", execute_read_query(sql_country_dist)[0])
