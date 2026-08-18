import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.ai_backend.database import execute_read_query

print("==========================================================")
print("AVERAGE CONTENT AGE KPI GRAIN INVESTIGATION")
print("==========================================================")

# 1. Silver Layer (silver.silver_titles) - Distinct Title Grain
sql_silver = """
SELECT 
    ROUND(AVG(content_age), 2) as silver_avg_age_exact,
    ROUND(AVG(content_age), 1) as silver_avg_age_rounded
FROM silver.silver_titles;
"""
print("\n--- 1. SILVER LAYER (silver.silver_titles) [Distinct Title Grain] ---")
print(execute_read_query(sql_silver)[0])

# 2. Gold Fact Content - Fact Row Grain (Exploded by Country/Genre/Director)
sql_gold_fact = """
SELECT 
    ROUND(AVG(content_age), 2) as gold_fact_avg_age_exact,
    ROUND(AVG(content_age), 1) as gold_fact_avg_age_rounded
FROM gold.fact_content;
"""
print("\n--- 2. GOLD FACT CONTENT (gold.fact_content) [Fact Row Grain] ---")
print(execute_read_query(sql_gold_fact)[0])

# 3. Gold Fact Content - Distinct Title Grain (Subquery or Dim Title Join)
sql_gold_distinct_title = """
SELECT 
    ROUND(AVG(title_avg_age), 2) as gold_distinct_title_avg_age_exact,
    ROUND(AVG(title_avg_age), 1) as gold_distinct_title_avg_age_rounded
FROM (
    SELECT DISTINCT title_key, content_age as title_avg_age
    FROM gold.fact_content
) t;
"""
print("\n--- 3. GOLD FACT DISTINCT TITLE GRAIN ---")
print(execute_read_query(sql_gold_distinct_title)[0])

# 4. Gold Dim Date / Release Year Calculation
sql_release_year_age = """
SELECT 
    ROUND(AVG(2026 - release_year), 2) as silver_release_year_age_exact,
    ROUND(AVG(2026 - release_year), 1) as silver_release_year_age_rounded
FROM silver.silver_titles;
"""
print("\n--- 4. SILVER RELEASE YEAR AGE (2026 - release_year) ---")
print(execute_read_query(sql_release_year_age)[0])

# 5. Metadata View (metadata.v_kpi_general_summary)
sql_meta_view = "SELECT average_content_age FROM metadata.v_kpi_general_summary;"
print("\n--- 5. METADATA VIEW (metadata.v_kpi_general_summary) ---")
print(execute_read_query(sql_meta_view)[0])

# 6. kpi_tool.py calculation (from tools/kpi_tool.py)
sql_kpi_tool = "SELECT ROUND(AVG(content_age), 1) AS avg_age FROM silver.silver_titles;"
print("\n--- 6. KPI TOOL CALCULATION (tools/kpi_tool.py on silver.silver_titles) ---")
print(execute_read_query(sql_kpi_tool)[0])
