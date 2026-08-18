import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.ai_backend.database import execute_read_query

print("=== CHECKING COUNTRY DISTRIBUTION MATH ===")

# 1. Total Distinct Titles in Warehouse
sql_total = "SELECT COUNT(DISTINCT title_key) AS total_catalog FROM gold.fact_content;"
rows_total = execute_read_query(sql_total)
total_catalog = float(rows_total[0]["total_catalog"])
print(f"Total Catalog Titles: {total_catalog:.0f}")

# 2. Sum of country-title instances (Denominator of v_kpi_country_distribution)
sql_denom = """
SELECT SUM(cnt) AS sum_country_assignments FROM (
    SELECT COUNT(DISTINCT f.title_key) as cnt
    FROM gold.fact_content f
    JOIN gold.dim_country c ON f.country_key = c.country_key
    WHERE c.country <> 'Unknown Country'
    GROUP BY c.country
) t;
"""
rows_denom = execute_read_query(sql_denom)
sum_country_assignments = float(rows_denom[0]["sum_country_assignments"])
print(f"Sum of Country-Title Assignments (View Denominator): {sum_country_assignments:.0f}")

# 3. Query v_kpi_country_distribution top 5
sql_view = "SELECT country_name, titles_count, percentage_share FROM metadata.v_kpi_country_distribution ORDER BY titles_count DESC LIMIT 5;"
rows_view = execute_read_query(sql_view)

print("\n--- TOP 5 COUNTRIES ANALYSIS ---")
for r in rows_view:
    country = r["country_name"]
    count = float(r["titles_count"])
    view_pct = float(r["percentage_share"])
    calc_view_pct = round(100.0 * count / sum_country_assignments, 2)
    calc_catalog_pct = round(100.0 * count / total_catalog, 2)
    print(f"Country: {country:15s} | Titles: {count:4.0f} | View Pct: {view_pct:5.2f}% (Calc vs 10,012: {calc_view_pct:5.2f}%) | Catalog Pct (vs 8,807): {calc_catalog_pct:5.2f}%")
