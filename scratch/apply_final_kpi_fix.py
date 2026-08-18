import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from apps.ai_backend.database import get_db_connection, execute_read_query

print("=== APPLYING FINAL AUTHORIZED KPI GRAIN FIX TO METADATA.V_KPI_GENERAL_SUMMARY ===")

fix_ddl = """
CREATE OR REPLACE VIEW metadata.v_kpi_general_summary AS
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
"""

conn = get_db_connection()
cursor = conn.cursor()
cursor.execute(fix_ddl)
conn.commit()
cursor.close()
conn.close()

print("DDL executed successfully! Verifying updated view in PostgreSQL...")

sql_verify = "SELECT * FROM metadata.v_kpi_general_summary;"
row = execute_read_query(sql_verify)[0]
print("\n--- POSTGRESQL VIEW OUTPUT ---")
print(dict(row))

total_titles = row["total_titles"]
total_movies = row["total_movies"]
total_tv_shows = row["total_tv_shows"]
average_content_age = float(row["average_content_age"])

assert total_titles == 8807, f"Expected 8807 total titles, got {total_titles}"
assert total_movies == 6131, f"Expected 6131 total movies, got {total_movies}"
assert total_tv_shows == 2676, f"Expected 2676 total tv shows, got {total_tv_shows}"
assert average_content_age == 11.8, f"Expected 11.8 average content age, got {average_content_age}"

print("\nALL FOUR KPI VALUES VERIFIED IN POSTGRESQL!")
print(f"Total Titles       : {total_titles:,} (Expected: 8,807)")
print(f"Total Movies       : {total_movies:,} (Expected: 6,131)")
print(f"Total TV Shows     : {total_tv_shows:,} (Expected: 2,676)")
print(f"Average Content Age: {average_content_age} yrs (Expected: 11.8 yrs)")
