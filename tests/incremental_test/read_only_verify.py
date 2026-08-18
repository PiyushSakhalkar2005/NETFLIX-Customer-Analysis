import os
import sys
import json
import psycopg2

project_root = r"d:\Piyu\My Projects\Netflix project"

def main():
    conn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='root', dbname='netflix_dw_new')
    cur = conn.cursor()

    test_ids = ('TEST001','TEST002','TEST003','TEST004','TEST005')

    print("=" * 60)
    print("PHASE 2: SCHEMAS AND TABLES CHECK")
    print("=" * 60)
    cur.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables 
        WHERE table_schema IN ('bronze', 'silver', 'gold', 'metadata')
        ORDER BY table_schema, table_name;
    """)
    tables = cur.fetchall()
    for t in tables:
        print(f"  {t[0]}.{t[1]}")

    print("\n" + "=" * 60)
    print("PHASE 3: BRONZE VERIFICATION")
    print("=" * 60)
    cur.execute("""
        SELECT show_id, title, ingestion_timestamp, batch_id, load_type
        FROM bronze.bronze_netflix_csv
        WHERE show_id IN %s
        ORDER BY show_id;
    """, (test_ids,))
    print("Test records in Bronze:")
    for r in cur.fetchall():
        print("  ", r)

    cur.execute("""
        SELECT show_id, COUNT(*) AS record_count
        FROM bronze.bronze_netflix_csv
        WHERE show_id LIKE 'TEST%'
        GROUP BY show_id
        ORDER BY show_id;
    """)
    print("\nBronze count per TEST show_id:")
    for r in cur.fetchall():
        print("  ", r)

    cur.execute("SELECT COUNT(*) FROM bronze.bronze_netflix_csv;")
    print(f"\nTotal Bronze rows in Postgres: {cur.fetchone()[0]}")

    print("\n" + "=" * 60)
    print("PHASE 4: SILVER VERIFICATION")
    print("=" * 60)
    cur.execute("""
        SELECT show_id, title, type, rating, release_year
        FROM silver.silver_titles
        WHERE show_id IN %s
        ORDER BY show_id;
    """, (test_ids,))
    print("Test records in Silver Titles:")
    for r in cur.fetchall():
        print("  ", r)

    cur.execute("SELECT COUNT(*) FROM silver.silver_titles WHERE show_id LIKE 'TEST%';")
    print(f"\nTotal TEST records count in silver_titles: {cur.fetchone()[0]}")

    cur.execute("""
        SELECT show_id, COUNT(*)
        FROM silver.silver_titles
        WHERE show_id LIKE 'TEST%'
        GROUP BY show_id
        HAVING COUNT(*) > 1;
    """)
    dups = cur.fetchall()
    print(f"Silver titles duplicates (expected 0 rows): {dups}")

    cur.execute("SELECT COUNT(*) FROM silver.silver_titles;")
    print(f"Total Silver Titles rows in Postgres: {cur.fetchone()[0]}")

    print("\n" + "=" * 60)
    print("PHASE 5: SILVER NORMALIZATION VERIFICATION")
    print("=" * 60)
    for t, col in [('silver.silver_country','country'), ('silver.silver_genres','genre'), ('silver.silver_directors','director'), ('silver.silver_cast','cast_member')]:
        cur.execute(f"SELECT show_id, {col} FROM {t} WHERE show_id IN %s ORDER BY show_id, {col};", (test_ids,))
        rows = cur.fetchall()
        print(f"\n{t} ({len(rows)} mappings):")
        for r in rows:
            print("  ", r)

    print("\n" + "=" * 60)
    print("PHASE 6 & 7: SCD TYPE 2 VERIFICATION")
    print("=" * 60)
    cur.execute("""
        SELECT show_id, title, rating, version_number, is_current, effective_start_date, effective_end_date
        FROM silver.silver_titles_scd2
        WHERE show_id IN %s
        ORDER BY show_id, version_number;
    """, (test_ids,))
    print("All test records in silver_titles_scd2:")
    for r in cur.fetchall():
        print("  ", r)

    cur.execute("SELECT COUNT(*) FROM silver.silver_titles_scd2;")
    print(f"\nTotal Silver SCD2 rows in Postgres: {cur.fetchone()[0]}")

    cur.execute("""
        SELECT show_id, rating, version_number, is_current, effective_start_date, effective_end_date
        FROM silver.silver_titles_scd2
        WHERE show_id = 'TEST001'
        ORDER BY version_number;
    """)
    print("\nTEST001 SCD2 versions detail:")
    for r in cur.fetchall():
        print("  ", r)

    cur.execute("""
        SELECT show_id, COUNT(*) AS versions
        FROM silver.silver_titles_scd2
        WHERE show_id = 'TEST001'
        GROUP BY show_id;
    """)
    print(f"TEST001 version count: {cur.fetchone()}")

    cur.execute("""
        SELECT COUNT(*)
        FROM silver.silver_titles_scd2
        WHERE show_id = 'TEST001' AND is_current = true;
    """)
    print(f"TEST001 active current version count: {cur.fetchone()[0]}")

    print("\n" + "=" * 60)
    print("PHASE 8: GOLD VERIFICATION")
    print("=" * 60)
    cur.execute("""
        SELECT title_key, show_id, title, description, content_category
        FROM gold.dim_title
        WHERE title LIKE 'Incremental Test%'
        ORDER BY show_id;
    """)
    print("Gold dim_title test records:")
    for r in cur.fetchall():
        print("  ", r)

    cur.execute("""
        SELECT f.title_key, d.show_id, d.title, f.duration_minutes, f.season_count, f.release_year, f.ingestion_timestamp
        FROM gold.fact_content f
        JOIN gold.dim_title d ON f.title_key = d.title_key
        WHERE d.title LIKE 'Incremental Test%'
        ORDER BY d.show_id;
    """)
    fact_rows = cur.fetchall()
    print(f"\nGold fact_content test rows count: {len(fact_rows)}")
    for r in fact_rows:
        print("  ", r)

    cur.execute("SELECT COUNT(*) FROM gold.fact_content;")
    print(f"\nTotal Gold fact_content rows in Postgres: {cur.fetchone()[0]}")

    print("\n" + "=" * 60)
    print("PHASE 9: WATERMARK VERIFICATION")
    print("=" * 60)
    cur.execute("""
        SELECT pipeline_name, last_processed_timestamp, last_successful_batch_id, updated_at 
        FROM metadata.pipeline_watermarks 
        WHERE pipeline_name = 'netflix_titles_pipeline';
    """)
    print("Postgres metadata.pipeline_watermarks:", cur.fetchone())

    wm_path = os.path.join(project_root, 'data', 'metadata', 'watermarks.json')
    if os.path.exists(wm_path):
        with open(wm_path, 'r') as f:
            print("JSON data/metadata/watermarks.json:", json.load(f))

    print("\n" + "=" * 60)
    print("PHASE 10: IDEMPOTENCY LOG EVIDENCE INSPECTION")
    print("=" * 60)
    audit_runs_path = os.path.join(project_root, 'data', 'metadata', 'pipeline_runs_audit.json')
    if os.path.exists(audit_runs_path):
        with open(audit_runs_path, 'r') as f:
            audit_runs = json.load(f)
            print(f"Total pipeline audit run entries logged: {len(audit_runs)}")
            print("Last 3 audit run entries:")
            for ar in audit_runs[-3:]:
                print("  ", ar)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
