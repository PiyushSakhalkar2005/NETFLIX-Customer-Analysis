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
    print("PHASE 5: BRONZE VERIFICATION")
    print("=" * 60)
    cur.execute("SELECT COUNT(*) FROM bronze.bronze_netflix_csv;")
    print(f"Total Bronze Count: {cur.fetchone()[0]}")

    cur.execute("SELECT show_id, title, ingestion_timestamp, load_type FROM bronze.bronze_netflix_csv WHERE show_id IN %s ORDER BY show_id;", (test_ids,))
    print("Test records in Bronze:")
    for r in cur.fetchall():
        print("  ", r)

    print("\n" + "=" * 60)
    print("PHASE 6: SILVER VERIFICATION")
    print("=" * 60)
    cur.execute("SELECT COUNT(*) FROM silver.silver_titles;")
    print(f"Total Silver Titles Count: {cur.fetchone()[0]}")

    cur.execute("SELECT show_id, title, type, date_added, release_year, rating FROM silver.silver_titles WHERE show_id IN %s ORDER BY show_id;", (test_ids,))
    print("Test records in Silver Titles:")
    for r in cur.fetchall():
        print("  ", r)

    print("\nNormalized lookup tables verification for TEST records:")
    for table, col in [('silver.silver_country','country'), ('silver.silver_genres','genre'), ('silver.silver_directors','director'), ('silver.silver_cast','cast_member')]:
        cur.execute(f"SELECT show_id, {col} FROM {table} WHERE show_id IN %s ORDER BY show_id, {col};", (test_ids,))
        rows = cur.fetchall()
        print(f"  {table} ({len(rows)} mappings):")
        for r in rows:
            print("    ", r)

    print("\n" + "=" * 60)
    print("PHASE 7: SCD TYPE 2 VERIFICATION")
    print("=" * 60)
    cur.execute("SELECT COUNT(*) FROM silver.silver_titles_scd2;")
    print(f"Total Silver SCD2 Count: {cur.fetchone()[0]}")

    cur.execute("SELECT show_id, version_number, is_current, effective_start_date, effective_end_date FROM silver.silver_titles_scd2 WHERE show_id IN %s ORDER BY show_id, version_number;", (test_ids,))
    print("Test records in SCD2:")
    for r in cur.fetchall():
        print("  ", r)

    print("\n" + "=" * 60)
    print("PHASE 8: GOLD VERIFICATION")
    print("=" * 60)
    cur.execute("SELECT COUNT(*) FROM gold.fact_content;")
    print(f"Total Gold Fact Count: {cur.fetchone()[0]}")

    cur.execute("""
        SELECT f.title_key, t.show_id, t.title, f.duration_minutes, f.season_count 
        FROM gold.fact_content f 
        JOIN gold.dim_title t ON f.title_key = t.title_key 
        WHERE t.show_id IN %s 
        ORDER BY t.show_id;
    """, (test_ids,))
    fact_rows = cur.fetchall()
    print(f"Fact Content rows mapped for TEST records (Total: {len(fact_rows)}):")
    for r in fact_rows:
        print("  ", r)

    print("\n" + "=" * 60)
    print("PHASE 9: WATERMARK VERIFICATION")
    print("=" * 60)
    cur.execute("SELECT pipeline_name, last_processed_timestamp, last_successful_batch_id, updated_at FROM metadata.pipeline_watermarks WHERE pipeline_name = %s;", ('netflix_titles_pipeline',))
    print("Postgres Watermark:", cur.fetchone())

    wm_file = os.path.join(project_root, "data", "metadata", "watermarks.json")
    with open(wm_file, 'r') as f:
        print("JSON Watermark:", json.load(f))

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
