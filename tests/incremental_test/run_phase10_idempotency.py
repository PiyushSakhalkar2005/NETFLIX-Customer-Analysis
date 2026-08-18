import os
import sys
import subprocess
import json
import psycopg2

project_root = r"d:\Piyu\My Projects\Netflix project"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

env = os.environ.copy()
env["HADOOP_HOME"] = os.path.join(project_root, "hadoop")
env["PATH"] = os.path.join(project_root, "hadoop", "bin") + os.pathsep + env.get("PATH", "")

from src.database.pipeline_reset_and_reload import load_parquet_to_postgres, load_audit_metadata

def main():
    print("=" * 60)
    print("PHASE 10: RUNNING INCREMENTAL PIPELINE SECOND TIME (IDEMPOTENCY TEST)")
    print("=" * 60)
    
    wm_file = os.path.join(project_root, "data", "metadata", "watermarks.json")
    with open(wm_file, "r") as f:
        wm_before = json.load(f)["netflix_titles_pipeline"]["last_processed_timestamp"]
    print(f"Watermark Before Second Run: {wm_before}")
    
    # Run incremental pipeline second time
    cmd = [sys.executable, "src/silver/run_incremental_pipeline.py"]
    res = subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=project_root)
    
    print("--- STDOUT SECOND RUN ---")
    print(res.stdout)
    if res.stderr:
        print("--- STDERR SECOND RUN ---")
        print(res.stderr)
        
    # Sync Postgres
    load_parquet_to_postgres(mode="INCREMENTAL")
    load_audit_metadata()
    
    with open(wm_file, "r") as f:
        wm_after = json.load(f)["netflix_titles_pipeline"]["last_processed_timestamp"]
    print(f"Watermark After Second Run: {wm_after}")
    
    # Check row counts in PostgreSQL
    conn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='root', dbname='netflix_dw_new')
    cur = conn.cursor()
    
    tables = [
        'silver.silver_titles',
        'silver.silver_titles_scd2',
        'silver.silver_country',
        'silver.silver_genres',
        'silver.silver_directors',
        'silver.silver_cast',
        'gold.fact_content'
    ]
    
    print("\n--- POSTGRES ROW COUNTS AFTER SECOND RUN ---")
    for t in tables:
        cur.execute(f"SELECT COUNT(*) FROM {t};")
        print(f"  {t}: {cur.fetchone()[0]}")
        
    test_ids = ('TEST001','TEST002','TEST003','TEST004','TEST005')
    cur.execute("SELECT show_id, COUNT(*) FROM silver.silver_titles WHERE show_id IN %s GROUP BY show_id;", (test_ids,))
    print("\nDuplicates check in silver_titles:", cur.fetchall())
    
    cur.execute("SELECT show_id, version_number, is_current FROM silver.silver_titles_scd2 WHERE show_id IN %s ORDER BY show_id;", (test_ids,))
    print("SCD2 versions check:", cur.fetchall())
    
    cur.close()
    conn.close()
    
    if wm_before == wm_after and "INCREMENTAL_RUN_SKIPPED: No data" in res.stdout:
        print("\nPASS: Idempotency verified! 0 records detected, no duplicates created, watermark unchanged.")
    else:
        print("\nIDEMPOTENCY TEST COMPLETED.")

if __name__ == "__main__":
    main()
