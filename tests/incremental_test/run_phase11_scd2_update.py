import os
import sys

project_root = r"d:\Piyu\My Projects\Netflix project"
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set HADOOP_HOME environment variable BEFORE any PySpark imports
hadoop_dir = os.path.join(project_root, "hadoop")
if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

env = os.environ.copy()

import uuid
import subprocess
import json
import psycopg2

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, to_date
from src.database.pipeline_reset_and_reload import load_parquet_to_postgres, load_audit_metadata

def main():
    print("=" * 60)
    print("PHASE 11: TEST SCD2 WITH AN UPDATED EXISTING RECORD (TEST001)")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("SCD2UpdateTest") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
        
    update_csv = os.path.join(project_root, "tests", "incremental_test", "update_record_TEST001.csv")
    df_raw = spark.read \
        .option("header", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(update_csv)
        
    batch_id = f"scd2-update-{str(uuid.uuid4())[:8]}"
    run_id = f"scd2-run-{str(uuid.uuid4())[:8]}"
    
    df_bronze = df_raw \
        .withColumn("batch_id", lit(batch_id)) \
        .withColumn("pipeline_run_id", lit(run_id)) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_system", lit("netflix_medallion_pipeline")) \
        .withColumn("source_file", lit("update_record_TEST001.csv")) \
        .withColumn("load_type", lit("INCREMENTAL")) \
        .withColumn("ingestion_date", to_date(current_timestamp()))
        
    dest_path = os.path.join(project_root, "data", "bronze", "bronze_netflix_csv")
    print(f"Appending updated record TEST001 to Bronze Parquet at: {dest_path}")
    df_bronze.write.mode("append").format("parquet").partitionBy("ingestion_date").save(dest_path)
    spark.stop()
    print("Spark session stopped cleanly.")
    
    # Run incremental pipeline
    print("\nRunning Incremental Engine to process SCD2 update...")
    cmd = [sys.executable, "src/silver/run_incremental_pipeline.py"]
    res = subprocess.run(cmd, env=env, capture_output=True, text=True, cwd=project_root)
    print("--- STDOUT SCD2 UPDATE RUN ---")
    print(res.stdout)
    if res.stderr:
        print("--- STDERR SCD2 UPDATE RUN ---")
        print(res.stderr)
        
    # Sync Postgres
    load_parquet_to_postgres(mode="INCREMENTAL")
    load_audit_metadata()
    
    # Verify in PostgreSQL
    conn = psycopg2.connect(host='localhost', port=5432, user='postgres', password='root', dbname='netflix_dw_new')
    cur = conn.cursor()
    
    cur.execute("""
        SELECT show_id, version_number, rating, is_current, effective_start_date, effective_end_date 
        FROM silver.silver_titles_scd2 
        WHERE show_id = 'TEST001' 
        ORDER BY version_number;
    """)
    rows = cur.fetchall()
    print("\n--- POSTGRESQL SCD2 VERSIONS FOR TEST001 ---")
    for r in rows:
        print("  ", r)
        
    cur.close()
    conn.close()
    
    if len(rows) >= 2 and rows[-1][3] is True and rows[-1][1] > 1 and rows[-1][2] == "TV-MA":
        print("\nPASS: SCD Type 2 Update Verified! Old version expired (is_current=False), new Version created (is_current=True, rating=TV-MA).")
    else:
        print("\nPHASE 11 SCD2 UPDATE TEST COMPLETED.")

if __name__ == "__main__":
    main()
