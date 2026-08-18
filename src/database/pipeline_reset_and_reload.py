import os
import sys
import shutil
import json
import subprocess
import re
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import argparse
from src.utils.config_loader import ConfigLoader

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'user': 'postgres',
    'password': 'root',
    'dbname': 'netflix_dw_new'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def reset_database():
    print("Step 1: Truncating database tables...")
    tables_to_truncate = [
        # Gold
        "gold.fact_content",
        "gold.dim_title",
        "gold.dim_director",
        "gold.dim_country",
        "gold.dim_genre",
        "gold.dim_rating",
        "gold.dim_type",
        "gold.dim_date",
        "gold.kpi_content_summary",
        "gold.kpi_country_summary",
        "gold.kpi_genre_summary",
        "gold.kpi_rating_summary",
        "gold.kpi_release_summary",
        "gold.kpi_summary",
        "gold.country_distribution",
        "gold.genre_distribution",
        "gold.rating_distribution",
        "gold.top_genres_by_country",
        "gold.director_rankings",
        "gold.yearly_releases_summary",
        "gold.gold_titles_enriched",
        # Silver
        "silver.silver_cast",
        "silver.silver_country",
        "silver.silver_directors",
        "silver.silver_genres",
        "silver.silver_titles",
        "silver.silver_titles_scd2",
        # Bronze
        "bronze.bronze_netflix_csv",
        "bronze.bronze_netflix_incremental_json",
        # Public
        "public.netflix_raw",
        "public.netflix_updates",
        # Metadata
        "metadata.pipeline_steps",
        "metadata.pipeline_runs",
        "metadata.pipeline_watermarks",
        "metadata.dataset_metadata",
        "metadata.audit_log"
    ]
    
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Disable triggers/constraints for cascade truncate
        for table in tables_to_truncate:
            try:
                cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
                print(f"  Truncated {table}")
            except Exception as e:
                print(f"  Warning truncating {table}: {e}")
                conn.rollback()
        conn.commit()
        print("Database reset completed successfully.\n")
    except Exception as e:
        print("Error during database reset:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

def clean_local_storage():
    print("Step 2: Clearing local Parquet files and logs...")
    data_dir = os.path.join(project_root, "data")
    folders_to_clear = ["bronze", "silver", "gold", "rejected_records", "transformation_rejects", "metadata"]
    
    for folder in folders_to_clear:
        folder_path = os.path.join(data_dir, folder)
        if os.path.exists(folder_path):
            # Clean folder contents but keep the folder itself
            for item in os.listdir(folder_path):
                # Don't delete raw/ netflix files
                if folder == "raw":
                    continue
                item_path = os.path.join(folder_path, item)
                try:
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path)
                    else:
                        os.remove(item_path)
                except Exception as e:
                    print(f"  Failed to delete {item_path}: {e}")
            print(f"  Cleared {folder_path}")
    print("Local storage cleanup completed.\n")

def run_cmd(args, step_name):
    print(f"Running ETL step: {step_name}...")
    print(f"Command: {' '.join(args)}")
    result = subprocess.run(args, capture_output=True, text=True, cwd=project_root)
    if result.returncode != 0:
        print(f"Error in {step_name}:")
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
        raise RuntimeError(f"Step {step_name} failed with exit code {result.returncode}")
    print(result.stdout)
    return result.stdout

def run_etl_pipeline(mode: str = "INCREMENTAL"):
    print(f"Step 3: Rerunning PySpark ETL Pipeline (Mode: {mode})...")
    # 1. Landing raw ingestion (CSV, JSON, XML)
    ingest_output = run_cmd(["python", "src/ingestion/ingest_raw.py", "--source", "all"], "Landing Ingestion")
    
    # Extract batch_id
    batch_match = re.search(r"batch_id=([a-fA-F0-9\-]+)", ingest_output)
    if not batch_match:
        raise RuntimeError("Could not parse batch_id from landing ingestion stdout.")
    batch_id = batch_match.group(1)
    print(f"Extracted Ingestion Batch ID: {batch_id}\n")
    
    # 2. Data Quality validation
    run_cmd(["python", "src/quality/run_validation.py", "--source", "all", "--batch-id", batch_id], "Data Quality Validation")
    
    # 3. Incremental Processing Engine Silver/Gold Load
    cmd = ["python", "src/silver/run_incremental_pipeline.py", "--batch-id", batch_id]
    if mode.upper() == "FULL":
        cmd.append("--force-full")
    run_cmd(cmd, f"Medallion Load ({mode} Mode)")
    
    # 4. Advanced Business Transformations
    run_cmd(["python", "src/transformations/run_business_transforms.py", "--batch-id", batch_id], "Business Transformations")
    
    print("PySpark ETL pipeline rerun completed successfully.\n")
    return batch_id

def load_parquet_to_postgres(mode: str = "INCREMENTAL"):
    print(f"Step 4: Loading Parquet data into PostgreSQL (Mode: {mode})...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Schema mappings: (parquet_subpath_or_file, schema, table_name, drop_scd2_cols_flag)
    mappings = [
        # Bronze
        ("data/bronze/bronze_netflix_csv", "bronze", "bronze_netflix_csv", False),
        ("data/bronze/bronze_netflix_json", "bronze", "bronze_netflix_json", False),
        ("data/bronze/bronze_netflix_xml", "bronze", "bronze_netflix_xml", False),
        
        # Silver
        ("data/silver/silver_titles", "silver", "silver_titles", True),
        ("data/silver/silver_titles", "silver", "silver_titles_scd2", False),
        ("data/silver/silver_country", "silver", "silver_country", False),
        ("data/silver/silver_genres", "silver", "silver_genres", False),
        ("data/silver/silver_directors", "silver", "silver_directors", False),
        ("data/silver/silver_cast", "silver", "silver_cast", False),
        
        # Gold
        ("data/gold/dim_title", "gold", "dim_title", False),
        ("data/gold/dim_director", "gold", "dim_director", False),
        ("data/gold/dim_country", "gold", "dim_country", False),
        ("data/gold/dim_genre", "gold", "dim_genre", False),
        ("data/gold/dim_rating", "gold", "dim_rating", False),
        ("data/gold/dim_type", "gold", "dim_type", False),
        ("data/gold/dim_date", "gold", "dim_date", False),
        ("data/gold/fact_content", "gold", "fact_content", False),
        
        # KPI Tables
        ("data/gold/kpi_content_summary", "gold", "kpi_content_summary", False),
        ("data/gold/kpi_country_summary", "gold", "kpi_country_summary", False),
        ("data/gold/kpi_genre_summary", "gold", "kpi_genre_summary", False),
        ("data/gold/kpi_rating_summary", "gold", "kpi_rating_summary", False),
        ("data/gold/kpi_release_summary", "gold", "kpi_release_summary", False),
        ("data/gold/kpi_summary", "gold", "kpi_summary", False),
        ("data/gold/country_distribution", "gold", "country_distribution", False),
        ("data/gold/genre_distribution", "gold", "genre_distribution", False),
        ("data/gold/rating_distribution", "gold", "rating_distribution", False),
        ("data/gold/top_genres_by_country", "gold", "top_genres_by_country", False),
        ("data/gold/director_rankings", "gold", "director_rankings", False),
        ("data/gold/yearly_releases_summary", "gold", "yearly_releases_summary", False),
        ("data/gold/gold_titles_enriched", "gold", "gold_titles_enriched", False)
    ]
    
    primary_keys = {
        "silver.silver_cast": ['show_id', 'cast_member'],
        "silver.silver_country": ['show_id', 'country'],
        "silver.silver_directors": ['show_id', 'director'],
        "silver.silver_genres": ['show_id', 'genre'],
        "silver.silver_titles": ['show_id'],
        "silver.silver_titles_scd2": ['show_id', 'version_number'],
        "gold.dim_title": ['title_key'],
        "gold.dim_director": ['director_key'],
        "gold.dim_country": ['country_key'],
        "gold.dim_genre": ['genre_key'],
        "gold.dim_rating": ['rating_key'],
        "gold.dim_type": ['type_key'],
        "gold.dim_date": ['date_key'],
        "gold.fact_content": ['title_key', 'director_key', 'country_key', 'genre_key', 'rating_key', 'type_key', 'date_key']
    }

    scd2_cols = [
        "effective_start_date", "effective_end_date", "is_current", 
        "version_number", "record_created_timestamp", "record_updated_timestamp"
    ]
    
    try:
        for path_suffix, schema, table, drop_scd2 in mappings:
            full_path = os.path.join(project_root, path_suffix)
            if not os.path.exists(full_path):
                print(f"  Warning: Parquet path {full_path} not found. Skipping...")
                continue
                
            # Read Parquet files into DataFrame
            df = pd.read_parquet(full_path)
            if df.empty:
                print(f"  Parquet dataset {schema}.{table} is empty. Skipping...")
                continue
            
            # Deduplicate by primary keys if applicable
            full_table_name = f"{schema}.{table}"
            if full_table_name in primary_keys:
                if "version_number" in df.columns:
                    df = df.sort_values("version_number", ascending=False)
                df = df.drop_duplicates(subset=primary_keys[full_table_name])
            
            # Fetch Postgres table columns dynamically to filter DataFrame fields
            cursor.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = '{schema}' AND table_name = '{table}'
                ORDER BY ordinal_position;
            """)
            db_cols = [row[0] for row in cursor.fetchall()]
            
            if drop_scd2:
                # Filter out SCD 2 columns for basic silver_titles table
                df = df.drop(columns=[c for c in scd2_cols if c in df.columns], errors='ignore')
                
            # Keep only columns present in Postgres DDL
            valid_cols = [c for c in df.columns if c in db_cols]
            df = df[valid_cols]
            
            # Reorder DataFrame columns to match Postgres columns order exactly
            # and align missing columns if any by filling with Nulls
            for col_name in db_cols:
                if col_name not in df.columns:
                    df[col_name] = None
            df = df[db_cols]
            
            # Replace empty strings and NaNs/NaTs with None for database inserts
            df = df.replace({"": None})
            df = df.astype(object)
            df = df.where(df.notnull(), None)
            
            cols_str = ",".join([f'"{c}"' for c in db_cols])
            conflict_clause = ""
            
            if mode.upper() == "INCREMENTAL":
                # Handle KPI aggregate tables by truncating before inserting refreshed aggregates
                if table.startswith("kpi_") or table in ["country_distribution", "genre_distribution", "rating_distribution", "top_genres_by_country", "director_rankings", "yearly_releases_summary", "gold_titles_enriched"]:
                    cursor.execute(f'TRUNCATE TABLE "{schema}"."{table}" CASCADE;')
                elif full_table_name == "silver.silver_titles":
                    update_cols = [f'"{c}" = EXCLUDED."{c}"' for c in db_cols if c != "show_id"]
                    conflict_clause = f' ON CONFLICT ("show_id") DO UPDATE SET {", ".join(update_cols)}'
                elif full_table_name == "silver.silver_titles_scd2":
                    update_cols = [f'"{c}" = EXCLUDED."{c}"' for c in db_cols if c not in ["show_id", "version_number"]]
                    conflict_clause = f' ON CONFLICT ("show_id", "version_number") DO UPDATE SET {", ".join(update_cols)}'
                elif full_table_name == "silver.silver_country":
                    conflict_clause = ' ON CONFLICT ("show_id", "country") DO NOTHING'
                elif full_table_name == "silver.silver_genres":
                    conflict_clause = ' ON CONFLICT ("show_id", "genre") DO NOTHING'
                elif full_table_name == "silver.silver_directors":
                    conflict_clause = ' ON CONFLICT ("show_id", "director") DO NOTHING'
                elif full_table_name == "silver.silver_cast":
                    conflict_clause = ' ON CONFLICT ("show_id", "cast_member") DO NOTHING'
                elif full_table_name == "gold.dim_title":
                    update_cols = [f'"{c}" = EXCLUDED."{c}"' for c in db_cols if c != "title_key"]
                    conflict_clause = f' ON CONFLICT ("title_key") DO UPDATE SET {", ".join(update_cols)}'
                elif full_table_name == "gold.dim_director":
                    conflict_clause = ' ON CONFLICT ("director_key") DO NOTHING'
                elif full_table_name == "gold.dim_country":
                    conflict_clause = ' ON CONFLICT ("country_key") DO NOTHING'
                elif full_table_name == "gold.dim_genre":
                    conflict_clause = ' ON CONFLICT ("genre_key") DO NOTHING'
                elif full_table_name == "gold.dim_rating":
                    conflict_clause = ' ON CONFLICT ("rating_key") DO NOTHING'
                elif full_table_name == "gold.dim_type":
                    conflict_clause = ' ON CONFLICT ("type_key") DO NOTHING'
                elif full_table_name == "gold.dim_date":
                    conflict_clause = ' ON CONFLICT ("date_key") DO NOTHING'
                elif full_table_name == "gold.fact_content":
                    conflict_clause = ' ON CONFLICT ("title_key", "director_key", "country_key", "genre_key", "rating_key", "type_key", "date_key") DO NOTHING'

            query = f'INSERT INTO "{schema}"."{table}" ({cols_str}) VALUES %s{conflict_clause}'
            tuples = [tuple(x) for x in df.values]
            
            execute_values(cursor, query, tuples)
            conn.commit()
            print(f"  Successfully loaded/UPSERTed {len(tuples)} rows into {schema}.{table}")
            
        print("Data loaded to database tables successfully.\n")
    except Exception as e:
        print("Error loading Parquet to PostgreSQL:", e)
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def load_audit_metadata():
    print("Step 5: Loading metadata log audits to PostgreSQL...")
    conn = get_db_connection()
    cursor = conn.cursor()
    
    metadata_dir = os.path.join(project_root, "data", "metadata")
    runs_file = os.path.join(metadata_dir, "pipeline_runs_audit.json")
    steps_file = os.path.join(metadata_dir, "pipeline_steps_audit.json")
    watermarks_file = os.path.join(metadata_dir, "watermarks.json")
    
    try:
        # Load Runs
        if os.path.exists(runs_file):
            with open(runs_file, "r") as f:
                runs = json.load(f)
            for r in runs:
                cursor.execute("""
                    INSERT INTO metadata.pipeline_runs (
                        pipeline_run_id, pipeline_name, batch_id, execution_mode, start_time, end_time, 
                        execution_duration_ms, execution_status, execution_host, spark_application_id,
                        watermark_timestamp, rows_read, rows_inserted, rows_updated, rows_skipped, rows_rejected, error_details
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pipeline_run_id) DO UPDATE SET 
                        execution_mode = EXCLUDED.execution_mode,
                        end_time = EXCLUDED.end_time,
                        execution_duration_ms = EXCLUDED.execution_duration_ms,
                        execution_status = EXCLUDED.execution_status,
                        spark_application_id = EXCLUDED.spark_application_id,
                        watermark_timestamp = EXCLUDED.watermark_timestamp,
                        rows_read = EXCLUDED.rows_read,
                        rows_inserted = EXCLUDED.rows_inserted,
                        rows_updated = EXCLUDED.rows_updated,
                        rows_skipped = EXCLUDED.rows_skipped,
                        rows_rejected = EXCLUDED.rows_rejected,
                        error_details = EXCLUDED.error_details;
                """, (
                    r.get("pipeline_run_id"), r.get("pipeline_name"), r.get("batch_id"),
                    r.get("execution_mode", "INCREMENTAL"), r.get("start_time"), r.get("end_time"),
                    r.get("execution_duration_ms"), r.get("execution_status"), r.get("execution_host"),
                    r.get("spark_application_id"), r.get("watermark_timestamp"), r.get("rows_read", 0),
                    r.get("rows_inserted", 0), r.get("rows_updated", 0), r.get("rows_skipped", 0),
                    r.get("rows_rejected", 0), r.get("error_details")
                ))
            conn.commit()
            print(f"  Loaded {len(runs)} run details to metadata.pipeline_runs")
            
        # Load Steps
        if os.path.exists(steps_file):
            with open(steps_file, "r") as f:
                steps = json.load(f)
            for s in steps:
                cursor.execute("""
                    INSERT INTO metadata.pipeline_steps (
                        pipeline_run_id, pipeline_stage, start_time, end_time, execution_duration_ms, execution_status,
                        records_read, records_written, records_inserted, records_updated, records_deleted, records_rejected,
                        duplicate_count, validation_failures, error_message
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    s.get("pipeline_run_id"), s.get("pipeline_stage"), s.get("start_time"), s.get("end_time"),
                    s.get("execution_duration_ms"), s.get("execution_status"), s.get("records_read", 0),
                    s.get("records_written", 0), s.get("records_inserted", 0), s.get("records_updated", 0),
                    s.get("records_deleted", 0), s.get("records_rejected", 0), s.get("duplicate_count", 0),
                    s.get("validation_failures", 0), s.get("error_message")
                ))
            conn.commit()
            print(f"  Loaded {len(steps)} step logs to metadata.pipeline_steps")
            
        # Load Watermarks
        if os.path.exists(watermarks_file):
            with open(watermarks_file, "r") as f:
                watermarks = json.load(f)
            for p_name, w in watermarks.items():
                cursor.execute("""
                    INSERT INTO metadata.pipeline_watermarks (
                        pipeline_name, last_processed_timestamp, last_successful_batch_id, last_business_key, last_run_duration_ms, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pipeline_name) DO UPDATE SET
                        last_processed_timestamp = EXCLUDED.last_processed_timestamp,
                        last_successful_batch_id = EXCLUDED.last_successful_batch_id,
                        last_business_key = EXCLUDED.last_business_key,
                        last_run_duration_ms = EXCLUDED.last_run_duration_ms,
                        updated_at = EXCLUDED.updated_at;
                """, (
                    w.get("pipeline_name"), w.get("last_processed_timestamp"),
                    w.get("last_successful_batch"), w.get("last_business_key", "show_id"),
                    w.get("last_run_time_ms"), w.get("updated_at")
                ))
            conn.commit()
            print(f"  Loaded watermarks to metadata.pipeline_watermarks")
            
        print("Metadata log audits loaded completed successfully.\n")
    except Exception as e:
        print("Error loading metadata audits:", e)
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def recreate_views():
    print("Step 6: Refreshing SQL Views...")
    views_path = os.path.join(project_root, "src", "database", "powerbi_views.sql")
    if not os.path.exists(views_path):
        print(f"  Warning: Views SQL file {views_path} not found.")
        return
        
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        with open(views_path, "r", encoding="utf-8") as f:
            sql = f.read()
            
        cursor.execute(sql)
        conn.commit()
        print("  Successfully executed powerbi_views.sql script.")
        print("SQL Views refreshed.\n")
    except Exception as e:
        print("Error refreshing SQL Views:", e)
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()

def run_validation_report():
    print("Step 7: Validating final PostgreSQL row counts:")
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        tables = [
            "bronze.bronze_netflix_csv",
            "silver.silver_titles",
            "silver.silver_titles_scd2",
            "silver.silver_country",
            "silver.silver_genres",
            "silver.silver_directors",
            "silver.silver_cast",
            "gold.dim_title",
            "gold.dim_director",
            "gold.dim_country",
            "gold.dim_genre",
            "gold.dim_rating",
            "gold.dim_type",
            "gold.dim_date",
            "gold.fact_content"
        ]
        for tbl in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {tbl};")
            count = cursor.fetchone()[0]
            print(f"  {tbl}: {count}")
    except Exception as e:
        print("Error running validation report:", e)
    finally:
        cursor.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Netflix Data Pipeline Reset & Reload Orchestrator")
    parser.add_argument("--mode", type=str, choices=["FULL", "INCREMENTAL"], help="Execution mode (FULL or INCREMENTAL)")
    args = parser.parse_args()
    
    config = ConfigLoader.load()
    mode = args.mode or config.get("pipeline", {}).get("execution_mode") or config.get("incremental_processing", {}).get("execution_mode", "INCREMENTAL")
    mode = mode.upper()
    
    print("="*60)
    print(f"NETFLIX DATA ENGINEERING PIPELINE RUNNER - MODE: {mode}")
    print("="*60)
    
    if mode == "FULL":
        reset_database()
        clean_local_storage()
        
    batch_id = run_etl_pipeline(mode=mode)
    load_parquet_to_postgres(mode=mode)
    load_audit_metadata()
    recreate_views()
    run_validation_report()
    
    print("="*60)
    print(f"PIPELINE EXECUTION ({mode} MODE) COMPLETED SUCCESSFULLY")
    print("="*60)

if __name__ == '__main__':
    main()
