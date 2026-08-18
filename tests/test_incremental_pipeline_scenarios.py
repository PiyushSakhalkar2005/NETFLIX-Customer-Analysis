import os
import sys
import csv
import pytest
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, to_date, col, expr

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set HADOOP_HOME and PYTHONPATH for Windows environment
hadoop_dir = os.path.join(project_root, "hadoop")
if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")
os.environ["PYTHONPATH"] = project_root + os.pathsep + os.environ.get("PYTHONPATH", "")

# Fix PySpark worker environment variables for Windows Python 3.13
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Patch typing.io for Python 3.13 compatibility
import typing
if not hasattr(typing, "__path__"):
    typing.__path__ = []
try:
    import typing.io
except ImportError:
    import types
    typing_io = types.ModuleType("typing.io")
    typing_io.BinaryIO = typing.BinaryIO
    typing_io.TextIO = typing.TextIO
    sys.modules["typing.io"] = typing_io

from src.silver.incremental_engine import IncrementalEngine
from src.utils.watermark_manager import WatermarkManager
from src.utils.config_loader import ConfigLoader
from src.bronze.bronze_loader import BronzeLoader
from src.database.pipeline_reset_and_reload import get_db_connection, load_parquet_to_postgres, reset_database, clean_local_storage

@pytest.fixture(scope="module")
def spark_session():
    """Provides module-scoped PySpark session."""
    session = SparkSession.builder \
        .appName("NetflixIncrementalScenarioTests") \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]") \
        .getOrCreate()
    yield session
    session.stop()

def test_scenario_1_initial_full_load(spark_session):
    """Scenario 1: Initial Full Load -> Expected: 8807 records loaded."""
    config = ConfigLoader.load()
    
    # 1. Reset Database & Local Storage
    reset_database()
    clean_local_storage()
    
    # 2. Ingest raw CSV source to Bronze
    loader = BronzeLoader(spark_session, config=config)
    batch_id = loader.load_source_to_bronze(source_name="netflix_csv", pipeline_run_id="r-full-1")
    
    # 3. Run IncrementalEngine in FULL mode
    engine = IncrementalEngine(spark_session, config=config)
    results = engine.run_pipeline(batch_id=batch_id, run_id="r-full-1", force_full_load=True)
    
    assert results["status"] == "SUCCESS"
    assert results["records_processed"] == 8807
    
    # 4. Sync PostgreSQL in FULL mode
    load_parquet_to_postgres(mode="FULL")
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM silver.silver_titles;")
    count = cur.fetchone()[0]
    conn.close()
    
    assert count == 8807, f"Expected 8807 records in Postgres silver.silver_titles, got {count}"
    print(f"\n[PASSED] Scenario 1: Initial Full Load - 8807 records loaded successfully.")

def test_scenario_2_incremental_new_records(spark_session):
    """Scenario 2: Run Incremental with 10 new records -> Expected: Only 10 records inserted."""
    config = ConfigLoader.load()
    raw_dir = os.path.join(project_root, "data", "raw")
    inc_csv_path = os.path.join(raw_dir, "netflix_inc_10.csv")
    
    # Create 10 new titles in CSV
    new_rows = [["show_id", "type", "title", "director", "cast", "country", "date_added", "release_year", "rating", "duration", "listed_in", "description"]]
    for i in range(1, 11):
        show_id = f"s9000{i}"
        new_rows.append([
            show_id, "Movie", f"New Incremental Title {i}", "Test Director", "Test Cast",
            "United States", "August 3, 2026", "2026", "PG-13", "100 min", "Dramas",
            f"Description for incremental record {show_id}"
        ])
        
    with open(inc_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(new_rows)
        
    # Read raw CSV natively in Spark with explicit interval offset to guarantee watermark delta
    df_raw = spark_session.read.option("header", "true").csv(inc_csv_path)
    df_bronze_inc = df_raw \
        .withColumn("batch_id", lit("batch-inc-10")) \
        .withColumn("pipeline_run_id", lit("run-inc-10")) \
        .withColumn("ingestion_timestamp", expr("current_timestamp() + INTERVAL 10 SECONDS")) \
        .withColumn("source_system", lit("netflix_medallion_pipeline")) \
        .withColumn("source_file", lit(inc_csv_path)) \
        .withColumn("load_type", lit("INCREMENTAL")) \
        .withColumn("ingestion_date", to_date(current_timestamp()))
        
    bronze_csv_dir = os.path.join(project_root, "data", "bronze", "bronze_netflix_csv")
    df_bronze_inc.write.mode("append").format("parquet").partitionBy("ingestion_date").save(bronze_csv_dir)
    
    # Run incremental pipeline
    engine = IncrementalEngine(spark_session, config=config)
    results = engine.run_pipeline(batch_id="batch-inc-10", run_id="run-inc-10", force_full_load=False)
    
    assert results["status"] == "SUCCESS"
    assert results["records_processed"] == 10
    assert results["inserted"] == 10
    
    # Sync Postgres incrementally
    load_parquet_to_postgres(mode="INCREMENTAL")
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM silver.silver_titles;")
    count = cur.fetchone()[0]
    conn.close()
    
    assert count == 8817, f"Expected 8817 records in Postgres silver.silver_titles, got {count}"
    print(f"\n[PASSED] Scenario 2: Incremental Load - 10 new records inserted (Postgres Total: {count}).")

def test_scenario_3_incremental_no_duplicates(spark_session):
    """Scenario 3: Run Incremental Again -> Expected: 0 duplicate inserts."""
    config = ConfigLoader.load()
    engine = IncrementalEngine(spark_session, config=config)
    
    # Run incremental pipeline without adding new records
    results = engine.run_pipeline(batch_id="batch-inc-dup", run_id="run-inc-dup", force_full_load=False)
    
    assert results["status"] == "SKIPPED"
    assert results["records_processed"] == 0
    assert results["inserted"] == 0
    
    # Sync Postgres incrementally
    load_parquet_to_postgres(mode="INCREMENTAL")
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM silver.silver_titles;")
    count = cur.fetchone()[0]
    conn.close()
    
    assert count == 8817, f"Expected 8817 records, got {count}"
    print(f"\n[PASSED] Scenario 3: Incremental Re-run - 0 duplicate inserts (Postgres Total: {count}).")

def test_scenario_4_record_modification_scd(spark_session):
    """Scenario 4: Modify existing records -> Expected: SCD processing updates affected records correctly."""
    config = ConfigLoader.load()
    raw_dir = os.path.join(project_root, "data", "raw")
    mod_csv_path = os.path.join(raw_dir, "netflix_mod_s1.csv")
    
    mod_rows = [
        ["show_id", "type", "title", "director", "cast", "country", "date_added", "release_year", "rating", "duration", "listed_in", "description"],
        ["s1", "Movie", "Dick Johnson Is Dead - Special Director Cut", "Kirsten Johnson", "Unknown Cast", "United States", "September 25, 2020", "2020", "PG-13", "90 min", "Documentaries", "Remastered edition of Kirsten Johnson's film."]
    ]
    
    with open(mod_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerows(mod_rows)
        
    df_raw = spark_session.read.option("header", "true").csv(mod_csv_path)
    df_bronze_mod = df_raw \
        .withColumn("batch_id", lit("batch-mod-1")) \
        .withColumn("pipeline_run_id", lit("run-mod-1")) \
        .withColumn("ingestion_timestamp", expr("current_timestamp() + INTERVAL 30 SECONDS")) \
        .withColumn("source_system", lit("netflix_medallion_pipeline")) \
        .withColumn("source_file", lit(mod_csv_path)) \
        .withColumn("load_type", lit("INCREMENTAL")) \
        .withColumn("ingestion_date", to_date(current_timestamp()))
        
    bronze_csv_dir = os.path.join(project_root, "data", "bronze", "bronze_netflix_csv")
    df_bronze_mod.write.mode("append").format("parquet").partitionBy("ingestion_date").save(bronze_csv_dir)
    
    engine = IncrementalEngine(spark_session, config=config)
    results = engine.run_pipeline(batch_id="batch-mod-1", run_id="run-mod-1", force_full_load=False)
    
    assert results["status"] == "SUCCESS"
    assert results["records_processed"] == 1
    assert results["inserted"] == 1  # SCD2 active version
    assert results["updated"] == 1   # SCD2 expired version
    
    load_parquet_to_postgres(mode="INCREMENTAL")
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT title FROM silver.silver_titles WHERE show_id = 's1';")
    title = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM silver.silver_titles_scd2 WHERE show_id = 's1';")
    scd2_count = cur.fetchone()[0]
    conn.close()
    
    assert "Special Director Cut" in title, f"Expected updated title for s1, got {title}"
    assert scd2_count == 2, f"Expected 2 versions in SCD2 for s1, got {scd2_count}"
    print(f"\n[PASSED] Scenario 4: Record Modification - SCD updated record s1 correctly (Versions: {scd2_count}).")

def test_scenario_5_run_full_mode_again(spark_session):
    """Scenario 5: Run FULL mode again -> Expected: Complete rebuild succeeds without errors."""
    config = ConfigLoader.load()
    raw_dir = os.path.join(project_root, "data", "raw")
    
    # Clean temporary CSV files created for tests
    for temp_file in ["netflix_inc_10.csv", "netflix_mod_s1.csv"]:
        f_path = os.path.join(raw_dir, temp_file)
        if os.path.exists(f_path):
            os.remove(f_path)
            
    # 1. Reset Database & Local Storage
    reset_database()
    clean_local_storage()
    
    # 2. Ingest raw CSV source to Bronze
    loader = BronzeLoader(spark_session, config=config)
    batch_id = loader.load_source_to_bronze(source_name="netflix_csv", pipeline_run_id="r-full-2")
    
    # 3. Run IncrementalEngine in FULL mode
    engine = IncrementalEngine(spark_session, config=config)
    results = engine.run_pipeline(batch_id=batch_id, run_id="r-full-2", force_full_load=True)
    
    assert results["status"] == "SUCCESS"
    assert results["records_processed"] == 8807
    
    load_parquet_to_postgres(mode="FULL")
    
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM silver.silver_titles;")
    count = cur.fetchone()[0]
    conn.close()
    
    assert count == 8807, f"Expected 8807 records after FULL mode rebuild, got {count}"
    print(f"\n[PASSED] Scenario 5: Full Mode Rebuild - Rebuild succeeded with 8807 records.")

if __name__ == "__main__":
    pytest.main(["-s", "-v", __file__])
