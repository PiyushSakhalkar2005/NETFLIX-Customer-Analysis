import os
import sys
import pytest
import csv
import json
from pyspark.sql import SparkSession
from datetime import datetime, date

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set HADOOP_HOME and PYTHONPATH for local Windows test run compatibility
hadoop_dir = os.path.join(project_root, "hadoop")
if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")
os.environ["PYTHONPATH"] = project_root + os.pathsep + os.environ.get("PYTHONPATH", "")

# Patch typing.io for Python 3.13+ compatibility with older PySpark versions
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

@pytest.fixture(scope="session")
def spark_session():
    """Provides a local Spark session for testing pipeline execution."""
    spark = SparkSession.builder \
        .appName("TestIncrementalEngine") \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_incremental_processing(spark_session, tmp_path):
    # 1. Arrange: Create storage directories for Bronze, Silver, Gold
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(silver_dir, exist_ok=True)
    os.makedirs(gold_dir, exist_ok=True)
    
    watermark_file = tmp_path / "watermarks.json"
    
    mock_config = {
        "storage": {
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir),
            "gold_dir": str(gold_dir)
        },
        "scd_type1": {
            "business_key": "show_id",
            "compare_columns": ["country", "genre", "director"]
        },
        "scd_type2": {
            "business_key": "show_id",
            "compare_columns": ["type", "title", "date_added", "release_year", "rating", "duration", "description"],
            "high_date": "9999-12-31"
        },
        "incremental_processing": {
            "pipeline_name": "test_netflix_pipeline",
            "load_type": "incremental",
            "watermark_file_path": str(watermark_file),
            "business_key": "show_id"
        }
    }
    
    # --- CHRONOLOGICAL STAGE 1: Write and process Batch 1 ---
    bronze_csv_1 = tmp_path / "bronze_titles_1.csv"
    with open(bronze_csv_1, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "type", "title", "director", "cast", "country", "date_added", "release_year", "rating", "duration", "listed_in", "description", "batch_id", "pipeline_run_id", "ingestion_timestamp", "source_file"])
        writer.writerows([
            ["s1", "Movie", "Title One", "Dir A", "Cast A", "United States", "September 25, 2021", "2020", "PG-13", "90 min", "Documentaries", "Desc One", "b1", "p1", "2026-07-14 00:00:00", "file1.csv"],
            ["s2", "TV Show", "Title Two", "Dir B", "Cast B", "India", "September 24, 2021", "2021", "TV-MA", "2 Seasons", "Dramas", "Desc Two", "b1", "p1", "2026-07-14 00:00:00", "file1.csv"]
        ])
    df_bronze_1 = spark_session.read.option("header", "true").option("nullValue", "").csv(str(bronze_csv_1))
    from pyspark.sql.functions import col
    df_bronze_1 = df_bronze_1 \
        .withColumn("release_year", col("release_year").cast("integer")) \
        .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))
    df_bronze_1.write.mode("overwrite").format("parquet").save(str(bronze_dir / "bronze_titles"))
    
    # Run pipeline as initial load (force full)
    engine = IncrementalEngine(spark_session, config=mock_config)
    run_1_stats = engine.run_pipeline(batch_id="b1", run_id="run-p1", force_full_load=True)
    
    assert run_1_stats["status"] == "SUCCESS"
    assert run_1_stats["records_processed"] == 2
    assert run_1_stats["inserted"] == 2
    assert run_1_stats["updated"] == 0
    
    # Verify watermark updated to Batch 1 timestamp
    watermark_mgr = WatermarkManager(config=mock_config)
    w1 = watermark_mgr.get_watermark()
    assert str(w1["last_processed_timestamp"]) != "1970-01-01T00:00:00"
    
    # --- CHRONOLOGICAL STAGE 2: Write and process Batch 2 (Incremental update) ---
    bronze_csv_2 = tmp_path / "bronze_titles_2.csv"
    with open(bronze_csv_2, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "type", "title", "director", "cast", "country", "date_added", "release_year", "rating", "duration", "listed_in", "description", "batch_id", "pipeline_run_id", "ingestion_timestamp", "source_file"])
        writer.writerows([
            # s3 (New show)
            ["s3", "Movie", "Title Three", "Dir A", "Cast C", "United States", "September 23, 2021", "2019", "R", "120 min", "Comedies", "Desc Three", "b2", "p2", "2026-07-14 06:00:00", "file2.csv"],
            # s1 (Update)
            ["s1", "Movie", "Title One Updated", "Dir A", "Cast A", "United States", "September 25, 2021", "2020", "PG-13", "90 min", "Documentaries", "Desc One", "b2", "p2", "2026-07-14 06:00:00", "file2.csv"]
        ])
    df_bronze_2 = spark_session.read.option("header", "true").option("nullValue", "").csv(str(bronze_csv_2))
    df_bronze_2 = df_bronze_2 \
        .withColumn("release_year", col("release_year").cast("integer")) \
        .withColumn("ingestion_timestamp", col("ingestion_timestamp").cast("timestamp"))
    # Write to Bronze overwriting the staging path
    df_bronze_2.write.mode("overwrite").format("parquet").save(str(bronze_dir / "bronze_titles"))
    
    # Run incremental pipeline
    run_2_stats = engine.run_pipeline(batch_id="b2", run_id="run-p2")
    
    # Assert Incremental Processing stats
    assert run_2_stats["status"] == "SUCCESS"
    assert run_2_stats["records_processed"] == 2 # s3 and s1 updated
    assert run_2_stats["inserted"] == 2          # s3 (Version 1) + s1 (Version 2)
    assert run_2_stats["updated"] == 1           # s1 Version 1 expired
    
    # Assert watermark updated to Batch 2 max timestamp
    w2 = watermark_mgr.get_watermark()
    assert str(w2["last_processed_timestamp"]) > str(w1["last_processed_timestamp"])
    
    # --- CHRONOLOGICAL STAGE 3: Run pipeline with no new data ---
    run_3_stats = engine.run_pipeline(batch_id="b3", run_id="run-p3")
    assert run_3_stats["status"] == "SKIPPED"
    assert run_3_stats["records_processed"] == 0
