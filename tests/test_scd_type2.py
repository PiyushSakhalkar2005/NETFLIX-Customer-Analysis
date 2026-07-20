import os
import sys
import pytest
import csv
from pyspark.sql import SparkSession
from datetime import datetime, date, timedelta

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

from src.silver.scd_type2 import ScdType2Processor

@pytest.fixture(scope="session")
def spark_session():
    """Provides a local Spark session for testing dimension merges."""
    spark = SparkSession.builder \
        .appName("TestScdType2") \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_scd_type2_merge(spark_session, tmp_path):
    # 1. Arrange: Create mock config, target path, and staging source datasets
    target_dir = tmp_path / "target_titles_scd2"
    os.makedirs(target_dir, exist_ok=True)
    
    # Target (Master) initial table (write to CSV and load via Spark Parquet to avoid executor crashes)
    target_csv = tmp_path / "initial_target_scd2.csv"
    with open(target_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "type", "title", "date_added", "release_year", "rating", "duration", "description", "content_age", "duration_minutes", "season_count", "is_recent_release", "batch_id", "pipeline_run_id", "ingestion_timestamp", "effective_start_date", "effective_end_date", "is_current", "version_number", "record_created_timestamp", "record_updated_timestamp"])
        writer.writerows([
            # s1 (will be updated and expired)
            ["s1", "Movie", "Initial Title", "2021-09-25", "2020", "PG", "90 min", "Desc s1", "6", "90", "", "False", "b1", "p1", "2026-07-14 00:00:00", "2026-07-01", "9999-12-31", "True", "1", "2026-07-01 00:00:00", "2026-07-01 00:00:00"],
            # s2 (remains unchanged)
            ["s2", "TV Show", "Unchanged Title", "2021-09-24", "2021", "TV-MA", "2 Seasons", "Desc s2", "5", "", "2", "True", "b1", "p1", "2026-07-14 00:00:00", "2026-07-01", "9999-12-31", "True", "1", "2026-07-01 00:00:00", "2026-07-01 00:00:00"]
        ])
    df_initial = spark_session.read.option("header", "true").option("nullValue", "").csv(str(target_csv))
    from pyspark.sql.functions import col
    df_initial = df_initial \
        .withColumn("release_year", col("release_year").cast("integer")) \
        .withColumn("duration_minutes", col("duration_minutes").cast("integer")) \
        .withColumn("season_count", col("season_count").cast("integer")) \
        .withColumn("content_age", col("content_age").cast("integer")) \
        .withColumn("effective_start_date", col("effective_start_date").cast("date")) \
        .withColumn("effective_end_date", col("effective_end_date").cast("date")) \
        .withColumn("is_current", col("is_current").cast("boolean")) \
        .withColumn("version_number", col("version_number").cast("integer")) \
        .withColumn("record_created_timestamp", col("record_created_timestamp").cast("timestamp")) \
        .withColumn("record_updated_timestamp", col("record_updated_timestamp").cast("timestamp"))
        
    df_initial.write.mode("overwrite").format("parquet").partitionBy("release_year").save(str(target_dir))
    
    # Source (Incremental Staging) batch containing 1 Insert, 1 Update, 1 Unchanged, and 1 Duplicate key
    source_csv = tmp_path / "source_batch_scd2.csv"
    with open(source_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "type", "title", "date_added", "release_year", "rating", "duration", "description", "content_age", "duration_minutes", "season_count", "is_recent_release", "batch_id", "pipeline_run_id", "ingestion_timestamp"])
        writer.writerows([
            # s1 (Update: title changes)
            ["s1", "Movie", "Updated Title", "2021-09-25", "2020", "PG", "90 min", "Desc s1", "6", "90", "", "False", "b2", "p2", "2026-07-14 00:00:00"],
            # s2 (Unchanged: same values as target)
            ["s2", "TV Show", "Unchanged Title", "2021-09-24", "2021", "TV-MA", "2 Seasons", "Desc s2", "5", "", "2", "True", "b1", "p1", "2026-07-14 00:00:00"],
            # s3 (New Insert)
            ["s3", "Movie", "New Title", "2021-09-23", "2022", "R", "100 min", "Desc s3", "4", "100", "", "True", "b2", "p2", "2026-07-14 00:00:00"],
            # Duplicate s3 in incoming batch to test duplicate key robustness
            ["s3", "Movie", "Duplicate New Title", "2021-09-23", "2022", "R", "100 min", "Desc s3", "4", "100", "", "True", "b2", "p2", "2026-07-14 00:00:00"]
        ])
    df_source = spark_session.read.option("header", "true").option("nullValue", "").csv(str(source_csv))
    df_source = df_source \
        .withColumn("release_year", col("release_year").cast("integer")) \
        .withColumn("duration_minutes", col("duration_minutes").cast("integer")) \
        .withColumn("season_count", col("season_count").cast("integer")) \
        .withColumn("content_age", col("content_age").cast("integer"))
        
    mock_config = {
        "scd_type2": {
            "business_key": "show_id",
            "compare_columns": ["type", "title", "date_added", "release_year", "rating", "duration", "description"],
            "high_date": "9999-12-31"
        }
    }
    
    # 2. Act: Execute SCD Type 2 Merge
    processor = ScdType2Processor(spark_session, config=mock_config)
    run_stats = processor.merge_titles(df_source, str(target_dir))
    
    # 3. Assert stats
    assert run_stats["mode"] == "MERGE_UPDATE"
    assert run_stats["records_inserted"] == 2 # s3 (Version 1) + s1 (Version 2)
    assert run_stats["records_expired"] == 1  # s1 (Version 1)
    
    # 4. Assert master target database values
    df_merged = spark_session.read.parquet(str(target_dir))
    # Total rows in history: s1 V1 (expired), s1 V2 (active), s2 V1 (unchanged), s3 V1 (inserted) = 4 rows
    assert df_merged.count() == 4
    
    # Verify s1 has two versions
    df_s1 = df_merged.filter("show_id = 's1'").orderBy("version_number")
    assert df_s1.count() == 2
    
    # Check s1 Version 1 (Expired historical record)
    s1_v1 = df_s1.first()
    assert s1_v1["version_number"] == 1
    assert s1_v1["is_current"] is False
    assert s1_v1["title"] == "Initial Title"
    assert str(s1_v1["effective_end_date"]) == str(date.today() - timedelta(days=1))
    
    # Check s1 Version 2 (Active updated record)
    s1_v2 = df_s1.collect()[1]
    assert s1_v2["version_number"] == 2
    assert s1_v2["is_current"] is True
    assert s1_v2["title"] == "Updated Title"
    assert str(s1_v2["effective_start_date"]) == str(date.today())
    assert str(s1_v2["effective_end_date"]) == "9999-12-31"
    
    # Check s2 (remains active unchanged version 1)
    row_s2 = df_merged.filter("show_id = 's2'").first()
    assert row_s2["version_number"] == 1
    assert row_s2["is_current"] is True
    assert row_s2["title"] == "Unchanged Title"
    assert str(row_s2["effective_start_date"]) == "2026-07-01"
    
    # Check s3 (inserted brand-new show as Version 1)
    row_s3 = df_merged.filter("show_id = 's3'").first()
    assert row_s3["version_number"] == 1
    assert row_s3["is_current"] is True
    assert row_s3["title"] == "New Title" # deduplicated row mapped correctly
    assert str(row_s3["effective_start_date"]) == str(date.today())
