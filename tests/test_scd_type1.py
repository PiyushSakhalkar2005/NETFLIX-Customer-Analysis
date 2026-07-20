import os
import sys
import pytest
import csv
from pyspark.sql import SparkSession

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

from src.silver.scd_type1 import ScdType1Processor

@pytest.fixture(scope="session")
def spark_session():
    """Provides a local Spark session for testing dimension merges."""
    spark = SparkSession.builder \
        .appName("TestScdType1") \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_scd_type1_merge(spark_session, tmp_path):
    # 1. Arrange: Create mock config, target path, and staging source datasets
    target_dir = tmp_path / "target_titles"
    os.makedirs(target_dir, exist_ok=True)
    
    # Target (Master) initial table (write to CSV and load via Spark Parquet to avoid executor crashes)
    target_csv = tmp_path / "initial_target.csv"
    with open(target_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "type", "title", "date_added", "release_year", "rating", "duration", "description", "content_age", "duration_minutes", "season_count", "is_recent_release", "batch_id", "pipeline_run_id", "ingestion_timestamp"])
        writer.writerows([
            # s1 (will be updated)
            ["s1", "Movie", "Initial Title", "2021-09-25", "2020", "PG", "90 min", "Desc s1", "6", "90", "", "False", "b1", "p1", "2026-07-14 00:00:00"],
            # s2 (remains unchanged)
            ["s2", "TV Show", "Unchanged Title", "2021-09-24", "2021", "TV-MA", "2 Seasons", "Desc s2", "5", "", "2", "True", "b1", "p1", "2026-07-14 00:00:00"]
        ])
    df_initial = spark_session.read.option("header", "true").option("nullValue", "").csv(str(target_csv))
    from pyspark.sql.functions import col
    df_initial = df_initial \
        .withColumn("release_year", col("release_year").cast("integer")) \
        .withColumn("duration_minutes", col("duration_minutes").cast("integer")) \
        .withColumn("season_count", col("season_count").cast("integer")) \
        .withColumn("content_age", col("content_age").cast("integer"))
    df_initial.write.mode("overwrite").format("parquet").partitionBy("release_year").save(str(target_dir))
    
    # Source (Incremental Staging) batch containing 1 Insert, 1 Update, 1 Unchanged
    source_csv = tmp_path / "source_batch.csv"
    with open(source_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "type", "title", "date_added", "release_year", "rating", "duration", "description", "content_age", "duration_minutes", "season_count", "is_recent_release", "batch_id", "pipeline_run_id", "ingestion_timestamp"])
        writer.writerows([
            # s1 (Update: title and rating change)
            ["s1", "Movie", "Updated Title", "2021-09-25", "2020", "PG-13", "90 min", "Desc s1", "6", "90", "", "False", "b2", "p2", "2026-07-14 00:00:00"],
            # s2 (Unchanged: same values as target)
            ["s2", "TV Show", "Unchanged Title", "2021-09-24", "2021", "TV-MA", "2 Seasons", "Desc s2", "5", "", "2", "True", "b1", "p1", "2026-07-14 00:00:00"],
            # s3 (New Insert)
            ["s3", "Movie", "New Title", "2021-09-23", "2022", "R", "100 min", "Desc s3", "4", "100", "", "True", "b2", "p2", "2026-07-14 00:00:00"]
        ])
    df_source = spark_session.read.option("header", "true").option("nullValue", "").csv(str(source_csv))
    df_source = df_source \
        .withColumn("release_year", col("release_year").cast("integer")) \
        .withColumn("duration_minutes", col("duration_minutes").cast("integer")) \
        .withColumn("season_count", col("season_count").cast("integer")) \
        .withColumn("content_age", col("content_age").cast("integer"))
        
    mock_config = {
        "scd_type1": {
            "business_key": "show_id",
            "compare_columns": ["type", "title", "date_added", "release_year", "rating", "duration", "description"]
        }
    }
    
    # 2. Act: Execute SCD Type 1 Merge
    processor = ScdType1Processor(spark_session, config=mock_config)
    run_stats = processor.merge_titles(df_source, str(target_dir))
    
    # 3. Assert stats mapping
    assert run_stats["mode"] == "MERGE_UPDATE"
    assert run_stats["records_inserted"] == 1 # s3
    assert run_stats["records_updated"] == 1  # s1
    assert run_stats["records_unchanged"] == 1 # s2
    
    # 4. Assert Master Target values
    df_merged = spark_session.read.parquet(str(target_dir))
    assert df_merged.count() == 3
    
    # Check s1 values (overwritten fields)
    row_s1 = df_merged.filter("show_id = 's1'").first()
    assert row_s1["title"] == "Updated Title"
    assert row_s1["rating"] == "PG-13"
    assert row_s1["batch_id"] == "b2" # audit column overwritten for updates
    
    # Check s2 values (unchanged)
    row_s2 = df_merged.filter("show_id = 's2'").first()
    assert row_s2["title"] == "Unchanged Title"
    assert row_s2["rating"] == "TV-MA"
    assert row_s2["batch_id"] == "b1" # original audit columns preserved
    
    # Check s3 values (inserted)
    row_s3 = df_merged.filter("show_id = 's3'").first()
    assert row_s3["title"] == "New Title"
    assert row_s3["rating"] == "R"
    assert row_s3["batch_id"] == "b2"
