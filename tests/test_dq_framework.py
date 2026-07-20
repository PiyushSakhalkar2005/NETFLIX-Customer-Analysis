import os
import sys
import pytest

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

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.quality.dq_framework import DataQualityFramework

@pytest.fixture(scope="session")
def spark_session():
    """Provides a local Spark session for validation testing."""
    spark = SparkSession.builder \
        .appName("TestDQFramework") \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_dq_validation_and_rejection(spark_session, tmp_path):
    # 1. Arrange: Create mock configuration rules file
    rules_file = tmp_path / "test_rules.yaml"
    rules_content = """
validation_rules:
  required_columns:
    - show_id
    - type
    - title
    - country
    - date_added
    - release_year
    - rating
    - duration
    - description

  null_checks:
    - show_id
    - title
    - type
    - release_year

  unique_checks:
    - show_id

  datatype_checks:
    release_year: "IntegerType"
    date_added: "date"

  value_sets:
    type:
      - Movie
      - TV Show
    rating:
      - PG
      - R

  business_rules:
    max_release_year: 2026
    min_title_length: 1

  completeness_checks:
    - country
    - description
"""
    with open(rules_file, "w", encoding="utf-8") as f:
        f.write(rules_content)
        
    # 2. Write mock data to CSV and load via Spark
    csv_file = tmp_path / "test_data.csv"
    import csv
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "type", "title", "country", "date_added", "release_year", "rating", "duration", "description"])
        writer.writerows([
            # Row 1: Fully Valid Movie
            ["s100", "Movie", "Good Show", "USA", "September 24, 2021", "2021", "PG", "90 min", "Excellent movie descriptive text"],
            # Row 2: Failed Null Check (Null Title / Empty title in CSV)
            ["s101", "Movie", "", "USA", "September 24, 2021", "2021", "PG", "90 min", "Excellent description"],
            # Row 3: Failed Domain check (Invalid Type "Game")
            ["s102", "Game", "Title Game", "Canada", "September 24, 2021", "2021", "PG", "90 min", "Excellent description"],
            # Row 4: Failed Business Rule (Future release_year)
            ["s103", "Movie", "Future Show", "India", "September 24, 2021", "2028", "PG", "90 min", "Excellent description"],
            # Row 5: Failed Completeness check (Empty country)
            ["s104", "Movie", "No Country", "", "September 24, 2021", "2021", "PG", "90 min", "Excellent description"]
        ])
        
    df = spark_session.read.option("header", "true").option("nullValue", "").csv(str(csv_file))
    
    # 2. Act: Instantiate DQ Framework and run validation
    dq = DataQualityFramework(spark_session, rules_path=str(rules_file))
    
    success, metrics = dq.validate_dataset(df, "test_source", "batch-999")
    
    # Assert GE stats
    assert success is False
    assert metrics["unsuccessful_expectations"] > 0
    
    # Extract rejected records
    rejected_dir = tmp_path / "rejected"
    passed_count, failed_count = dq.extract_and_save_rejected(df, str(rejected_dir), "batch-999")
    
    # 3. Assert count mapping
    assert passed_count == 1 # Only row 1 is fully valid
    assert failed_count == 4 # Rows 2, 3, 4, 5 fail checks
    
    # Check saved rejected Parquet structure
    rejected_parquet_path = os.path.join(rejected_dir, "rejected_batch_batch-999")
    assert os.path.exists(rejected_parquet_path)
    
    df_rejected = spark_session.read.parquet(rejected_parquet_path)
    assert df_rejected.count() == 4
    
    # Check reason columns
    rejected_rows = df_rejected.collect()
    for row in rejected_rows:
        if row["show_id"] == "s101":
            assert "Null value in required column 'title'" in row["reason_for_failure"]
        elif row["show_id"] == "s102":
            assert "Invalid type" in row["reason_for_failure"]
        elif row["show_id"] == "s103":
            assert "release_year cannot be in the future" in row["reason_for_failure"]
        elif row["show_id"] == "s104":
            assert "Required field 'country' is blank or empty" in row["reason_for_failure"]
            
        assert row["batch_id"] == "batch-999"
        assert row["validation_timestamp"] is not None
