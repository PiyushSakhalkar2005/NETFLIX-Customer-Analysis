import os
import sys
import pytest
from pyspark.sql import SparkSession

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set HADOOP_HOME for local Windows test run compatibility
hadoop_dir = os.path.join(project_root, "hadoop")
if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

# Patch typing.io for Python 3.13+ compatibility with older PySpark versions
try:
    import typing.io
except ImportError:
    import types
    import typing
    typing_io = types.ModuleType("typing.io")
    typing_io.BinaryIO = typing.BinaryIO
    typing_io.TextIO = typing.TextIO
    sys.modules["typing.io"] = typing_io

from src.bronze.bronze_loader import BronzeLoader

@pytest.fixture(scope="session")
def spark_session():
    """Provides a local Spark session for unit testing."""
    spark = SparkSession.builder \
        .appName("TestBronzeLoader") \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_bronze_load_csv(spark_session, tmp_path):
    # 1. Arrange: Create temp source csv file and configuration mapping
    raw_dir = tmp_path / "raw"
    bronze_dir = tmp_path / "bronze"
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(bronze_dir, exist_ok=True)
    
    csv_file = raw_dir / "test_titles.csv"
    csv_content = (
        "show_id,type,title,director,release_year\n"
        "s1,Movie,Test Movie 1,Director A,2026\n"
        "s2,TV Show,Test Show 2,,2025\n"
    )
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write(csv_content)
        
    mock_config = {
        "pipeline": {
            "name": "test_netflix_pipeline"
        },
        "storage": {
            "raw_dir": str(raw_dir),
            "bronze_dir": str(bronze_dir)
        },
        "ingestion": {
            "sources": {
                "test_csv": {
                    "format": "csv",
                    "path": str(csv_file),
                    "load_type": "FULL",
                    "options": {
                        "header": "true",
                        "inferSchema": "false"
                    }
                }
            }
        }
    }
    
    # 2. Act: Execute BronzeLoader load process
    loader = BronzeLoader(spark_session, config=mock_config)
    batch_id = loader.load_source_to_bronze("test_csv", pipeline_run_id="run-12345")
    
    # 3. Assert: Verify files and data in the Bronze target path
    dest_path = os.path.join(bronze_dir, "bronze_test_csv")
    assert os.path.exists(dest_path)
    
    # Verify we can read parquet back
    df_result = spark_session.read.parquet(dest_path)
    
    # Check rows count
    assert df_result.count() == 2
    
    # Check original columns are preserved
    cols = df_result.columns
    assert "show_id" in cols
    assert "type" in cols
    assert "title" in cols
    assert "director" in cols
    assert "release_year" in cols
    
    # Check metadata columns are added correctly
    assert "batch_id" in cols
    assert "pipeline_run_id" in cols
    assert "ingestion_timestamp" in cols
    assert "source_system" in cols
    assert "source_file" in cols
    assert "load_type" in cols
    assert "ingestion_date" in cols
    
    # Verify values
    row_data = df_result.collect()
    for row in row_data:
        assert row["batch_id"] == batch_id
        assert row["pipeline_run_id"] == "run-12345"
        assert row["source_system"] == "test_netflix_pipeline"
        assert row["source_file"] == str(csv_file)
        assert row["load_type"] == "FULL"
        # Check partitioned folder structure works
        assert str(row["ingestion_date"]) in os.listdir(dest_path)[0] or any(str(row["ingestion_date"]) in f for f in os.listdir(dest_path))
