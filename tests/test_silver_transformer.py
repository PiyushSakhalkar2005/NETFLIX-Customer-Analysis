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

from src.silver.silver_transformer import SilverTransformer

@pytest.fixture(scope="session")
def spark_session():
    """Provides a local Spark session for testing transformations."""
    spark = SparkSession.builder \
        .appName("TestSilverTransformer") \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_silver_transformations(spark_session, tmp_path):
    # 1. Arrange: Create directories and configuration rules
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(silver_dir, exist_ok=True)
    
    # We write mock data to CSV and load via Spark to save to Bronze Parquet first.
    # This matches exactly the Medallion flow!
    csv_file = tmp_path / "mock_bronze.csv"
    with open(csv_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "type", "title", "director", "cast", "country", "date_added", "release_year", "rating", "duration", "listed_in", "description"])
        writer.writerows([
            # Row 1: Valid Movie with extra whitespaces and casing issues to clean
            ["s1", " movie ", "  Dick Johnson  Is  Dead  ", "Kirsten Johnson", "", "  united  states  ", "September 25, 2021", "2020", "PG-13", "90 min", "Documentaries", "As her father nears..."],
            # Row 2: Duplicate show_id (s1) to check de-duplication
            ["s1", "movie", "Duplicate Dick Johnson", "Kirsten Johnson", "", "united states", "September 25, 2021", "2020", "PG-13", "90 min", "Documentaries", "As her father nears..."],
            # Row 3: Valid TV Show with multiple countries, directors, cast and genres to check lookup bridge mappings
            ["s2", "TV Show", "Blood & Water", "Director A, Director B", "Actor A, Actor B", "South Africa, USA", "September 24, 2021", "2021", "TV-MA", "2 Seasons", "Dramas, Mysteries", "After meeting..."],
            # Row 4: Row with null values to test defaults
            ["s3", "Movie", "Null Show", "", "", "", "", "2019", "", "120 min", "Comedies", "A simple comedies show."],
            # Row 5: Row with failed date added format to test transformation rejects isolation
            ["s4", "Movie", "Bad Date Show", "Director C", "", "India", "24/09/2021", "2021", "R", "100 min", "Action", "Invalid date format check"]
        ])
        
    df_raw = spark_session.read.option("header", "true").option("nullValue", "").csv(str(csv_file))
    
    # Enrich with mock Bronze metadata columns to match real ingestion schema
    from pyspark.sql.functions import lit, current_timestamp
    df_bronze = df_raw \
        .withColumn("batch_id", lit("run-99")) \
        .withColumn("pipeline_run_id", lit("pipeline-run-99")) \
        .withColumn("ingestion_timestamp", current_timestamp())
        
    # Save raw to simulated Bronze layer Parquet path
    bronze_source_path = os.path.join(bronze_dir, "bronze_test_source")
    df_bronze.write.mode("overwrite").format("parquet").save(bronze_source_path)
    
    mock_config = {
        "storage": {
            "base_path": str(tmp_path),
            "bronze_dir": str(bronze_dir),
            "silver_dir": str(silver_dir)
        },
        "silver": {
            "null_replacements": {
                "director": "Unknown Director",
                "cast": "Unknown Cast",
                "country": "Unknown Country",
                "rating": "UR",
                "duration": "0 min"
            },
            "partition_column": "release_year"
        }
    }
    
    # 2. Act: Run SilverTransformer
    transformer = SilverTransformer(spark_session, config=mock_config)
    stats = transformer.transform_bronze_to_silver("test_source", batch_id="run-99")
    
    # 3. Assert stats
    assert stats["status"] == "SUCCESS"
    assert stats["duplicates_removed"] == 1 # s1 duplicate dropped
    assert stats["failed_dates_count"] == 1 # s4 date unparseable
    assert stats["records_written"] == 3 # s1, s2, s3 passed valid checks
    
    # 4. Assert Silver Titles data structure and values
    titles_path = os.path.join(silver_dir, "silver_titles")
    assert os.path.exists(titles_path)
    df_titles = spark_session.read.parquet(titles_path)
    
    # Check row values
    row_s1 = df_titles.filter("show_id = 's1'").first()
    assert row_s1["type"] == "Movie"
    assert row_s1["title"] == "Dick Johnson Is Dead" # multiple spaces removed, trimmed
    assert str(row_s1["date_added"]) == "2021-09-25" # parsed correctly
    assert row_s1["duration_minutes"] == 90
    assert row_s1["season_count"] is None
    assert row_s1["is_recent_release"] is False # 2020 is not recent (< 2021)
    
    row_s2 = df_titles.filter("show_id = 's2'").first()
    assert row_s2["type"] == "TV Show"
    assert row_s2["season_count"] == 2
    assert row_s2["duration_minutes"] is None
    assert row_s2["is_recent_release"] is True # 2021 is recent (>= 2021)
    
    row_s3 = df_titles.filter("show_id = 's3'").first()
    assert row_s3["rating"] == "UR" # handled null replacement
    assert row_s3["date_added"] is None # empty string date parsed to null
    
    # 5. Assert bridge/normalized mapping tables values
    # - Countries
    df_countries = spark_session.read.parquet(os.path.join(silver_dir, "silver_country"))
    # s1 -> United States (initcap applied, trimmed)
    assert df_countries.filter("show_id = 's1'").first()["country"] == "United States"
    # s2 -> South Africa, USA exploded into 2 rows
    assert df_countries.filter("show_id = 's2'").count() == 2
    assert df_countries.filter("show_id = 's2' AND country = 'South Africa'").count() == 1
    assert df_countries.filter("show_id = 's2' AND country = 'Usa'").count() == 1
    
    # - Cast members
    df_cast = spark_session.read.parquet(os.path.join(silver_dir, "silver_cast"))
    assert df_cast.filter("show_id = 's2'").count() == 2
    assert df_cast.filter("show_id = 's2' AND cast_member = 'Actor A'").count() == 1
    assert df_cast.filter("show_id = 's2' AND cast_member = 'Actor B'").count() == 1
    
    # - Genres
    df_genres = spark_session.read.parquet(os.path.join(silver_dir, "silver_genres"))
    assert df_genres.filter("show_id = 's2'").count() == 2
    
    # 6. Verify rejected files
    rejects_dir = os.path.join(tmp_path, "transformation_rejects")
    assert os.path.exists(rejects_dir)
    df_rejects = spark_session.read.parquet(os.path.join(rejects_dir, "rejects_batch_run-99"))
    assert df_rejects.count() == 1
    assert df_rejects.first()["show_id"] == "s4"
    assert df_rejects.first()["reason_for_failure"] == "Unparseable date_added format"
