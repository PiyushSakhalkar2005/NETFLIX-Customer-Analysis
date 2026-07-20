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

from src.gold.gold_warehouse import GoldWarehouse

@pytest.fixture(scope="session")
def spark_session():
    """Provides a local Spark session for testing Gold warehouse load."""
    spark = SparkSession.builder \
        .appName("TestGoldWarehouse") \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_gold_star_schema(spark_session, tmp_path):
    # 1. Arrange: Create mock Silver tables and output Gold directory paths
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    os.makedirs(silver_dir, exist_ok=True)
    os.makedirs(gold_dir, exist_ok=True)
    
    # Write mock Silver datasets to CSV and load via Spark Parquet to avoid executor crashes
    # - silver_titles
    titles_csv = tmp_path / "silver_titles.csv"
    with open(titles_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "type", "title", "date_added", "release_year", "rating", "duration", "description", "content_age", "duration_minutes", "season_count", "is_recent_release", "batch_id", "pipeline_run_id", "ingestion_timestamp"])
        writer.writerows([
            ["s1", "Movie", "Movie 1", "2021-09-25", "2020", "PG-13", "90 min", "Desc 1", "6", "90", "", "False", "b1", "p1", "2026-07-14 00:00:00"],
            ["s2", "TV Show", "Show 2", "2021-09-24", "2021", "TV-MA", "2 Seasons", "Desc 2", "5", "", "2", "True", "b1", "p1", "2026-07-14 00:00:00"],
            # Title s3 has null date_added to verify fallback date key mapping
            ["s3", "Movie", "Movie 3", "", "2019", "R", "120 min", "Desc 3", "7", "120", "", "False", "b1", "p1", "2026-07-14 00:00:00"]
        ])
    df_titles = spark_session.read.option("header", "true").option("nullValue", "").csv(str(titles_csv))
    from pyspark.sql.functions import col
    df_titles = df_titles \
        .withColumn("release_year", col("release_year").cast("integer")) \
        .withColumn("duration_minutes", col("duration_minutes").cast("integer")) \
        .withColumn("season_count", col("season_count").cast("integer")) \
        .withColumn("content_age", col("content_age").cast("integer")) \
        .withColumn("date_added", col("date_added").cast("date"))
    df_titles.write.mode("overwrite").format("parquet").save(os.path.join(silver_dir, "silver_titles"))
    
    # - silver_country
    country_csv = tmp_path / "silver_country.csv"
    with open(country_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "country"])
        writer.writerows([
            ["s1", "United States"],
            ["s2", "South Africa"],
            ["s3", "United States"]
        ])
    df_country = spark_session.read.option("header", "true").csv(str(country_csv))
    df_country.write.mode("overwrite").format("parquet").save(os.path.join(silver_dir, "silver_country"))
    
    # - silver_genres
    genres_csv = tmp_path / "silver_genres.csv"
    with open(genres_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "genre"])
        writer.writerows([
            ["s1", "Documentaries"],
            ["s2", "Dramas"],
            ["s3", "Comedies"]
        ])
    df_genres = spark_session.read.option("header", "true").csv(str(genres_csv))
    df_genres.write.mode("overwrite").format("parquet").save(os.path.join(silver_dir, "silver_genres"))
    
    # - silver_directors
    directors_csv = tmp_path / "silver_directors.csv"
    with open(directors_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "director"])
        writer.writerows([
            ["s1", "Director A"],
            ["s2", "Director B"],
            ["s3", "Director A"]
        ])
    df_directors = spark_session.read.option("header", "true").csv(str(directors_csv))
    df_directors.write.mode("overwrite").format("parquet").save(os.path.join(silver_dir, "silver_directors"))
    
    mock_config = {
        "storage": {
            "silver_dir": str(silver_dir),
            "gold_dir": str(gold_dir)
        }
    }
    
    # 2. Act: Build Gold Star Schema
    warehouse = GoldWarehouse(spark_session, config=mock_config)
    run_stats = warehouse.build_star_schema(batch_id="batch-g1", run_id="run-g1")
    
    # 3. Assert execution stats
    assert run_stats["status"] == "SUCCESS"
    assert run_stats["records_inserted"] == 3
    
    # 4. Assert Dimensions are loaded
    df_dim_title = spark_session.read.parquet(os.path.join(gold_dir, "dim_title"))
    assert df_dim_title.count() == 3
    
    df_dim_director = spark_session.read.parquet(os.path.join(gold_dir, "dim_director"))
    assert df_dim_director.count() == 2 # Director A, Director B
    
    df_dim_country = spark_session.read.parquet(os.path.join(gold_dir, "dim_country"))
    assert df_dim_country.count() == 2 # USA, South Africa
    
    # Verify fallback Date Dimension row
    df_dim_date = spark_session.read.parquet(os.path.join(gold_dir, "dim_date"))
    fallback_row = df_dim_date.filter("date_key = -1").first()
    assert fallback_row is not None
    assert fallback_row["date_added"] is None
    
    # 5. Assert Fact Table values and surrogate keys linking
    df_fact = spark_session.read.parquet(os.path.join(gold_dir, "fact_content"))
    assert df_fact.count() == 3
    
    # Verify s1 key linking
    title_key_s1 = df_dim_title.filter("show_id = 's1'").first()["title_key"]
    director_key_s1 = df_dim_director.filter("director = 'Director A'").first()["director_key"]
    
    fact_s1 = df_fact.filter(f"title_key = {title_key_s1}").first()
    assert fact_s1["director_key"] == director_key_s1
    assert fact_s1["date_key"] == 20210925 # parsed correctly
    assert fact_s1["duration_minutes"] == 90
    assert fact_s1["season_count"] is None
    
    # Verify s3 date key maps to -1 fallback
    title_key_s3 = df_dim_title.filter("show_id = 's3'").first()["title_key"]
    fact_s3 = df_fact.filter(f"title_key = {title_key_s3}").first()
    assert fact_s3["date_key"] == -1
    
    # 6. Assert KPI summary tables
    df_kpi_content = spark_session.read.parquet(os.path.join(gold_dir, "kpi_content_summary"))
    kpis = df_kpi_content.first()
    assert kpis["total_records"] == 3
    assert kpis["total_movies"] == 2
    assert kpis["total_tv_shows"] == 1
    assert kpis["avg_movie_duration_mins"] == 105.00 # (90+120)/2
    assert kpis["avg_tv_seasons"] == 2.0
