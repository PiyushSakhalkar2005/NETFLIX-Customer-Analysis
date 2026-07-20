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

from src.transformations.business_transforms import BusinessTransforms

@pytest.fixture(scope="session")
def spark_session():
    """Provides a local Spark session for testing business transformations."""
    spark = SparkSession.builder \
        .appName("TestBusinessTransforms") \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_business_transforms(spark_session, tmp_path):
    # 1. Arrange: Create mock Silver tables and output Gold directory paths
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    os.makedirs(silver_dir, exist_ok=True)
    os.makedirs(gold_dir, exist_ok=True)
    
    # Define test data helpers. We write them to CSV and convert to Parquet to avoid Python executor workers.
    # - silver_titles
    titles_csv = tmp_path / "silver_titles.csv"
    with open(titles_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "type", "title", "date_added", "release_year", "rating", "duration", "description", "content_age", "duration_minutes", "season_count", "is_recent_release", "batch_id", "pipeline_run_id", "ingestion_timestamp"])
        writer.writerows([
            ["s1", "Movie", "Movie 1", "2021-09-25", "2020", "PG-13", "90 min", "Desc 1", "6", "90", "", "False", "b1", "p1", "2026-07-14 00:00:00"],
            ["s2", "TV Show", "Show 2", "2021-09-24", "2021", "TV-MA", "2 Seasons", "Desc 2", "5", "", "2", "True", "b1", "p1", "2026-07-14 00:00:00"],
            ["s3", "Movie", "Movie 3", "2021-09-23", "2019", "R", "120 min", "Desc 3", "7", "120", "", "False", "b1", "p1", "2026-07-14 00:00:00"],
            ["s4", "Movie", "Movie 4", "2021-09-22", "1998", "R", "110 min", "Desc 4", "28", "110", "", "False", "b1", "p1", "2026-07-14 00:00:00"] # Classic Movie
        ])
    df_titles = spark_session.read.option("header", "true").option("nullValue", "").csv(str(titles_csv))
    # Cast integers for derived window operations
    from pyspark.sql.functions import col
    df_titles = df_titles \
        .withColumn("release_year", col("release_year").cast("integer")) \
        .withColumn("duration_minutes", col("duration_minutes").cast("integer")) \
        .withColumn("season_count", col("season_count").cast("integer")) \
        .withColumn("content_age", col("content_age").cast("integer"))
    df_titles.write.mode("overwrite").format("parquet").save(os.path.join(silver_dir, "silver_titles"))
    
    # - silver_country
    country_csv = tmp_path / "silver_country.csv"
    with open(country_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "country"])
        writer.writerows([
            ["s1", "United States"],
            ["s2", "South Africa"],
            ["s2", "United States"],
            ["s3", "United States"],
            ["s4", "India"]
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
            ["s2", "Mysteries"],
            ["s3", "Comedies"],
            ["s4", "Action"]
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
            ["s3", "Director A"],
            ["s4", "Director C"]
        ])
    df_directors = spark_session.read.option("header", "true").csv(str(directors_csv))
    df_directors.write.mode("overwrite").format("parquet").save(os.path.join(silver_dir, "silver_directors"))
    
    # - silver_cast (dummy table to satisfy verify check)
    cast_csv = tmp_path / "silver_cast.csv"
    with open(cast_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["show_id", "cast_member"])
        writer.writerows([
            ["s1", "Actor A"]
        ])
    df_cast = spark_session.read.option("header", "true").csv(str(cast_csv))
    df_cast.write.mode("overwrite").format("parquet").save(os.path.join(silver_dir, "silver_cast"))
    
    mock_config = {
        "storage": {
            "silver_dir": str(silver_dir),
            "gold_dir": str(gold_dir)
        }
    }
    
    # 2. Act: Run business transformations
    transforms = BusinessTransforms(spark_session, config=mock_config)
    run_stats = transforms.execute_transforms(batch_id="test-batch-1")
    
    # 3. Assert execution stats
    assert run_stats["status"] == "SUCCESS"
    assert run_stats["records_processed"] == 4
    
    # 4. Assert KPI Summary values
    df_kpis = spark_session.read.parquet(os.path.join(gold_dir, "kpi_summary"))
    kpis = df_kpis.first()
    assert kpis["total_content"] == 4
    assert kpis["total_movies"] == 3
    assert kpis["total_tv_shows"] == 1
    assert kpis["avg_movie_duration_mins"] == 106.67 # (90+120+110)/3
    assert kpis["avg_tv_seasons"] == 2.0
    
    # 5. Assert Country distribution ranking
    df_countries_summary = spark_session.read.parquet(os.path.join(gold_dir, "country_distribution"))
    # US should be ranked first with count 3 (s1, s2, s3)
    us_row = df_countries_summary.filter("country = 'United States'").first()
    assert us_row["total_content"] == 3
    assert us_row["total_movies"] == 2
    assert us_row["total_tv_shows"] == 1
    
    # 6. Assert Director Rankings (Window Row Number)
    df_director_rank = spark_session.read.parquet(os.path.join(gold_dir, "director_rankings"))
    # Director A has 2 movies (s1, s3) -> rank 1
    first_rank_dir = df_director_rank.filter("director_rank = 1").first()
    assert first_rank_dir["director"] == "Director A"
    assert first_rank_dir["title_count"] == 2
    
    # 7. Assert Running releases (Window Sum)
    df_yearly = spark_session.read.parquet(os.path.join(gold_dir, "yearly_releases_summary"))
    # Year 1998 releases should be 1, running total should be 1
    row_1998 = df_yearly.filter("release_year = 1998").first()
    assert row_1998["yearly_releases"] == 1
    assert row_1998["running_total_releases"] == 1
    
    # Year 2021 releases should be 1, running total should be 4
    row_2021 = df_yearly.filter("release_year = 2021").first()
    assert row_2021["yearly_releases"] == 1
    assert row_2021["running_total_releases"] == 4
    
    # 8. Assert Decades and Classic classification logic
    df_enriched = spark_session.read.parquet(os.path.join(gold_dir, "gold_titles_enriched"))
    row_s4 = df_enriched.filter("show_id = 's4'").first()
    assert row_s4["release_decade"] == "1990s"
    assert row_s4["is_classic"] is True
    assert row_s4["content_category"] == "Classic"
    assert row_s4["duration_bucket"] == "Medium (60m-120m)"
