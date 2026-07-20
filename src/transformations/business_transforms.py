import os
import sys
import time
from typing import Dict, Any

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

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

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col, count, avg, sum, when, lit, concat, floor, broadcast, 
    dense_rank, row_number, lag, round, coalesce, current_timestamp
)

from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader

logger = get_logger("BusinessTransforms")

class BusinessTransformsException(Exception):
    """Custom exception class for Business Transformation Layer errors."""
    pass

class BusinessTransforms:
    """Consumes clean Silver relational tables and executes advanced analytical business transformations."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any] = None):
        self.spark = spark
        self.config = config or ConfigLoader.load()
        
    def execute_transforms(self, batch_id: str) -> Dict[str, Any]:
        """
        Main runner that reads Silver tables, applies business transformations,
        optimizes execution using broadcasts/caching, and writes analytical results to Gold storage.
        """
        logger.info(f"Initiating Business Transformations for batch: {batch_id}")
        start_time = time.time()
        
        silver_dir = self.config.get("storage", {}).get("silver_dir")
        gold_dir = self.config.get("storage", {}).get("gold_dir")
        
        # Verify source silver tables
        required_tables = ["silver_titles", "silver_country", "silver_genres", "silver_directors", "silver_cast"]
        for tbl in required_tables:
            tbl_path = os.path.join(silver_dir, tbl)
            if not os.path.exists(tbl_path):
                raise BusinessTransformsException(f"Missing required Silver source table: {tbl_path}")
                
        try:
            # 1. Read Silver relational tables and optimize with Caching
            logger.info("Loading Silver tables into Spark DataFrames...")
            df_titles = self.spark.read.parquet(os.path.join(silver_dir, "silver_titles")).cache()
            df_country = self.spark.read.parquet(os.path.join(silver_dir, "silver_country")).cache()
            df_genres = self.spark.read.parquet(os.path.join(silver_dir, "silver_genres")).cache()
            df_directors = self.spark.read.parquet(os.path.join(silver_dir, "silver_directors")).cache()
            
            records_received = df_titles.count()
            logger.info(f"Loaded {records_received} base titles from Silver.")
            
            if records_received == 0:
                logger.warning("No records found in Silver Titles. Transformations aborted.")
                return {"status": "SKIPPED", "records_processed": 0}
                
            # 2. Derived Columns and Category Profiling
            # Adding decade, duration buckets, and classic tags
            logger.info("Computing derived analytical fields...")
            df_enriched_titles = df_titles \
                .withColumn("release_decade", concat(floor(col("release_year") / 10) * 10, lit("s"))) \
                .withColumn("is_classic", col("release_year") < 2000) \
                .withColumn("content_category", when(col("is_classic"), lit("Classic")).otherwise(lit("Modern"))) \
                .withColumn("duration_bucket", 
                            when(col("type") == "Movie", 
                                 when(col("duration_minutes") < 60, lit("Short (<60m)"))
                                 .when(col("duration_minutes") < 120, lit("Medium (60m-120m)"))
                                 .otherwise(lit("Long (>120m)")))
                            .otherwise(
                                 when(col("season_count") == 1, lit("Single Season"))
                                 .when(col("season_count") <= 3, lit("Mid-Length Series"))
                                 .otherwise(lit("Long Running Series"))
                            ))
            
            # 3. Aggregations & Metrics Calculation
            logger.info("Calculating KPI summary metrics...")
            kpi_summary = df_enriched_titles.groupBy().agg(
                count("show_id").alias("total_content"),
                count(when(col("type") == "Movie", 1)).alias("total_movies"),
                count(when(col("type") == "TV Show", 1)).alias("total_tv_shows"),
                round(avg("duration_minutes"), 2).alias("avg_movie_duration_mins"),
                round(avg("season_count"), 2).alias("avg_tv_seasons"),
                round(count(when(col("type") == "Movie", 1)) / count("show_id") * 100, 2).alias("movie_ratio_pct"),
                round(count(when(col("type") == "TV Show", 1)) / count("show_id") * 100, 2).alias("tv_ratio_pct")
            )
            
            # 4. Joins and Broadcast Optimization
            # For lookup tables, we broadcast them to join without a full shuffle
            logger.info("Executing Broadcast Joins for Country, Genre, and Director rankings...")
            df_titles_by_country = df_enriched_titles.join(broadcast(df_country), "show_id")
            df_titles_by_genre = df_enriched_titles.join(broadcast(df_genres), "show_id")
            df_titles_by_director = df_enriched_titles.join(broadcast(df_directors), "show_id")
            
            # 5. Content Counts by Dimension
            # Country summary
            country_distribution = df_titles_by_country.groupBy("country").agg(
                count("show_id").alias("total_content"),
                count(when(col("type") == "Movie", 1)).alias("total_movies"),
                count(when(col("type") == "TV Show", 1)).alias("total_tv_shows")
            ).orderBy(col("total_content").desc())
            
            # Genre summary
            genre_distribution = df_titles_by_genre.groupBy("genre").agg(
                count("show_id").alias("total_content"),
                count(when(col("type") == "Movie", 1)).alias("total_movies"),
                count(when(col("type") == "TV Show", 1)).alias("total_tv_shows")
            ).orderBy(col("total_content").desc())
            
            # Rating distribution
            rating_distribution = df_enriched_titles.groupBy("rating").agg(
                count("show_id").alias("total_content")
            ).orderBy(col("total_content").desc())
            
            # 6. Advanced Window Functions & Rankings
            # Window 1: Top N (Top 3) genres by country using dense_rank
            logger.info("Executing Window Dense Rank: Top 3 genres by country...")
            genre_by_country_counts = df_titles_by_country.join(broadcast(df_genres), "show_id") \
                .groupBy("country", "genre") \
                .agg(count("show_id").alias("genre_count"))
                
            window_genre = Window.partitionBy("country").orderBy(col("genre_count").desc())
            top_genres_by_country = genre_by_country_counts \
                .withColumn("genre_rank", dense_rank().over(window_genre)) \
                .filter(col("genre_rank") <= 3)
                
            # Window 2: Ranking of directors by number of titles using row_number
            logger.info("Executing Window Row Number: Ranking directors by total titles...")
            director_counts = df_titles_by_director \
                .filter(col("director") != "Unknown Director") \
                .groupBy("director") \
                .agg(count("show_id").alias("title_count"))
                
            window_director = Window.orderBy(col("title_count").desc(), col("director").asc())
            director_rankings = director_counts \
                .withColumn("director_rank", row_number().over(window_director))
                
            # Window 3: Yearly Growth and Running totals using lag and sum over preceding
            logger.info("Executing Window Running Total and Lag: Yearly growth rates...")
            yearly_counts = df_enriched_titles.groupBy("release_year").agg(
                count("show_id").alias("yearly_releases")
            )
            
            window_yearly = Window.orderBy("release_year")
            yearly_releases_summary = yearly_counts \
                .withColumn("running_total_releases", sum("yearly_releases").over(window_yearly)) \
                .withColumn("previous_year_releases", lag("yearly_releases", 1).over(window_yearly)) \
                .withColumn("yearly_growth_pct", 
                            round(
                                (col("yearly_releases") - coalesce(col("previous_year_releases"), col("yearly_releases"))) 
                                / coalesce(col("previous_year_releases"), col("yearly_releases")) * 100, 
                                2
                            )) \
                .select("release_year", "yearly_releases", "running_total_releases", "yearly_growth_pct")
                
            # 7. Write Gold Aggregations/KPIs
            logger.info("Writing business datasets to Gold Parquet storage...")
            os.makedirs(gold_dir, exist_ok=True)
            
            # Export structured data outputs
            kpi_summary.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "kpi_summary"))
            country_distribution.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "country_distribution"))
            genre_distribution.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "genre_distribution"))
            rating_distribution.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "rating_distribution"))
            top_genres_by_country.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "top_genres_by_country"))
            director_rankings.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "director_rankings"))
            yearly_releases_summary.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "yearly_releases_summary"))
            
            # Export enriched base titles (preserving partitioning for analytics)
            enriched_titles_dest = os.path.join(gold_dir, "gold_titles_enriched")
            df_enriched_titles.write.mode("overwrite").format("parquet").partitionBy("release_year").save(enriched_titles_dest)
            
            # Uncache DataFrames to free JVM memory
            df_titles.unpersist()
            df_country.unpersist()
            df_genres.unpersist()
            df_directors.unpersist()
            
            elapsed_time_ms = (time.time() - start_time) * 1000
            logger.info(f"Business transformations completed successfully in {elapsed_time_ms:.2f}ms.")
            
            return {
                "status": "SUCCESS",
                "records_processed": records_received,
                "execution_duration_ms": elapsed_time_ms,
                "joins_executed": ["Titles + Countries", "Titles + Genres", "Titles + Directors", "Titles + Genres + Countries"],
                "aggregations_completed": ["KPI Summary", "Country distribution", "Genre distribution", "Rating distribution", "Top genres by country", "Director rankings", "Yearly growth summary"]
            }
            
        except Exception as e:
            logger.error(f"Transformation engine execution failed: {str(e)}", exc_info=True)
            raise BusinessTransformsException(f"Business transformations failed: {str(e)}") from e
