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

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, xxhash64, when, lit, date_format, year, month, 
    dayofmonth, quarter, dayofweek, count, avg, round, broadcast, sum
)

from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader

logger = get_logger("GoldWarehouse")

class GoldWarehouseException(Exception):
    """Custom exception class for Gold Layer execution errors."""
    pass

class GoldWarehouse:
    """Consumes clean Silver relational tables and loads them into a star schema dimensional model."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any] = None):
        self.spark = spark
        self.config = config or ConfigLoader.load()
        
    def build_star_schema(self, batch_id: str, run_id: str) -> Dict[str, Any]:
        """
        Loads Silver tables, generates surrogate keys, builds Kimball Star Schema dimensions and facts,
        optimizes performance, and writes final Parquet databases.
        """
        logger.info(f"Initiating Gold Star Schema build for batch: {batch_id}")
        start_time = time.time()
        
        silver_dir = self.config.get("storage", {}).get("silver_dir")
        gold_dir = self.config.get("storage", {}).get("gold_dir")
        
        try:
            # 1. Read Silver relational DataFrames
            logger.info("Loading Silver tables...")
            df_silver_titles = self.spark.read.parquet(os.path.join(silver_dir, "silver_titles")).cache()
            df_silver_country = self.spark.read.parquet(os.path.join(silver_dir, "silver_country")).cache()
            df_silver_genres = self.spark.read.parquet(os.path.join(silver_dir, "silver_genres")).cache()
            df_silver_directors = self.spark.read.parquet(os.path.join(silver_dir, "silver_directors")).cache()
            
            records_received = df_silver_titles.count()
            logger.info(f"Silver Base Titles count: {records_received}")
            
            if records_received == 0:
                logger.warning("Silver Title dataset is empty. Gold build aborted.")
                return {"status": "SKIPPED", "records_inserted": 0}
                
            # 2. Build Dimensions
            logger.info("Generating dimension tables...")
            
            # Calculate classic and content category fields dynamically from release_year
            df_dim_title = df_silver_titles.select(
                xxhash64(col("show_id")).alias("title_key"),
                col("show_id"),
                col("title"),
                col("description"),
                (col("release_year") < 2000).alias("is_classic")
            ).withColumn("content_category", when(col("is_classic"), lit("Classic")).otherwise(lit("Modern"))).distinct()
            
            # dim_director
            df_dim_director = df_silver_directors.select(
                xxhash64(col("director")).alias("director_key"),
                col("director")
            ).distinct()
            
            # dim_country
            df_dim_country = df_silver_country.select(
                xxhash64(col("country")).alias("country_key"),
                col("country")
            ).distinct()
            
            # dim_genre
            df_dim_genre = df_silver_genres.select(
                xxhash64(col("genre")).alias("genre_key"),
                col("genre")
            ).distinct()
            
            # dim_rating
            df_dim_rating = df_silver_titles.select(
                xxhash64(col("rating")).alias("rating_key"),
                col("rating")
            ).distinct()
            
            # dim_type
            df_dim_type = df_silver_titles.select(
                xxhash64(col("type")).alias("type_key"),
                col("type")
            ).distinct()
            
            # dim_date (Stamps add dates with calendar fields)
            df_dates_raw = df_silver_titles.filter(col("date_added").isNotNull()).select("date_added").distinct()
            df_dim_date = df_dates_raw.select(
                date_format(col("date_added"), "yyyyMMdd").cast("integer").alias("date_key"),
                col("date_added"),
                year(col("date_added")).alias("year"),
                month(col("date_added")).alias("month"),
                dayofmonth(col("date_added")).alias("day"),
                quarter(col("date_added")).alias("quarter"),
                dayofweek(col("date_added")).alias("day_of_week")
            )
            
            # Insert a fallback Date Dimension record for handling null dates added
            # We construct the fallback row dynamically from the existing dates DataFrame to bypass Python executor calls
            fallback_date_row = df_dates_raw.limit(1).select(
                lit(-1).alias("date_key"),
                lit(None).cast("date").alias("date_added"),
                lit(None).cast("integer").alias("year"),
                lit(None).cast("integer").alias("month"),
                lit(None).cast("integer").alias("day"),
                lit(None).cast("integer").alias("quarter"),
                lit(None).cast("integer").alias("day_of_week")
            )
            df_dim_date = df_dim_date.unionByName(fallback_date_row)
            
            # 3. Build Fact Table (fact_content)
            logger.info("Assembling fully flattened Star Schema Fact Table...")
            
            # Join base titles with multi-value mappings
            df_fact_raw = df_silver_titles \
                .join(df_silver_country, "show_id") \
                .join(df_silver_genres, "show_id") \
                .join(df_silver_directors, "show_id")
                
            # Projection of surrogate key IDs and measures
            df_fact = df_fact_raw.select(
                xxhash64(col("show_id")).alias("title_key"),
                xxhash64(col("director")).alias("director_key"),
                xxhash64(col("country")).alias("country_key"),
                xxhash64(col("genre")).alias("genre_key"),
                xxhash64(col("rating")).alias("rating_key"),
                xxhash64(col("type")).alias("type_key"),
                when(col("date_added").isNotNull(), date_format(col("date_added"), "yyyyMMdd").cast("integer"))
                .otherwise(lit(-1)).alias("date_key"),
                col("duration_minutes"),
                col("season_count"),
                col("release_year"),
                col("content_age"),
                lit(batch_id).alias("batch_id"),
                lit(run_id).alias("pipeline_run_id"),
                col("ingestion_timestamp")
            )
            
            # Cache dimensions locally for fast lookup joins
            df_dim_title.cache()
            df_dim_director.cache()
            df_dim_country.cache()
            df_dim_genre.cache()
            df_dim_rating.cache()
            df_dim_type.cache()
            df_dim_date.cache()
            
            # 4. Write dimensional files to Gold Parquet storage
            logger.info("Writing Dimensions to Gold Storage...")
            os.makedirs(gold_dir, exist_ok=True)
            
            df_dim_title.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "dim_title"))
            df_dim_director.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "dim_director"))
            df_dim_country.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "dim_country"))
            df_dim_genre.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "dim_genre"))
            df_dim_rating.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "dim_rating"))
            df_dim_type.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "dim_type"))
            df_dim_date.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "dim_date"))
            
            # Write Fact Table partitioned by release_year
            fact_dest = os.path.join(gold_dir, "fact_content")
            logger.info(f"Writing Fact Table to Gold partitioned by release_year at: {fact_dest}")
            df_fact.write.mode("overwrite").format("parquet").partitionBy("release_year").save(fact_dest)
            
            # 5. Build and Export KPI Tables (Enterprise Analytics Summary tables)
            logger.info("Generating business KPI summary tables from Star Schema...")
            
            # Read back Gold data using broadcast joins to compute KPIs
            df_gold_fact = self.spark.read.parquet(fact_dest).cache()
            
            # KPI 1: Content Summary
            kpi_content_summary = df_gold_fact.groupBy().agg(
                count("title_key").alias("total_records"),
                count(when(col("type_key") == xxhash64(lit("Movie")), 1)).alias("total_movies"),
                count(when(col("type_key") == xxhash64(lit("TV Show")), 1)).alias("total_tv_shows"),
                round(avg("duration_minutes"), 2).alias("avg_movie_duration_mins"),
                round(avg("season_count"), 2).alias("avg_tv_seasons")
            )
            
            # KPI 2: Country Summary
            kpi_country_summary = df_gold_fact \
                .join(broadcast(df_dim_country), "country_key") \
                .groupBy("country") \
                .agg(count("title_key").alias("total_content")) \
                .orderBy(col("total_content").desc())
                
            # KPI 3: Genre Summary
            kpi_genre_summary = df_gold_fact \
                .join(broadcast(df_dim_genre), "genre_key") \
                .groupBy("genre") \
                .agg(count("title_key").alias("total_content")) \
                .orderBy(col("total_content").desc())
                
            # KPI 4: Release Summary
            kpi_release_summary = df_gold_fact.groupBy("release_year").agg(
                count("title_key").alias("yearly_releases")
            ).orderBy("release_year")
            
            # KPI 5: Rating Summary
            kpi_rating_summary = df_gold_fact \
                .join(broadcast(df_dim_rating), "rating_key") \
                .groupBy("rating") \
                .agg(count("title_key").alias("total_content")) \
                .orderBy(col("total_content").desc())
                
            # Write KPI tables
            kpi_content_summary.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "kpi_content_summary"))
            kpi_country_summary.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "kpi_country_summary"))
            kpi_genre_summary.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "kpi_genre_summary"))
            kpi_release_summary.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "kpi_release_summary"))
            kpi_rating_summary.write.mode("overwrite").format("parquet").save(os.path.join(gold_dir, "kpi_rating_summary"))
            
            # Clean up cache variables
            df_silver_titles.unpersist()
            df_silver_country.unpersist()
            df_silver_genres.unpersist()
            df_silver_directors.unpersist()
            df_dim_title.unpersist()
            df_dim_director.unpersist()
            df_dim_country.unpersist()
            df_dim_genre.unpersist()
            df_dim_rating.unpersist()
            df_dim_type.unpersist()
            df_dim_date.unpersist()
            df_gold_fact.unpersist()
            
            elapsed_time_ms = (time.time() - start_time) * 1000
            records_inserted = df_fact.count()
            logger.info(f"Gold Layer Star Schema built successfully in {elapsed_time_ms:.2f}ms. Records processed: {records_inserted}")
            
            return {
                "status": "SUCCESS",
                "records_inserted": records_inserted,
                "execution_time_ms": elapsed_time_ms
            }
            
        except Exception as e:
            logger.error(f"Gold Layer dimensional build failed: {str(e)}", exc_info=True)
            raise GoldWarehouseException(f"Gold Layer Star Schema build failed: {str(e)}") from e
