import os
import sys
import time
from datetime import datetime
from typing import Dict, Any, Tuple

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
    col, trim, regexp_replace, initcap, to_date, year, current_date, 
    when, split, explode, lit, regexp_extract, current_timestamp
)

from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader

logger = get_logger("SilverTransformer")

class SilverTransformerException(Exception):
    """Custom exception class for Silver layer transformation operations."""
    pass

class SilverTransformer:
    """Consumes clean Bronze data, cleans, normalizes, and relationalizes it into Silver tables."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any] = None):
        self.spark = spark
        self.config = config or ConfigLoader.load()
        self.null_replacements = self.config.get("silver", {}).get("null_replacements", {})
        
    def transform_bronze_to_silver(self, source_name: str, batch_id: str) -> Dict[str, Any]:
        """
        Main execution pipeline for Silver Layer processing.
        Reads validated Bronze datasets, cleans values, normalizes comma-separated arrays,
        and saves normalized Parquet tables.
        """
        logger.info(f"Initiating Silver transformation for source: {source_name}")
        start_time = time.time()
        
        # Define paths
        bronze_dir = self.config.get("storage", {}).get("bronze_dir")
        silver_dir = self.config.get("storage", {}).get("silver_dir")
        
        bronze_path = os.path.join(bronze_dir, f"bronze_{source_name}")
        
        if not os.path.exists(bronze_path):
            raise SilverTransformerException(f"Bronze layer source directory not found: {bronze_path}")
            
        try:
            # 1. Read Bronze data
            df_bronze = self.spark.read.parquet(bronze_path)
            records_received = df_bronze.count()
            logger.info(f"Loaded {records_received} records from Bronze.")
            
            if records_received == 0:
                logger.warning("Bronze dataset is empty. Silver load aborted.")
                return {"status": "SKIPPED", "records_processed": 0}
                
            # 2. De-duplication on show_id
            df_dedup = df_bronze.dropDuplicates(["show_id"])
            duplicates_removed = records_received - df_dedup.count()
            logger.info(f"Removed {duplicates_removed} duplicate records.")
            
            # 3. Clean string columns (trim, remove double spaces, handle nulls)
            string_cols = ["title", "director", "cast", "country", "duration", "listed_in", "description"]
            df_cleaned = df_dedup
            
            nulls_handled = 0
            for c in string_cols:
                # Count original nulls
                null_count = df_cleaned.filter(col(c).isNull()).count()
                nulls_handled += null_count
                
                # Replace multiple spaces with a single space, and trim
                cleaned_col = trim(regexp_replace(col(c), r"\s+", " "))
                
                # Handle nulls
                repl_val = self.null_replacements.get(c, "Unknown")
                df_cleaned = df_cleaned.withColumn(
                    c, 
                    when(cleaned_col.isNull() | (cleaned_col == ""), lit(repl_val)).otherwise(cleaned_col)
                )
                
            # 4. Standardize Domain fields
            df_std = df_cleaned.withColumn(
                "type", 
                when(trim(col("type")).rlike("(?i)^tv show$"), lit("TV Show"))
                .when(trim(col("type")).rlike("(?i)^movie$"), lit("Movie"))
                .otherwise(initcap(trim(col("type"))))
            )
            
            # - rating standardization
            rating_repl = self.null_replacements.get("rating", "UR")
            df_std = df_std.withColumn(
                "rating", 
                when(col("rating").isNull() | (trim(col("rating")) == ""), lit(rating_repl))
                .otherwise(trim(col("rating")))
            )
            
            # - parse date_added to yyyy-MM-dd
            # raw values resemble: "September 24, 2021" or " September 24, 2021"
            df_std = df_std.withColumn(
                "parsed_date_added", 
                to_date(trim(col("date_added")), "MMMM d, yyyy")
            )
            
            # Move unparseable date added values to transform rejects
            df_failed_dates = df_std.filter(col("parsed_date_added").isNull() & col("date_added").isNotNull() & (trim(col("date_added")) != ""))
            failed_dates_count = df_failed_dates.count()
            
            if failed_dates_count > 0:
                base_dir = self.config.get("storage", {}).get("base_path", os.path.join(project_root, "data"))
                rejects_dir = os.path.join(base_dir, "transformation_rejects")
                os.makedirs(rejects_dir, exist_ok=True)
                reject_path = os.path.join(rejects_dir, f"rejects_batch_{batch_id}")
                logger.warning(f"Detected {failed_dates_count} records with invalid date_added format. Writing to rejects: {reject_path}")
                df_failed_dates \
                    .withColumn("reason_for_failure", lit("Unparseable date_added format")) \
                    .withColumn("rejected_at", current_timestamp()) \
                    .write.mode("overwrite").format("parquet").save(reject_path)
                    
            # Keep only records that parsed correctly or were originally blank/null
            df_valid = df_std.filter(col("parsed_date_added").isNotNull() | col("date_added").isNull() | (trim(col("date_added")) == ""))
            
            # Ensure release_year is integer
            df_valid = df_valid.withColumn("release_year", col("release_year").cast("integer"))
            
            # 5. Data Enrichment
            # content_age
            current_yr = datetime.now().year
            df_enriched = df_valid \
                .withColumn("content_age", lit(current_yr) - col("release_year")) \
                .withColumn("is_recent_release", col("release_year") >= (lit(current_yr) - 5))
                
            # duration parsing (Movies duration_minutes, TV Shows season_count)
            # Duration raw values are like "90 min" or "2 Seasons"
            df_enriched = df_enriched \
                .withColumn("duration_minutes", 
                            when(col("type") == "Movie", regexp_extract(col("duration"), r"(\d+)\s*min", 1).cast("integer"))
                            .otherwise(lit(None).cast("integer"))) \
                .withColumn("season_count", 
                            when(col("type") == "TV Show", regexp_extract(col("duration"), r"(\d+)\s*Season", 1).cast("integer"))
                            .otherwise(lit(None).cast("integer")))
                            
            # 6. Normalized Lookup Tables Generation
            # - silver_titles table (Fact Title table)
            df_titles = df_enriched.select(
                "show_id", "type", "title", 
                col("parsed_date_added").alias("date_added"), 
                "release_year", "rating", "duration", "description",
                "content_age", "duration_minutes", "season_count", "is_recent_release",
                "batch_id", "pipeline_run_id", "ingestion_timestamp"
            )
            
            # - silver_country map (normalizing comma-separated countries list)
            # Standardize country names to initcap (e.g. india -> India)
            df_country = df_enriched \
                .select("show_id", explode(split(col("country"), r",\s*")).alias("raw_country")) \
                .withColumn("country", initcap(trim(col("raw_country")))) \
                .select("show_id", "country") \
                .filter(col("country") != "")
                
            # - silver_genres map
            df_genres = df_enriched \
                .select("show_id", explode(split(col("listed_in"), r",\s*")).alias("genre")) \
                .withColumn("genre", trim(col("genre"))) \
                .select("show_id", "genre") \
                .filter(col("genre") != "")
                
            # - silver_directors map
            df_directors = df_enriched \
                .select("show_id", explode(split(col("director"), r",\s*")).alias("director")) \
                .withColumn("director", trim(col("director"))) \
                .select("show_id", "director") \
                .filter(col("director") != "")
                
            # - silver_cast map
            df_cast = df_enriched \
                .select("show_id", explode(split(col("cast"), r",\s*")).alias("cast_member")) \
                .withColumn("cast_member", trim(col("cast_member"))) \
                .select("show_id", "cast_member") \
                .filter(col("cast_member") != "")
                
            # 7. Partitioned writes to Parquet
            partition_col = self.config.get("silver", {}).get("partition_column", "release_year")
            
            # Write silver_titles
            titles_dest = os.path.join(silver_dir, "silver_titles")
            logger.info(f"Writing silver_titles Parquet partitioned by {partition_col} at: {titles_dest}")
            df_titles.write.mode("overwrite").format("parquet").partitionBy(partition_col).save(titles_dest)
            
            # Write normalizedlookup tables
            tables = {
                "silver_country": df_country,
                "silver_genres": df_genres,
                "silver_directors": df_directors,
                "silver_cast": df_cast
            }
            
            for tbl_name, tbl_df in tables.items():
                tbl_dest = os.path.join(silver_dir, tbl_name)
                logger.info(f"Writing normalized silver table {tbl_name} at: {tbl_dest}")
                tbl_df.write.mode("overwrite").format("parquet").save(tbl_dest)
                
            elapsed_time_ms = (time.time() - start_time) * 1000
            records_written = df_titles.count()
            
            logger.info(f"Silver transformation complete in {elapsed_time_ms:.2f}ms. Records processed: {records_written}")
            
            return {
                "status": "SUCCESS",
                "records_received": records_received,
                "records_written": records_written,
                "duplicates_removed": duplicates_removed,
                "nulls_handled": nulls_handled,
                "failed_dates_count": failed_dates_count,
                "execution_duration_ms": elapsed_time_ms
            }
            
        except Exception as e:
            logger.error(f"Silver transformation engine failed for source '{source_name}': {str(e)}", exc_info=True)
            raise SilverTransformerException(f"Silver Layer transformation failed: {str(e)}") from e

    def transform(self, df_bronze: DataFrame, dest_titles_dir: str, dest_country_dir: str, 
                  dest_genres_dir: str, dest_directors_dir: str, dest_cast_dir: str, 
                  batch_id: str = "staging_batch") -> Dict[str, Any]:
        """
        Processes an in-memory Bronze DataFrame and writes relational Silver tables to custom paths.
        Supports processing incremental delta batches.
        """
        logger.info("Executing relational transformations on Bronze DataFrame...")
        start_time = time.time()
        
        try:
            records_received = df_bronze.count()
            if records_received == 0:
                logger.warning("Bronze dataset is empty.")
                return {"status": "SKIPPED", "records_processed": 0}
                
            # Deduplication
            df_dedup = df_bronze.dropDuplicates(["show_id"])
            duplicates_removed = records_received - df_dedup.count()
            
            # Clean string columns
            string_cols = ["title", "director", "cast", "country", "duration", "listed_in", "description"]
            df_cleaned = df_dedup
            
            nulls_handled = 0
            for c in string_cols:
                null_count = df_cleaned.filter(col(c).isNull()).count()
                nulls_handled += null_count
                cleaned_col = trim(regexp_replace(col(c), r"\s+", " "))
                repl_val = self.null_replacements.get(c, "Unknown")
                df_cleaned = df_cleaned.withColumn(
                    c, 
                    when(cleaned_col.isNull() | (cleaned_col == ""), lit(repl_val)).otherwise(cleaned_col)
                )
                
            # Standardize Domain fields
            df_std = df_cleaned.withColumn(
                "type", 
                when(trim(col("type")).rlike("(?i)^tv show$"), lit("TV Show"))
                .when(trim(col("type")).rlike("(?i)^movie$"), lit("Movie"))
                .otherwise(initcap(trim(col("type"))))
            )
            
            rating_repl = self.null_replacements.get("rating", "UR")
            df_std = df_std.withColumn(
                "rating", 
                when(col("rating").isNull() | (trim(col("rating")) == ""), lit(rating_repl))
                .otherwise(trim(col("rating")))
            )
            
            df_std = df_std.withColumn(
                "parsed_date_added", 
                to_date(trim(col("date_added")), "MMMM d, yyyy")
            )
            
            # Rejects
            df_failed_dates = df_std.filter(col("parsed_date_added").isNull() & col("date_added").isNotNull() & (trim(col("date_added")) != ""))
            failed_dates_count = df_failed_dates.count()
            
            if failed_dates_count > 0:
                base_dir = self.config.get("storage", {}).get("base_path", os.path.join(project_root, "data"))
                rejects_dir = os.path.join(base_dir, "transformation_rejects")
                os.makedirs(rejects_dir, exist_ok=True)
                reject_path = os.path.join(rejects_dir, f"rejects_batch_{batch_id}")
                df_failed_dates \
                    .withColumn("reason_for_failure", lit("Unparseable date_added format")) \
                    .withColumn("rejected_at", current_timestamp()) \
                    .write.mode("overwrite").format("parquet").save(reject_path)
                    
            df_valid = df_std.filter(col("parsed_date_added").isNotNull() | col("date_added").isNull() | (trim(col("date_added")) == ""))
            df_valid = df_valid.withColumn("release_year", col("release_year").cast("integer"))
            
            # Enrichment
            current_yr = datetime.now().year
            df_enriched = df_valid \
                .withColumn("content_age", lit(current_yr) - col("release_year")) \
                .withColumn("is_recent_release", col("release_year") >= (lit(current_yr) - 5))
                
            df_enriched = df_enriched \
                .withColumn("duration_minutes", 
                            when(col("type") == "Movie", regexp_extract(col("duration"), r"(\d+)\s*min", 1).cast("integer"))
                            .otherwise(lit(None).cast("integer"))) \
                .withColumn("season_count", 
                            when(col("type") == "TV Show", regexp_extract(col("duration"), r"(\d+)\s*Season", 1).cast("integer"))
                            .otherwise(lit(None).cast("integer")))
                            
            # Relational splits
            df_titles = df_enriched.select(
                "show_id", "type", "title", 
                col("parsed_date_added").alias("date_added"), 
                "release_year", "rating", "duration", "description",
                "content_age", "duration_minutes", "season_count", "is_recent_release",
                "batch_id", "pipeline_run_id", "ingestion_timestamp"
            )
            
            df_country = df_enriched \
                .select("show_id", explode(split(col("country"), r",\s*")).alias("raw_country")) \
                .withColumn("country", initcap(trim(col("raw_country")))) \
                .select("show_id", "country") \
                .filter(col("country") != "")
                
            df_genres = df_enriched \
                .select("show_id", explode(split(col("listed_in"), r",\s*")).alias("genre")) \
                .withColumn("genre", trim(col("genre"))) \
                .select("show_id", "genre") \
                .filter(col("genre") != "")
                
            df_directors = df_enriched \
                .select("show_id", explode(split(col("director"), r",\s*")).alias("director")) \
                .withColumn("director", trim(col("director"))) \
                .select("show_id", "director") \
                .filter(col("director") != "")
                
            df_cast = df_enriched \
                .select("show_id", explode(split(col("cast"), r",\s*")).alias("cast_member")) \
                .withColumn("cast_member", trim(col("cast_member"))) \
                .select("show_id", "cast_member") \
                .filter(col("cast_member") != "")
                
            # Write to custom staging paths
            partition_col = self.config.get("silver", {}).get("partition_column", "release_year")
            df_titles.write.mode("overwrite").format("parquet").partitionBy(partition_col).save(dest_titles_dir)
            df_country.write.mode("overwrite").format("parquet").save(dest_country_dir)
            df_genres.write.mode("overwrite").format("parquet").save(dest_genres_dir)
            df_directors.write.mode("overwrite").format("parquet").save(dest_directors_dir)
            df_cast.write.mode("overwrite").format("parquet").save(dest_cast_dir)
            
            elapsed_time_ms = (time.time() - start_time) * 1000
            records_written = df_titles.count()
            
            return {
                "status": "SUCCESS",
                "records_received": records_received,
                "records_written": records_written,
                "duplicates_removed": duplicates_removed,
                "nulls_handled": nulls_handled,
                "failed_dates_count": failed_dates_count,
                "execution_duration_ms": elapsed_time_ms
            }
            
        except Exception as e:
            logger.error(f"In-memory Silver transformation failed: {str(e)}", exc_info=True)
            raise SilverTransformerException(f"Silver transform failed: {str(e)}") from e
