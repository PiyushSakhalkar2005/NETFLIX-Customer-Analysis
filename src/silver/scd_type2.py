import os
import sys
import time
from typing import Dict, Any, List

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
from pyspark.sql.functions import col, lit, current_timestamp, current_date, to_date, date_sub

from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader

logger = get_logger("ScdType2Processor")

class ScdType2Exception(Exception):
    """Custom exception class for SCD Type 2 merge pipeline errors."""
    pass

class ScdType2Processor:
    """Implements production-grade SCD Type 2 (history preservation, versioning) for Silver tables."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any] = None):
        self.spark = spark
        self.config = config or ConfigLoader.load()
        scd_conf = self.config.get("scd_type2", {})
        self.business_key = scd_conf.get("business_key", "show_id")
        self.compare_columns = scd_conf.get("compare_columns", [])
        self.high_date = scd_conf.get("high_date", "9999-12-31")
        
    def merge_titles(self, df_source: DataFrame, target_dir_path: str) -> Dict[str, Any]:
        """
        Merges incoming df_source into the target silver_titles Parquet directory using SCD Type 2 logic.
        Deduplicates incoming keys, expires matched rows with changes, inserts new active rows, and preserves history.
        """
        logger.info(f"Initiating SCD Type 2 merge for target directory: {target_dir_path}")
        start_time = time.time()
        
        # Initial Load check
        is_initial_load = not os.path.exists(target_dir_path) or not any(
            f.endswith(".parquet") for root, dirs, files in os.walk(target_dir_path) for f in files
        )
        
        if not is_initial_load:
            try:
                target_schema = self.spark.read.parquet(target_dir_path).schema
                if "is_current" not in target_schema.names or "version_number" not in target_schema.names:
                    logger.warning("Target table exists but lacks SCD Type 2 columns. Treating as Initial Load.")
                    is_initial_load = True
            except Exception as e:
                logger.warning(f"Failed to read target table schema: {str(e)}. Treating as Initial Load.")
                is_initial_load = True
        
        try:
            # 1. Deduplicate incoming source batch to prevent duplicate merges
            source_count_orig = df_source.count()
            df_source_dedup = df_source.dropDuplicates([self.business_key])
            dedup_removed = source_count_orig - df_source_dedup.count()
            if dedup_removed > 0:
                logger.warning(f"Deduplicated source batch. Removed {dedup_removed} duplicate business keys.")
                
            if is_initial_load:
                logger.info("Target directory empty or missing. Executing initial load write...")
                
                # Stamp initial load with Version 1 active values
                df_initial = df_source_dedup \
                    .withColumn("effective_start_date", current_date()) \
                    .withColumn("effective_end_date", to_date(lit(self.high_date), "yyyy-MM-dd")) \
                    .withColumn("is_current", lit(True)) \
                    .withColumn("version_number", lit(1)) \
                    .withColumn("record_created_timestamp", current_timestamp()) \
                    .withColumn("record_updated_timestamp", current_timestamp())
                    
                records_inserted = df_initial.count()
                
                # Write partitioned by release_year
                df_initial.write.mode("overwrite").format("parquet").partitionBy("release_year").save(target_dir_path)
                
                elapsed_time_ms = (time.time() - start_time) * 1000
                return {
                    "records_inserted": records_inserted,
                    "records_expired": 0,
                    "records_current": records_inserted,
                    "execution_time_ms": elapsed_time_ms,
                    "mode": "INITIAL_LOAD"
                }
                
            # Incremental Load: Perform SCD Type 2 Merge
            logger.info("Target directory exists. Performing SCD Type 2 merge comparison...")
            df_target = self.spark.read.parquet(target_dir_path)
            
            # Split target into inactive (historical) and active records
            df_target_inactive = df_target.filter(col("is_current") == False)
            df_target_active = df_target.filter(col("is_current") == True)
            
            # 2. Detect New Inserts: Source records whose business key does not exist in target at all
            df_target_all_keys = df_target.select(self.business_key).distinct()
            df_inserted = df_source_dedup.join(df_target_all_keys, on=self.business_key, how="left_anti")
            
            df_inserted_stamped = df_inserted \
                .withColumn("effective_start_date", current_date()) \
                .withColumn("effective_end_date", to_date(lit(self.high_date), "yyyy-MM-dd")) \
                .withColumn("is_current", lit(True)) \
                .withColumn("version_number", lit(1)) \
                .withColumn("record_created_timestamp", current_timestamp()) \
                .withColumn("record_updated_timestamp", current_timestamp()) \
                .select(df_target.columns)
                
            records_inserted = df_inserted_stamped.count()
            
            # 3. Detect Updates: Match incoming source with current active target rows
            # Select columns from target that match base source schema
            base_columns = [c for c in df_source_dedup.columns if c != self.business_key]
            
            df_matched = df_source_dedup.alias("src").join(
                df_target_active.alias("tgt"),
                on=self.business_key,
                how="inner"
            )
            
            # Construct null-safe comparison condition
            change_filter = None
            for c in self.compare_columns:
                if c in df_source_dedup.columns and c in df_target_active.columns:
                    cond = ~col(f"src.{c}").eqNullSafe(col(f"tgt.{c}"))
                    change_filter = cond if change_filter is None else (change_filter | cond)
                    
            if change_filter is not None:
                # Target active rows that CHANGED
                df_changed_matched = df_matched.filter(change_filter)
                
                # New updates to insert: Version + 1, start date is today
                df_new_updates = df_changed_matched.select(
                    "src.*", 
                    current_date().alias("effective_start_date"),
                    to_date(lit(self.high_date), "yyyy-MM-dd").alias("effective_end_date"),
                    lit(True).alias("is_current"),
                    (col("tgt.version_number") + 1).alias("version_number"),
                    col("tgt.record_created_timestamp").alias("record_created_timestamp"),
                    current_timestamp().alias("record_updated_timestamp")
                ).select(df_target.columns)
                
                # Old active rows to expire: End date is yesterday, is_current = False
                df_expired = df_changed_matched.select(
                    "tgt.*"
                ).withColumn("is_current", lit(False)) \
                 .withColumn("effective_end_date", date_sub(current_date(), 1)) \
                 .withColumn("record_updated_timestamp", current_timestamp()) \
                 .select(df_target.columns)
                 
                # Active rows that did not change
                df_unchanged_active = df_matched.filter(~change_filter).select("tgt.*")
            else:
                # No comparison columns defined
                df_new_updates = self.spark.createDataFrame([], schema=df_target.schema)
                df_expired = self.spark.createDataFrame([], schema=df_target.schema)
                df_unchanged_active = df_matched.select("tgt.*")
                
            records_expired = df_expired.count()
            records_new_updates = df_new_updates.count()
            
            # 4. Active rows in target not present in source batch at all
            df_unmatched_active = df_target_active.join(df_source_dedup, on=self.business_key, how="left_anti")
            
            # Combine all unchanged active rows
            df_unchanged_total = df_unchanged_active.unionByName(df_unmatched_active)
            
            # 5. Consolidate final state:
            # - historical inactive target records
            # - unchanged active target records
            # - newly expired target records
            # - new inserts (Version 1)
            # - new updates (Version > 1)
            df_final = df_target_inactive \
                .unionByName(df_unchanged_total) \
                .unionByName(df_expired) \
                .unionByName(df_inserted_stamped) \
                .unionByName(df_new_updates)
                
            records_current = df_final.filter(col("is_current") == True).count()
            
            logger.info(f"SCD Type 2 merge counts: inserted_new={records_inserted}, updated_new={records_new_updates}, expired_old={records_expired}, active_current={records_current}")
            
            # Write final output using two-step safe overwrite process to avoid Spark lazy locks
            temp_path = target_dir_path + "_temp"
            df_final.write.mode("overwrite").format("parquet").partitionBy("release_year").save(temp_path)
            
            df_temp = self.spark.read.parquet(temp_path)
            df_temp.write.mode("overwrite").format("parquet").partitionBy("release_year").save(target_dir_path)
            
            import shutil
            shutil.rmtree(temp_path, ignore_errors=True)
            
            elapsed_time_ms = (time.time() - start_time) * 1000
            return {
                "records_inserted": records_inserted + records_new_updates,
                "records_expired": records_expired,
                "records_current": records_current,
                "execution_time_ms": elapsed_time_ms,
                "mode": "MERGE_UPDATE"
            }
            
        except Exception as e:
            logger.error(f"SCD Type 2 merge execution failed: {str(e)}", exc_info=True)
            raise ScdType2Exception(f"SCD Type 2 Merge failed: {str(e)}") from e
