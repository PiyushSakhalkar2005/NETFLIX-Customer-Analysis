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
from pyspark.sql.functions import col, lit, current_timestamp

from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader

logger = get_logger("ScdType1Processor")

class ScdType1Exception(Exception):
    """Custom exception class for SCD Type 1 merge pipeline errors."""
    pass

class ScdType1Processor:
    """Implements production-grade SCD Type 1 (overwrite updates, no history) for Silver dimension tables."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any] = None):
        self.spark = spark
        self.config = config or ConfigLoader.load()
        scd_conf = self.config.get("scd_type1", {})
        self.business_key = scd_conf.get("business_key", "show_id")
        self.compare_columns = scd_conf.get("compare_columns", [])
        
    def merge_titles(self, df_source: DataFrame, target_dir_path: str) -> Dict[str, Any]:
        """
        Merges df_source into the target silver_titles Parquet directory using SCD Type 1 logic.
        Identifies inserts and updates (using null-safe comparison), overwrites changed records,
        and saves the consolidated state back to target_dir_path.
        """
        logger.info(f"Initiating SCD Type 1 merge for target directory: {target_dir_path}")
        start_time = time.time()
        
        # If target path does not exist or has no Parquet files, write source directly (Initial Load)
        is_initial_load = not os.path.exists(target_dir_path) or not any(
            f.endswith(".parquet") for root, dirs, files in os.walk(target_dir_path) for f in files
        )
        
        try:
            if is_initial_load:
                logger.info("Target directory empty or missing. Executing initial load write...")
                records_inserted = df_source.count()
                
                # Write to target path partitioned by release_year
                df_source.write.mode("overwrite").format("parquet").partitionBy("release_year").save(target_dir_path)
                
                elapsed_time_ms = (time.time() - start_time) * 1000
                return {
                    "records_inserted": records_inserted,
                    "records_updated": 0,
                    "records_unchanged": 0,
                    "execution_time_ms": elapsed_time_ms,
                    "mode": "INITIAL_LOAD"
                }
                
            # Incremental Load: Perform Merge
            logger.info("Target directory exists. Performing SCD Type 1 merge comparison...")
            df_target = self.spark.read.parquet(target_dir_path)
            
            # Align schema columns to prevent union mismatches
            df_source_aligned = df_source.select(df_target.columns)
            
            # 1. Detect New Inserts: Source records not in target
            df_inserted = df_source_aligned.join(df_target, on=self.business_key, how="left_anti")
            records_inserted = df_inserted.count()
            
            # 2. Detect Updates: Source records present in target but having changes
            # Perform inner join on business key
            df_matched = df_source_aligned.alias("src").join(
                df_target.alias("tgt"), 
                on=self.business_key, 
                how="inner"
            )
            
            # Construct null-safe comparison filter across all comparison columns
            change_filter = None
            for c in self.compare_columns:
                if c in df_target.columns:
                    # eqNullSafe is Spark's null-safe equality operator. ~ negates it to check for inequality.
                    cond = ~col(f"src.{c}").eqNullSafe(col(f"tgt.{c}"))
                    change_filter = cond if change_filter is None else (change_filter | cond)
                    
            if change_filter is not None:
                df_updated = df_matched.filter(change_filter).select("src.*")
                df_unchanged_matched = df_matched.filter(~change_filter).select("tgt.*")
            else:
                # No comparison columns configured
                df_updated = self.spark.createDataFrame([], schema=df_target.schema)
                df_unchanged_matched = df_matched.select("tgt.*")
                
            records_updated = df_updated.count()
            
            # 3. Detect Unchanged Target Records: Target rows not present in incoming source
            df_unchanged_unmatched = df_target.join(df_source_aligned, on=self.business_key, how="left_anti")
            
            # Combine all unchanged records
            df_unchanged = df_unchanged_unmatched.unionByName(df_unchanged_matched)
            records_unchanged = df_unchanged.count()
            
            logger.info(f"SCD Type 1 merge counts: inserted={records_inserted}, updated={records_updated}, unchanged={records_unchanged}")
            
            # 4. Consolidate final state: unchanged + updated (overwritten) + inserted
            df_final = df_unchanged.unionByName(df_updated).unionByName(df_inserted)
            
            # Overwrite final Parquet database using a safe temporary directory to avoid lazy-evaluation deletion conflicts
            temp_path = target_dir_path + "_temp"
            df_final.write.mode("overwrite").format("parquet").partitionBy("release_year").save(temp_path)
            
            # Load from temp and overwrite main target directory
            df_temp = self.spark.read.parquet(temp_path)
            df_temp.write.mode("overwrite").format("parquet").partitionBy("release_year").save(target_dir_path)
            
            # Clean up temporary storage path
            import shutil
            shutil.rmtree(temp_path, ignore_errors=True)
            
            elapsed_time_ms = (time.time() - start_time) * 1000
            return {
                "records_inserted": records_inserted,
                "records_updated": 2 * records_updated if False else records_updated,  # Track exact updates
                "records_unchanged": records_unchanged,
                "execution_time_ms": elapsed_time_ms,
                "mode": "MERGE_UPDATE"
            }
            
        except Exception as e:
            logger.error(f"SCD Type 1 merge failed: {str(e)}", exc_info=True)
            raise ScdType1Exception(f"SCD Type 1 Merge failed: {str(e)}") from e

    def merge_lookups(self, df_source: DataFrame, target_dir_path: str) -> Dict[str, Any]:
        """
        Merges normal exploded lookups (e.g. silver_country, silver_genres, etc.).
        Overwrites lookup mapping records for updated/incoming show_ids.
        """
        logger.info(f"Initiating Lookup SCD Type 1 merge for target directory: {target_dir_path}")
        start_time = time.time()
        
        is_initial_load = not os.path.exists(target_dir_path) or not any(
            f.endswith(".parquet") for root, dirs, files in os.walk(target_dir_path) for f in files
        )
        
        try:
            if is_initial_load:
                logger.info("Target lookup directory empty or missing. Writing source lookups directly...")
                df_source.write.mode("overwrite").format("parquet").save(target_dir_path)
                elapsed_time_ms = (time.time() - start_time) * 1000
                return {"records_merged": df_source.count(), "execution_time_ms": elapsed_time_ms, "mode": "INITIAL_LOAD"}
                
            df_target = self.spark.read.parquet(target_dir_path)
            
            # Identify which show_ids are incoming in the source batch
            df_incoming_ids = df_source.select(self.business_key).distinct()
            
            # Delete old lookup mappings for updated show_ids by anti-joining on show_id
            df_unchanged = df_target.join(df_incoming_ids, on=self.business_key, how="left_anti")
            
            # Combine unchanged lookups with the new lookup mappings
            df_final = df_unchanged.unionByName(df_source.select(df_target.columns))
            
            # Overwrite final Parquet database using a safe temporary directory
            temp_path = target_dir_path + "_temp"
            df_final.write.mode("overwrite").format("parquet").save(temp_path)
            
            # Load from temp and overwrite main target directory
            df_temp = self.spark.read.parquet(temp_path)
            df_temp.write.mode("overwrite").format("parquet").save(target_dir_path)
            
            # Clean up temporary storage path
            import shutil
            shutil.rmtree(temp_path, ignore_errors=True)
            
            elapsed_time_ms = (time.time() - start_time) * 1000
            return {
                "records_merged": df_source.count(),
                "execution_time_ms": elapsed_time_ms,
                "mode": "MERGE_UPDATE"
            }
            
        except Exception as e:
            logger.error(f"Lookup SCD Type 1 merge failed: {str(e)}", exc_info=True)
            raise ScdType1Exception(f"Lookup SCD Type 1 Merge failed: {str(e)}") from e
