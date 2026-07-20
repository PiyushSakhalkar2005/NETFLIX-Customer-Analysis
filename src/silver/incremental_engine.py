import os
import sys
import time
from datetime import datetime
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
from pyspark.sql.functions import col, max as spark_max, lit

from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader
from src.utils.watermark_manager import WatermarkManager
from src.utils.audit_manager import AuditManager
from src.utils.notifications import NotificationManager
from src.silver.silver_transformer import SilverTransformer
from src.silver.scd_type1 import ScdType1Processor
from src.silver.scd_type2 import ScdType2Processor
from src.gold.gold_warehouse import GoldWarehouse

logger = get_logger("IncrementalEngine")

class IncrementalEngineException(Exception):
    """Custom exception class for Incremental Processing Pipeline errors."""
    pass

class IncrementalEngine:
    """Orchestrates end-to-end full and incremental pipeline execution across Bronze, Silver, and Gold layers."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any] = None):
        self.spark = spark
        self.config = config or ConfigLoader.load()
        self.watermark_mgr = WatermarkManager(self.config)
        self.audit_mgr = AuditManager(self.config)
        
        inc_conf = self.config.get("incremental_processing", {})
        self.load_type_override = inc_conf.get("load_type", "incremental").lower()
        self.business_key = inc_conf.get("business_key", "show_id")
        
    def run_pipeline(self, batch_id: str, run_id: str, force_full_load: bool = False) -> Dict[str, Any]:
        """
        Loads the Bronze dataset, filters out records processed before the last watermark,
        standardizes the delta, merges it via SCD Type 1 or 2, rebuilds the Gold Star schema,
        and saves a new watermark checkpoint. Logs metadata details for auditing.
        """
        logger.info(f"Initiating Incremental Pipeline execution. Run ID: {run_id}, Batch ID: {batch_id}")
        start_time = time.time()
        
        # 1. Start pipeline run audit record
        self.audit_mgr.start_run(run_id, "netflix_titles_pipeline", batch_id)
        
        storage = self.config.get("storage", {})
        bronze_dir = storage.get("bronze_dir")
        silver_dir = storage.get("silver_dir")
        gold_dir = storage.get("gold_dir")
        
        # Determine active load type: Full or Incremental
        is_full_load = force_full_load or (self.load_type_override == "full")
        watermark = self.watermark_mgr.get_watermark()
        last_processed = watermark["last_processed_timestamp"]
        
        try:
            # 1. Load and consolidate raw Bronze parquet subfolders under bronze_dir
            logger.info("Consolidating multi-source Bronze parquet subdirectories...")
            bronze_start = time.time()
            if not os.path.exists(bronze_dir):
                logger.warning("Bronze parent directory does not exist. Pipeline aborted.")
                self.audit_mgr.log_step(run_id, "Bronze", "FAILED", int((time.time() - bronze_start)*1000), error_message="No Bronze directory found")
                self.audit_mgr.end_run(run_id, "FAILED")
                return {"status": "SKIPPED", "reason": "No Bronze directory found"}
                
            bronze_subdirs = [
                os.path.join(bronze_dir, d) for d in os.listdir(bronze_dir)
                if os.path.isdir(os.path.join(bronze_dir, d)) and d.startswith("bronze_")
            ]
            
            if not bronze_subdirs:
                logger.warning("No Bronze subdirectories (starting with bronze_) found. Pipeline aborted.")
                self.audit_mgr.log_step(run_id, "Bronze", "FAILED", int((time.time() - bronze_start)*1000), error_message="No Bronze subfolders found")
                self.audit_mgr.end_run(run_id, "FAILED")
                return {"status": "SKIPPED", "reason": "No Bronze subfolders found"}
                
            df_bronze = None
            for subdir in bronze_subdirs:
                df_sub = self.spark.read.parquet(subdir)
                
                # Normalize column names to match CSV standard
                rename_map = {
                    "_batch_id": "batch_id",
                    "_ingested_at": "ingestion_timestamp",
                    "_source_path": "source_file",
                    "_load_type": "load_type"
                }
                for old_col, new_col in rename_map.items():
                    if old_col in df_sub.columns:
                        df_sub = df_sub.withColumnRenamed(old_col, new_col)
                        
                # Ensure pipeline_run_id and source_system exist
                if "pipeline_run_id" not in df_sub.columns:
                    df_sub = df_sub.withColumn("pipeline_run_id", lit(run_id))
                if "source_system" not in df_sub.columns:
                    df_sub = df_sub.withColumn("source_system", lit("JSON_INGEST"))
                    
                if df_bronze is None:
                    df_bronze = df_sub
                else:
                    common_cols = list(set(df_bronze.columns).intersection(set(df_sub.columns)))
                    df_bronze = df_bronze.select(common_cols).unionByName(df_sub.select(common_cols))
                    
            total_bronze_records = df_bronze.count()
            self.audit_mgr.log_step(run_id, "Bronze", "SUCCESS", int((time.time() - bronze_start) * 1000), records_read=total_bronze_records, records_written=total_bronze_records)
            
            # 2. Filter Bronze for incremental updates (if applicable)
            if is_full_load:
                logger.info("Performing Full Load execution. Processing all Bronze records...")
                df_delta = df_bronze
            else:
                logger.info(f"Performing Incremental Load. Filtering for updates after watermark: {last_processed}")
                # Filter rows where ingestion_timestamp is greater than the watermark timestamp
                df_delta = df_bronze.filter(col("ingestion_timestamp") > last_processed)
                
            records_processed = df_delta.count()
            logger.info(f"Bronze delta count to process: {records_processed} (Total Bronze: {total_bronze_records})")
            
            if records_processed == 0:
                logger.info("No new records since last watermark checkpoint. Pipeline skipped successfully.")
                self.audit_mgr.log_step(run_id, "Bronze_Delta_Filter", "SKIPPED", 0)
                self.audit_mgr.end_run(run_id, "SKIPPED")
                return {
                    "status": "SKIPPED",
                    "records_processed": 0,
                    "inserted": 0,
                    "updated": 0,
                    "skipped": 0,
                    "execution_time_ms": (time.time() - start_time) * 1000
                }
                
            # 3. Process Silver Cleansing and Relational Normalization on the Delta
            logger.info("Running Silver cleaning transformer on delta...")
            silver_start = time.time()
            transformer = SilverTransformer(self.spark, self.config)
            
            # Create a temporary staging area for the processed delta
            delta_silver_titles_path = os.path.join(silver_dir, f"staging_delta_{batch_id}_titles")
            delta_silver_country_path = os.path.join(silver_dir, f"staging_delta_{batch_id}_country")
            delta_silver_genres_path = os.path.join(silver_dir, f"staging_delta_{batch_id}_genres")
            delta_silver_directors_path = os.path.join(silver_dir, f"staging_delta_{batch_id}_directors")
            delta_silver_cast_path = os.path.join(silver_dir, f"staging_delta_{batch_id}_cast")
            
            # Clean and write staging delta files
            transformer.transform(
                df_delta,
                dest_titles_dir=delta_silver_titles_path,
                dest_country_dir=delta_silver_country_path,
                dest_genres_dir=delta_silver_genres_path,
                dest_directors_dir=delta_silver_directors_path,
                dest_cast_dir=delta_silver_cast_path
            )
            self.audit_mgr.log_step(run_id, "Silver_Cleansing", "SUCCESS", int((time.time() - silver_start) * 1000), records_read=records_processed, records_written=records_processed)
            
            # 4. Integrate with SCD: Merge the delta from staging into the Master Silver tables
            logger.info("Merging delta into Silver Master Dimensions via Slowly Changing Dimensions...")
            
            # Read clean delta tables
            df_delta_titles = self.spark.read.parquet(delta_silver_titles_path)
            df_delta_country = self.spark.read.parquet(delta_silver_country_path)
            df_delta_genres = self.spark.read.parquet(delta_silver_genres_path)
            df_delta_directors = self.spark.read.parquet(delta_silver_directors_path)
            df_delta_cast = self.spark.read.parquet(delta_silver_cast_path)
            
            # Target silver paths
            target_titles_path = os.path.join(silver_dir, "silver_titles")
            target_country_path = os.path.join(silver_dir, "silver_country")
            target_genres_path = os.path.join(silver_dir, "silver_genres")
            target_directors_path = os.path.join(silver_dir, "silver_directors")
            target_cast_path = os.path.join(silver_dir, "silver_cast")
            
            # Merge Lookup tables using SCD Type 1 overwrite rules
            scd1_start = time.time()
            scd1 = ScdType1Processor(self.spark, self.config)
            scd1.merge_lookups(df_delta_country, target_country_path)
            scd1.merge_lookups(df_delta_genres, target_genres_path)
            scd1.merge_lookups(df_delta_directors, target_directors_path)
            scd1.merge_lookups(df_delta_cast, target_cast_path)
            self.audit_mgr.log_step(run_id, "SCD_Type1", "SUCCESS", int((time.time() - scd1_start) * 1000), records_read=records_processed, records_written=records_processed)
            
            # Merge Main Titles Dimension using SCD Type 2 rules
            scd2_start = time.time()
            scd2 = ScdType2Processor(self.spark, self.config)
            scd2_stats = scd2.merge_titles(df_delta_titles, target_titles_path)
            inserted = scd2_stats.get("records_inserted", 0)
            expired = scd2_stats.get("records_expired", 0)
            self.audit_mgr.log_step(run_id, "SCD_Type2", "SUCCESS", int((time.time() - scd2_start) * 1000), records_read=records_processed, records_inserted=inserted, records_updated=expired)
            
            # 5. Re-run Gold warehouse Star Schema compilation to reflect the master updates
            logger.info("Re-compiling Gold Star Schema facts and dimensions...")
            gold_start = time.time()
            gold = GoldWarehouse(self.spark, self.config)
            gold_stats = gold.build_star_schema(batch_id, run_id)
            self.audit_mgr.log_step(run_id, "Gold", "SUCCESS", int((time.time() - gold_start) * 1000), records_read=gold_stats.get("records_processed", 0), records_written=gold_stats.get("records_processed", 0))
            
            # Clean up staging delta folders
            import shutil
            for path in [delta_silver_titles_path, delta_silver_country_path, delta_silver_genres_path, delta_silver_directors_path, delta_silver_cast_path]:
                shutil.rmtree(path, ignore_errors=True)
                
            # 6. Capture new Watermark max value from the delta batch and convert to string format
            max_timestamp_row = df_delta.select(spark_max("ingestion_timestamp").alias("max_ts")).first()
            max_ts_val = max_timestamp_row["max_ts"]
            if max_ts_val:
                if hasattr(max_ts_val, "isoformat"):
                    new_watermark_ts = max_ts_val.isoformat()
                else:
                    new_watermark_ts = str(max_ts_val)
            else:
                new_watermark_ts = last_processed
            
            # Save watermark checkpoint
            elapsed_time_ms = int((time.time() - start_time) * 1000)
            self.watermark_mgr.update_watermark(new_watermark_ts, batch_id, elapsed_time_ms)
            
            # Close run audit record
            spark_app_id = self.spark.sparkContext.applicationId
            self.audit_mgr.end_run(run_id, "SUCCESS", spark_app_id)
            
            logger.info(f"Pipeline run {run_id} completed successfully. processed={records_processed}, inserted={inserted}, updated={expired}")
            
            return {
                "status": "SUCCESS",
                "records_processed": records_processed,
                "inserted": inserted,
                "updated": expired,
                "skipped": 0,
                "execution_time_ms": elapsed_time_ms
            }
            
        except Exception as e:
            logger.error(f"Incremental pipeline execution failed: {str(e)}", exc_info=True)
            self.audit_mgr.log_step(run_id, "Pipeline", "FAILED", int((time.time() - start_time)*1000), error_message=str(e))
            self.audit_mgr.end_run(run_id, "FAILED")
            NotificationManager.send_alert(run_id, "IncrementalEngine", str(e))
            raise IncrementalEngineException(f"Incremental Engine run failed: {str(e)}") from e
