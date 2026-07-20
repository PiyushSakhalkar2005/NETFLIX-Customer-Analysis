import os
import sys
import uuid
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
from pyspark.sql.functions import lit, current_timestamp, to_date

from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader
from src.ingestion.ingestion_engine import IngestionSourceFactory

logger = get_logger("BronzeLoader")

class BronzeLoaderException(Exception):
    """Custom exception class for Bronze layer loading operations."""
    pass

class BronzeLoader:
    """Handles loading raw data sources directly into the Medallion Bronze layer."""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any] = None):
        self.spark = spark
        self.config = config or ConfigLoader.load()
        
    def load_source_to_bronze(self, source_name: str, pipeline_run_id: str = None) -> str:
        """
        Extracts raw data via the Ingestion engine, appends standard pipeline metadata columns,
        and saves raw rows into Parquet files in the Bronze layer partitioned by ingestion date.
        
        Metadata fields added:
        - batch_id
        - pipeline_run_id
        - ingestion_timestamp
        - source_system
        - source_file
        - load_type
        
        Args:
            source_name (str): Key of the source definition inside pipeline_config.yaml
            pipeline_run_id (str, optional): Orchestrator run ID. Defaults to auto-generated UUID.
            
        Returns:
            str: Generated Batch ID
        """
        logger.info(f"Initiating Bronze layer load for source: {source_name}")
        
        # 1. Fetch Ingestion Configurations
        sources = self.config.get("ingestion", {}).get("sources", {})
        if source_name not in sources:
            raise BronzeLoaderException(f"Ingestion source '{source_name}' not defined in configurations.")
            
        src_cfg = sources[source_name]
        load_type = src_cfg.get("load_type", "FULL").upper()
        
        # Pipeline metadata properties
        source_system = self.config.get("pipeline", {}).get("name", "netflix_pipeline")
        source_file = src_cfg.get("path") or src_cfg.get("url") or src_cfg.get("table", "unknown")
        
        # Core destination paths
        bronze_dir = self.config.get("storage", {}).get("bronze_dir")
        dest_path = os.path.join(bronze_dir, f"bronze_{source_name}")
        
        # Unique run tracking keys
        batch_id = str(uuid.uuid4())
        run_id = pipeline_run_id or str(uuid.uuid4())
        
        try:
            # 2. Extract raw dataset via Ingestion Factory
            reader = IngestionSourceFactory.get_reader(source_name, self.config)
            df_raw = reader.read(self.spark)
            
            row_count = df_raw.count()
            logger.info(f"Extracted {row_count} rows from source. Injecting pipeline metadata...")
            
            # 3. Add Ingestion Audit Metadata (preserving all raw columns unmodified)
            df_bronze = df_raw \
                .withColumn("batch_id", lit(batch_id)) \
                .withColumn("pipeline_run_id", lit(run_id)) \
                .withColumn("ingestion_timestamp", current_timestamp()) \
                .withColumn("source_system", lit(source_system)) \
                .withColumn("source_file", lit(source_file)) \
                .withColumn("load_type", lit(load_type)) \
                .withColumn("ingestion_date", to_date(current_timestamp()))
                
            # 4. Save dataset to Bronze parquet directory
            write_mode = "overwrite" if load_type == "FULL" else "append"
            logger.info(f"Writing Parquet data to path: {dest_path} (mode: {write_mode})")
            
            # Partitioning by ingestion date to optimize file management and queries
            df_bronze.write \
                .mode(write_mode) \
                .format("parquet") \
                .partitionBy("ingestion_date") \
                .save(dest_path)
                
            logger.info(f"Bronze load completed successfully. batch_id: {batch_id}, rows written: {row_count}")
            return batch_id
            
        except Exception as e:
            logger.error(f"Failed to load raw data to Bronze for source '{source_name}': {str(e)}", exc_info=True)
            raise BronzeLoaderException(f"Bronze layer operation failed: {str(e)}") from e
