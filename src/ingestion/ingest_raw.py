import os
import sys
import argparse
from datetime import datetime
import uuid

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

# Add project root to system path for running scripts directly
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set HADOOP_HOME environment variable for local Windows Spark executions
hadoop_dir = os.path.join(project_root, "hadoop")
if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

from src.utils.config_loader import ConfigLoader
from src.utils.logger import get_logger
from src.ingestion.ingestion_engine import IngestionSourceFactory, IngestionException

logger = get_logger("IngestRawRunner")

def init_spark(app_name: str) -> SparkSession:
    """Initializes SparkSession configured to production standards."""
    logger.info("Initializing PySpark Session...")
    try:
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .master("local[*]") \
            .getOrCreate()
    except Exception as e:
        logger.critical(f"Failed to initialize Spark session: {str(e)}", exc_info=True)
        sys.exit(1)

def run_ingestion(source_name: str = None):
    # Load configuration
    try:
        config = ConfigLoader.load()
    except Exception as e:
        logger.error(f"Configuration load error: {str(e)}")
        sys.exit(1)
    
    if not source_name:
        source_name = config.get("ingestion", {}).get("active_source")
        
    logger.info(f"Starting ingestion process for source: {source_name}")
    
    source_config = config.get("ingestion", {}).get("sources", {}).get(source_name)
    if not source_config:
        logger.critical(f"Ingestion source '{source_name}' configuration not found.")
        sys.exit(1)

    load_type = source_config.get("load_type", "FULL").upper()
    source_path = source_config.get("path") or source_config.get("url") or source_config.get("table", "unknown")
    bronze_dir = config.get("storage", {}).get("bronze_dir")
    
    # Destination directory inside the Bronze layer
    dest_path = os.path.join(bronze_dir, f"bronze_{source_name}")
    
    # Initialize Spark Session
    spark = init_spark(f"Ingest_{source_name}")
    
    # Generate unique Batch ID
    batch_id = str(uuid.uuid4())
    
    try:
        # Get reader instance
        reader = IngestionSourceFactory.get_reader(source_name, config)
        
        # Read raw data
        logger.info(f"Fetching raw data from source '{source_name}'...")
        df_raw = reader.read(spark)
        
        # Check rows count
        row_count = df_raw.count()
        logger.info(f"Raw data read successfully. Count: {row_count} rows.")
        
        # Capture metadata / audit columns without altering existing data
        df_bronze = df_raw \
            .withColumn("_batch_id", lit(batch_id)) \
            .withColumn("_ingested_at", current_timestamp()) \
            .withColumn("_source_path", lit(source_path)) \
            .withColumn("_load_type", lit(load_type))
            
        # Determine write mode: Overwrite for FULL refreshed loads, Append for INCREMENTAL loads
        write_mode = "overwrite" if load_type == "FULL" else "append"
        logger.info(f"Writing to Bronze Parquet at: {dest_path} using mode '{write_mode}'")
        
        # Write to Bronze Parquet
        df_bronze.write \
            .mode(write_mode) \
            .format("parquet") \
            .save(dest_path)
            
        logger.info(f"Ingestion completed successfully for batch: {batch_id}")
        print(f"INGESTION_SUCCESS: batch_id={batch_id}, rows={row_count}")
        
    except IngestionException as ie:
        logger.error(f"Ingestion framework error: {str(ie)}")
        print(f"INGESTION_FAILED: {str(ie)}")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"Unexpected error during ingestion run: {str(e)}", exc_info=True)
        print(f"INGESTION_ERROR: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("Stopping PySpark Session...")
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Netflix Data Engineering Landing Ingestor")
    parser.add_argument("--source", type=str, help="Override active source configured in pipeline_config.yaml")
    args = parser.parse_args()
    run_ingestion(args.source)
