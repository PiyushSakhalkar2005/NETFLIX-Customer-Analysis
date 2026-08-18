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
    
    if not source_name or source_name.lower() in ["all", "multi"]:
        sources_to_ingest = ["netflix_csv", "netflix_json", "netflix_xml"]
    elif "," in source_name:
        sources_to_ingest = [s.strip() for s in source_name.split(",")]
    else:
        sources_to_ingest = [source_name]
        
    logger.info(f"Starting ingestion process for sources: {sources_to_ingest}")
    bronze_dir = config.get("storage", {}).get("bronze_dir")
    
    # Initialize Spark Session
    spark = init_spark("Ingest_Raw_MultiSource")
    last_batch_id = None
    
    try:
        for src_name in sources_to_ingest:
            source_config = config.get("ingestion", {}).get("sources", {}).get(src_name)
            if not source_config:
                logger.warning(f"Ingestion source '{src_name}' configuration not found. Skipping...")
                continue

            load_type = source_config.get("load_type", "FULL").upper()
            source_path = source_config.get("path") or source_config.get("url") or source_config.get("table", "unknown")
            dest_path = os.path.join(bronze_dir, f"bronze_{src_name}")
            batch_id = str(uuid.uuid4())
            last_batch_id = batch_id
            
            # Get reader instance
            reader = IngestionSourceFactory.get_reader(src_name, config)
            logger.info(f"Fetching raw data from source '{src_name}'...")
            df_raw = reader.read(spark)
            
            row_count = df_raw.count()
            logger.info(f"Raw data read successfully for {src_name}. Count: {row_count} rows.")
            
            df_bronze = df_raw \
                .withColumn("_batch_id", lit(batch_id)) \
                .withColumn("_ingested_at", current_timestamp()) \
                .withColumn("_source_path", lit(source_path)) \
                .withColumn("_load_type", lit(load_type))
                
            write_mode = "overwrite" if load_type == "FULL" else "append"
            logger.info(f"Writing to Bronze Parquet at: {dest_path} using mode '{write_mode}'")
            
            df_bronze.write \
                .mode(write_mode) \
                .format("parquet") \
                .save(dest_path)
                
            logger.info(f"Ingestion completed for source '{src_name}', batch: {batch_id}")
            print(f"INGESTION_SUCCESS: source={src_name}, batch_id={batch_id}, rows={row_count}")
            
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
    parser.add_argument("--source", type=str, help="Ingestion source name (e.g. netflix_csv, netflix_json, netflix_xml, or all)")
    args = parser.parse_args()
    run_ingestion(args.source)
