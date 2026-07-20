import os
import sys
import argparse
import uuid

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set HADOOP_HOME environment variable for local Windows Spark executions
hadoop_dir = os.path.join(project_root, "hadoop")
if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")

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

from pyspark.sql import SparkSession
from src.utils.logger import get_logger
from src.bronze.bronze_loader import BronzeLoader

logger = get_logger("RunBronzeLoad")

def init_spark(app_name: str) -> SparkSession:
    """Initializes SparkSession configured for local execution."""
    logger.info("Initializing Spark session...")
    try:
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.sql.parquet.compression.codec", "snappy") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .master("local[*]") \
            .getOrCreate()
    except Exception as e:
        logger.critical(f"Failed to start local Spark session: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Netflix Data Pipeline - Bronze Loader Runner")
    parser.add_argument("--source", type=str, required=True, help="Ingestion source name (e.g. netflix_csv)")
    parser.add_argument("--run-id", type=str, help="Pipeline Run ID")
    args = parser.parse_args()

    spark = init_spark(f"Bronze_Load_{args.source}")
    run_id = args.run_id or str(uuid.uuid4())
    
    try:
        loader = BronzeLoader(spark)
        batch_id = loader.load_source_to_bronze(args.source, run_id)
        print(f"BRONZE_LOAD_SUCCESS: batch_id={batch_id}")
    except Exception as e:
        logger.critical(f"Execution failed: {str(e)}")
        print(f"BRONZE_LOAD_FAILED: {str(e)}")
        sys.exit(1)
    finally:
        logger.info("Stopping Spark Session...")
        spark.stop()
