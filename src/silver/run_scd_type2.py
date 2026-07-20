import os
import sys
import argparse
import json
import uuid
from datetime import datetime

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Set HADOOP_HOME and PYTHONPATH for local Windows Spark executions
hadoop_dir = os.path.join(project_root, "hadoop")
if os.path.exists(hadoop_dir):
    os.environ["HADOOP_HOME"] = hadoop_dir
    os.environ["PATH"] = os.path.join(hadoop_dir, "bin") + os.pathsep + os.environ.get("PATH", "")
os.environ["PYTHONPATH"] = project_root + os.pathsep + os.environ.get("PYTHONPATH", "")

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
from src.utils.config_loader import ConfigLoader
from src.silver.scd_type2 import ScdType2Processor

logger = get_logger("RunScdType2")

def init_spark() -> SparkSession:
    logger.info("Initializing Spark session for SCD Type 2...")
    try:
        return SparkSession.builder \
            .appName("Netflix_SCD_Type_2") \
            .config("spark.sql.session.timeZone", "UTC") \
            .master("local[*]") \
            .getOrCreate()
    except Exception as e:
        logger.critical(f"Failed to start local Spark session: {str(e)}", exc_info=True)
        sys.exit(1)

def update_metadata_log(metrics: dict):
    log_dir = os.path.join(project_root, "data", "metadata")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "scd_type2_runs_log.json")
    
    runs = []
    if os.path.exists(log_path):
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                runs = json.load(f)
        except Exception:
            runs = []
            
    runs.append(metrics)
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(runs, f, indent=2)
    logger.info(f"Metadata log updated successfully at: {log_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Netflix Data Pipeline - SCD Type 2 Runner")
    parser.add_argument("--source-path", type=str, required=True, help="Path to clean staging Parquet table")
    parser.add_argument("--target-path", type=str, required=True, help="Path to target master SCD Type 2 dimension table")
    parser.add_argument("--batch-id", type=str, help="Ingestion Batch ID")
    parser.add_argument("--pipeline-run-id", type=str, help="Pipeline Run ID")
    args = parser.parse_args()
    
    spark = init_spark()
    batch_id = args.batch_id or str(uuid.uuid4())
    run_id = args.pipeline_run_id or f"run-{str(uuid.uuid4())[:8]}"
    
    try:
        if not os.path.exists(args.source_path):
            raise FileNotFoundError(f"Staging source path not found: {args.source_path}")
            
        df_source = spark.read.parquet(args.source_path)
        processor = ScdType2Processor(spark)
        
        run_stats = processor.merge_titles(df_source, args.target_path)
        
        # Save run metrics
        metrics = {
            "batch_id": batch_id,
            "pipeline_run_id": run_id,
            "timestamp": datetime.utcnow().isoformat(),
            "target_table": "silver_titles_scd2",
            "records_inserted": run_stats["records_inserted"],
            "records_expired": run_stats["records_expired"],
            "records_current": run_stats["records_current"],
            "execution_duration_ms": run_stats["execution_time_ms"],
            "mode": run_stats["mode"],
            "status": "SUCCESS"
        }
        update_metadata_log(metrics)
        print(f"SCD_TYPE2_SUCCESS: target={args.target_path}, inserted={run_stats['records_inserted']}, expired={run_stats['records_expired']}")
        
    except Exception as e:
        logger.critical(f"SCD Type 2 merge execution failed: {str(e)}", exc_info=True)
        print(f"SCD_TYPE2_FAILED: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
