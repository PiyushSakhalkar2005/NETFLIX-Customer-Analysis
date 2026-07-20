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
from src.silver.incremental_engine import IncrementalEngine

logger = get_logger("RunIncrementalPipeline")

def init_spark() -> SparkSession:
    logger.info("Initializing Spark session for Incremental Processing Pipeline...")
    try:
        return SparkSession.builder \
            .appName("Netflix_Incremental_Pipeline") \
            .config("spark.sql.session.timeZone", "UTC") \
            .master("local[*]") \
            .getOrCreate()
    except Exception as e:
        logger.critical(f"Failed to start local Spark session: {str(e)}", exc_info=True)
        sys.exit(1)

def update_metadata_log(metrics: dict):
    log_dir = os.path.join(project_root, "data", "metadata")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "incremental_runs_log.json")
    
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
    parser = argparse.ArgumentParser(description="Netflix Data Pipeline - Incremental Engine Orchestrator Runner")
    parser.add_argument("--batch-id", type=str, help="Ingestion Batch ID")
    parser.add_argument("--pipeline-run-id", type=str, help="Pipeline Run ID")
    parser.add_argument("--force-full", action="store_true", help="Force Full Load process bypassing watermarks")
    args = parser.parse_args()
    
    spark = init_spark()
    batch_id = args.batch_id or str(uuid.uuid4())
    run_id = args.pipeline_run_id or f"run-{str(uuid.uuid4())[:8]}"
    
    try:
        engine = IncrementalEngine(spark)
        run_stats = engine.run_pipeline(batch_id, run_id, force_full_load=args.force_full)
        
        if run_stats.get("status") == "SUCCESS":
            metrics = {
                "batch_id": batch_id,
                "pipeline_run_id": run_id,
                "timestamp": datetime.utcnow().isoformat(),
                "records_processed": run_stats["records_processed"],
                "records_inserted": run_stats["inserted"],
                "records_updated": run_stats["updated"],
                "records_skipped": run_stats["skipped"],
                "execution_duration_ms": run_stats["execution_time_ms"],
                "status": "SUCCESS"
            }
            update_metadata_log(metrics)
            print(f"INCREMENTAL_RUN_SUCCESS: processed={run_stats['records_processed']}, inserted={run_stats['inserted']}, updated={run_stats['updated']}")
        else:
            print(f"INCREMENTAL_RUN_SKIPPED: {run_stats.get('reason', 'No data')}")
            
    except Exception as e:
        logger.critical(f"Incremental pipeline script execution failed: {str(e)}", exc_info=True)
        print(f"INCREMENTAL_RUN_FAILED: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
