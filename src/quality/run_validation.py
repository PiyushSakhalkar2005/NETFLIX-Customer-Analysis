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
from src.quality.dq_framework import DataQualityFramework

logger = get_logger("RunValidation")

def init_spark(app_name: str) -> SparkSession:
    logger.info("Initializing Spark session for validation...")
    try:
        return SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.session.timeZone", "UTC") \
            .master("local[*]") \
            .getOrCreate()
    except Exception as e:
        logger.critical(f"Failed to start local Spark session: {str(e)}", exc_info=True)
        sys.exit(1)

def update_metadata_log(metrics: dict):
    log_dir = os.path.join(project_root, "data", "metadata")
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, "dq_validation_runs.json")
    
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
    parser = argparse.ArgumentParser(description="Netflix Data Pipeline - Data Quality Validator")
    parser.add_argument("--source", type=str, required=True, help="Bronze table source name (e.g. netflix_csv)")
    parser.add_argument("--batch-id", type=str, required=False, default=None, help="Batch ID generated during Bronze ingestion")
    args = parser.parse_args()
    batch_id = args.batch_id or str(uuid.uuid4())
    
    config = ConfigLoader.load()
    bronze_dir = config.get("storage", {}).get("bronze_dir")
    
    if args.source.lower() in ["all", "multi"]:
        sources = ["netflix_csv", "netflix_json", "netflix_xml"]
    elif "," in args.source:
        sources = [s.strip() for s in args.source.split(",")]
    else:
        sources = [args.source]
        
    spark = init_spark(f"DQ_Validation_MultiSource")
    
    for src in sources:
        bronze_source_path = os.path.join(bronze_dir, f"bronze_{src}")
        if not os.path.exists(bronze_source_path):
            logger.warning(f"Bronze source directory not found at: {bronze_source_path}. Skipping...")
            continue
    
    try:
        # Load dataset from Bronze
        logger.info(f"Loading Bronze dataset from: {bronze_source_path}")
        df = spark.read.parquet(bronze_source_path)
        
        row_count = df.count()
        if row_count == 0:
            logger.warning("Target Bronze dataset is empty. Validation skipped.")
            print("VALIDATION_SKIPPED: Empty dataset")
            sys.exit(0)
            
        # Initialize validation framework
        dq = DataQualityFramework(spark)
        
        # 1. Run GE Checks
        success, ge_summary = dq.validate_dataset(df, args.source, batch_id)
        
        # 2. Extract and Save Rejected records in Spark
        rejected_dir = os.path.join(project_root, "data", "rejected_records")
        passed_count, failed_count = dq.extract_and_save_rejected(df, rejected_dir, batch_id)
        
        # 3. Generate HTML report
        report_path = os.path.join(project_root, "docs", "reports", f"dq_report_{batch_id}.html")
        dq.generate_html_report(ge_summary, report_path)
        
        # 4. Save metadata audit log
        metrics = {
            "batch_id": batch_id,
            "source_name": args.source,
            "validation_timestamp": datetime.utcnow().isoformat(),
            "records_validated": row_count,
            "records_passed": passed_count,
            "records_failed": failed_count,
            "validation_duration_ms": ge_summary["validation_duration_ms"],
            "report_path": report_path,
            "overall_status": "PASSED" if success else "FAILED"
        }
        update_metadata_log(metrics)
        
        print(f"VALIDATION_COMPLETE: passed={passed_count}, failed={failed_count}, report={report_path}")
        
    except Exception as e:
        logger.critical(f"Data validation failed: {str(e)}", exc_info=True)
        print(f"VALIDATION_ERROR: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()
