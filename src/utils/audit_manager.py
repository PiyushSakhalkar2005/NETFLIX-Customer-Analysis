import os
import sys
import json
import socket
from datetime import datetime
from typing import Dict, Any, List

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader

logger = get_logger("AuditManager")

class AuditManager:
    """Production-grade pipeline audit and metadata manager. Logs runs and steps to local storage files."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or ConfigLoader.load()
        
        # Look up configured metadata directory, falling back to default
        storage = self.config.get("storage", {})
        metadata_dir = storage.get("metadata_dir")
        if metadata_dir:
            self.audit_dir = metadata_dir
        else:
            self.audit_dir = os.path.join(project_root, "data", "metadata")
        
        self.runs_file = os.path.join(self.audit_dir, "pipeline_runs_audit.json")
        self.steps_file = os.path.join(self.audit_dir, "pipeline_steps_audit.json")
        
    def _read_json(self, file_path: str) -> List[Dict[str, Any]]:
        if not os.path.exists(file_path):
            return []
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"AuditManager failed to read log file {file_path}: {str(e)}")
            return []
            
    def _write_json(self, file_path: str, data: List[Dict[str, Any]]):
        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"AuditManager failed to write log file {file_path}: {str(e)}")
            # Fail silently as per requirements: "If metadata update fails, the pipeline should continue while logging the error."
            
    def start_run(self, pipeline_run_id: str, pipeline_name: str, batch_id: str, execution_mode: str = "INCREMENTAL") -> Dict[str, Any]:
        """Registers the start of a pipeline run execution."""
        logger.info(f"Registering pipeline run start: run_id={pipeline_run_id}, name={pipeline_name}, mode={execution_mode}")
        
        run_record = {
            "pipeline_run_id": pipeline_run_id,
            "pipeline_name": pipeline_name,
            "batch_id": batch_id,
            "execution_mode": execution_mode.upper(),
            "start_time": datetime.utcnow().isoformat(),
            "end_time": None,
            "execution_duration_ms": 0,
            "execution_status": "RUNNING",
            "execution_host": socket.gethostname(),
            "spark_application_id": None,
            "watermark_timestamp": None,
            "rows_read": 0,
            "rows_inserted": 0,
            "rows_updated": 0,
            "rows_skipped": 0,
            "rows_rejected": 0,
            "error_details": None
        }
        
        try:
            runs = self._read_json(self.runs_file)
            # Remove any existing run with same ID to prevent duplicates
            runs = [r for r in runs if r.get("pipeline_run_id") != pipeline_run_id]
            runs.append(run_record)
            self._write_json(self.runs_file, runs)
        except Exception as e:
            logger.error(f"Failed to start pipeline run metadata record: {str(e)}")
            
        return run_record
        
    def end_run(self, pipeline_run_id: str, status: str, spark_app_id: str = None, 
                watermark_timestamp: str = None, rows_read: int = 0, rows_inserted: int = 0, 
                rows_updated: int = 0, rows_skipped: int = 0, rows_rejected: int = 0, 
                error_details: str = None) -> Dict[str, Any]:
        """Marks a pipeline run execution as completed (SUCCESS, FAILED, etc.) and records duration and metrics."""
        logger.info(f"Registering pipeline run end: run_id={pipeline_run_id}, status={status}")
        
        try:
            runs = self._read_json(self.runs_file)
            for r in runs:
                if r["pipeline_run_id"] == pipeline_run_id:
                    r["end_time"] = datetime.utcnow().isoformat()
                    r["execution_status"] = status
                    r["spark_application_id"] = spark_app_id
                    r["watermark_timestamp"] = watermark_timestamp
                    r["rows_read"] = rows_read
                    r["rows_inserted"] = rows_inserted
                    r["rows_updated"] = rows_updated
                    r["rows_skipped"] = rows_skipped
                    r["rows_rejected"] = rows_rejected
                    r["error_details"] = error_details
                    
                    # Compute duration
                    start_dt = datetime.fromisoformat(r["start_time"])
                    end_dt = datetime.fromisoformat(r["end_time"])
                    r["execution_duration_ms"] = int((end_dt - start_dt).total_seconds() * 1000)
                    
                    self._write_json(self.runs_file, runs)
                    return r
            logger.warning(f"Could not locate active pipeline run to close: {pipeline_run_id}")
        except Exception as e:
            logger.error(f"Failed to end pipeline run metadata record: {str(e)}")
            
        return {}
        
    def log_step(self, 
                 pipeline_run_id: str, 
                 pipeline_stage: str, 
                 status: str, 
                 duration_ms: int,
                 records_read: int = 0,
                 records_written: int = 0,
                 records_inserted: int = 0,
                 records_updated: int = 0,
                 records_deleted: int = 0,
                 records_rejected: int = 0,
                 duplicate_count: int = 0,
                 validation_failures: int = 0,
                 error_message: str = None) -> Dict[str, Any]:
        """Logs metadata execution metrics for a specific pipeline stage step."""
        logger.info(f"Logging step metrics: run_id={pipeline_run_id}, stage={pipeline_stage}, status={status}")
        
        step_record = {
            "pipeline_run_id": pipeline_run_id,
            "pipeline_stage": pipeline_stage,
            "start_time": (datetime.utcnow()).isoformat(), # default timestamp placeholder
            "end_time": (datetime.utcnow()).isoformat(),
            "execution_duration_ms": duration_ms,
            "execution_status": status,
            "records_read": records_read,
            "records_written": records_written,
            "records_inserted": records_inserted,
            "records_updated": records_updated,
            "records_deleted": records_deleted,
            "records_rejected": records_rejected,
            "duplicate_count": duplicate_count,
            "validation_failures": validation_failures,
            "error_message": error_message
        }
        
        try:
            steps = self._read_json(self.steps_file)
            steps.append(step_record)
            self._write_json(self.steps_file, steps)
        except Exception as e:
            logger.error(f"Failed to record step metadata audit: {str(e)}")
            
        return step_record
