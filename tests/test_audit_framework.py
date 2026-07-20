import os
import sys
import shutil
import pytest
from datetime import datetime
from unittest.mock import MagicMock

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.utils.audit_manager import AuditManager

@pytest.fixture
def temp_audit_dir(tmp_path):
    """Fixture creating a temporary directory for audit outputs."""
    audit_path = tmp_path / "metadata"
    audit_path.mkdir(parents=True, exist_ok=True)
    return audit_path

def test_audit_run_lifecycle(temp_audit_dir):
    # Initialize AuditManager referencing temp directory paths
    config = {
        "storage": {
            "metadata_dir": str(temp_audit_dir)
        }
    }
    
    manager = AuditManager(config)
    manager.runs_file = str(temp_audit_dir / "pipeline_runs_audit.json")
    manager.steps_file = str(temp_audit_dir / "pipeline_steps_audit.json")
    
    # 1. Start a run
    run_id = "test-run-123"
    batch_id = "test-batch-abc"
    run_start = manager.start_run(run_id, "test_pipeline", batch_id)
    
    assert run_start["pipeline_run_id"] == run_id
    assert run_start["execution_status"] == "RUNNING"
    assert run_start["batch_id"] == batch_id
    assert os.path.exists(manager.runs_file)
    
    # 2. Log step execution metrics
    step_record = manager.log_step(
        pipeline_run_id=run_id,
        pipeline_stage="Silver_Cleansing",
        status="SUCCESS",
        duration_ms=150,
        records_read=100,
        records_written=95,
        records_rejected=5
    )
    
    assert step_record["pipeline_stage"] == "Silver_Cleansing"
    assert step_record["execution_status"] == "SUCCESS"
    assert step_record["records_read"] == 100
    assert step_record["records_rejected"] == 5
    assert os.path.exists(manager.steps_file)
    
    # 3. Complete pipeline run
    run_end = manager.end_run(run_id, "SUCCESS", spark_app_id="spark-12345")
    
    assert run_end["execution_status"] == "SUCCESS"
    assert run_end["spark_application_id"] == "spark-12345"
    assert run_end["execution_duration_ms"] >= 0

def test_audit_failure_tolerance():
    # Setup manager pointing to invalid folder which normally throws OS error
    manager = AuditManager(None)
    manager.runs_file = "Z:\\nonexistent_drive\\nonexistent_folder\\runs.json"
    manager.steps_file = "Z:\\nonexistent_drive\\nonexistent_folder\\steps.json"
    
    # This should fail silently and log the error rather than throwing an exception
    try:
        run_record = manager.start_run("fail-run", "fail_pipeline", "b1")
        assert run_record is not None
        
        step_record = manager.log_step("fail-run", "Bronze", "FAILED", 100, error_message="mock error")
        assert step_record is not None
        
        end_record = manager.end_run("fail-run", "FAILED")
        assert end_record is not None
    except Exception as e:
        pytest.fail(f"AuditManager failed to tolerate invalid target paths: {str(e)}")
