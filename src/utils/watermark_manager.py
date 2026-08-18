import os
import sys
import json
from datetime import datetime
from typing import Dict, Any

# Ensure project root is in system path
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader

logger = get_logger("WatermarkManager")

class WatermarkManager:
    """Manages pipeline watermarks for tracking processed data timestamps and batch checkpoints."""
    
    def __init__(self, config: Dict[str, Any] = None):
        self.config = config or ConfigLoader.load()
        inc_conf = self.config.get("incremental_processing", {})
        self.pipeline_name = inc_conf.get("pipeline_name", "netflix_titles_pipeline")
        
        # Resolve watermark filepath relative to project root
        self.file_path = os.path.join(
            project_root, 
            inc_conf.get("watermark_file_path", "data/metadata/watermarks.json")
        )
        
    def get_watermark(self) -> Dict[str, Any]:
        """
        Retrieves the last successful watermark state.
        If missing, returns epoch default for a Full Load restart.
        """
        default_state = {
            "pipeline_name": self.pipeline_name,
            "last_processed_timestamp": "1970-01-01T00:00:00",
            "last_successful_batch": "INIT",
            "last_business_key": "show_id",
            "last_run_time_ms": 0,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        if not os.path.exists(self.file_path):
            logger.info("Watermark store missing. Returning default epoch state.")
            return default_state
            
        try:
            with open(self.file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data.get(self.pipeline_name, default_state)
        except Exception as e:
            logger.error(f"Failed to read watermark store: {str(e)}. Returning default.")
            return default_state
            
    def update_watermark(self, timestamp: str, batch_id: str, duration_ms: int, business_key: str = "show_id"):
        """Saves a new watermark timestamp, batch details, and business key to the store."""
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        
        store = {}
        if os.path.exists(self.file_path):
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    store = json.load(f)
            except Exception:
                store = {}
                
        store[self.pipeline_name] = {
            "pipeline_name": self.pipeline_name,
            "last_processed_timestamp": timestamp,
            "last_successful_batch": batch_id,
            "last_business_key": business_key,
            "last_run_time_ms": duration_ms,
            "updated_at": datetime.utcnow().isoformat()
        }
        
        try:
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(store, f, indent=2)
            logger.info(f"Watermark updated: last_timestamp={timestamp}, batch_id={batch_id}, business_key={business_key} in {self.file_path}")
        except Exception as e:
            logger.error(f"Failed to write updated watermark: {str(e)}")
            raise
