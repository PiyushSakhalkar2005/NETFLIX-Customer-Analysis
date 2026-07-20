from src.utils.logger import get_logger

logger = get_logger("NotificationManager")

class NotificationManager:
    """Mock interface to notify external alert channels (Slack, Email, PagerDuty) on pipeline failures."""
    
    @staticmethod
    def send_alert(run_id: str, stage: str, error_msg: str):
        logger.error(f"[ALERT NOTIFICATION SENT] Pipeline Run {run_id} failed at stage '{stage}'. Error: {error_msg}")
