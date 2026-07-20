import pytest
from unittest.mock import patch, MagicMock
from src.utils.retry import retry
from src.utils.notifications import NotificationManager

def test_retry_decorator_success():
    call_count = 0
    
    @retry(attempts=3, delay=0.1)
    def test_func():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("First attempt fails")
        return "Success"
        
    result = test_func()
    assert result == "Success"
    assert call_count == 2

def test_retry_decorator_failure():
    call_count = 0
    
    @retry(attempts=3, delay=0.1)
    def test_func():
        nonlocal call_count
        call_count += 1
        raise ValueError("Constant failure")
        
    with pytest.raises(ValueError):
        test_func()
    assert call_count == 3

def test_notification_alert_log(caplog):
    with patch("src.utils.notifications.logger") as mock_logger:
        NotificationManager.send_alert("run-1", "StageX", "ConnectionTimeout")
        mock_logger.error.assert_called_once_with(
            "[ALERT NOTIFICATION SENT] Pipeline Run run-1 failed at stage 'StageX'. Error: ConnectionTimeout"
        )
