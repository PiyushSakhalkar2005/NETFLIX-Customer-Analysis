import time
import functools
from src.utils.logger import get_logger

logger = get_logger("RetryDecorator")

def retry(attempts: int = 3, delay: float = 2.0, backoff: float = 2.0):
    """Decorator to retry function execution on exception with exponential backoff."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            curr_delay = delay
            for attempt in range(1, attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == attempts:
                        logger.error(f"Function {func.__name__} failed after {attempts} attempts. Error: {str(e)}")
                        raise
                    logger.warning(f"Function {func.__name__} failed on attempt {attempt}/{attempts}. Retrying in {curr_delay}s... Error: {str(e)}")
                    time.sleep(curr_delay)
                    curr_delay *= backoff
        return wrapper
    return decorator
