import time
from functools import wraps
from loguru import logger


def retry_on_failure(func):
    @wraps(func)
    def retry_wrapper(*args, **kwargs):
        max_retries = 5
        sleep_time = 10
        retries = 0
        while retries < max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                retries += 1
                logger.warning(
                    f"Retrying {func.__name__} due to {e}, attempt {retries}/{max_retries}"
                )
                time.sleep(sleep_time)
        raise Exception(f"{func.__name__} failed after {max_retries} attempts")

    return retry_wrapper
