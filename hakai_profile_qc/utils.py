import time
from functools import wraps

def retry(attempts=3, delay=1, exceptions=Exception):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    print(f"Attempt failed with error: {e}. Retrying...")
                    time.sleep(delay)
            raise Exception(f"Failed to execute {func.__name__} after {attempts} attempts")
        return wrapper
    return decorator