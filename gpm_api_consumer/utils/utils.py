import logging
from functools import wraps
from itertools import islice

def set_logger_level(temp_level):
    """
    Decorator to temporarily set the logger level while executing the decorated function.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger()  # Get the root logger
            original_level = logger.level  # Save the original logger level
            logger.setLevel(temp_level)  # Set the temporary logger level
            try:
                return func(*args, **kwargs)
            finally:
                logger.setLevel(original_level)  # Restore the original logger level
        return wrapper
    return decorator

def chunked_iterable(iterable, size):
    """
    Divide un iterable en trozos de tamaño `size`.
    """
    it = iter(iterable)
    while chunk := list(islice(it, size)):
        yield chunk
