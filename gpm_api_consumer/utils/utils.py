import logging
from functools import wraps
from itertools import islice
import unicodedata

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

def normalize_name(name: str) -> str:
    # Normalize the name to remove accents and special characters
    # and replace spaces and hyphens with underscores.
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = "".join([c for c in nfkd if not unicodedata.combining(c)])
    ascii_name = ascii_name.replace(" ", "_").replace("-", "_")
    safe = "".join([c for c in ascii_name if c.isalnum() or c == "_"])
    return safe