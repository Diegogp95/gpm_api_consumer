from functools import wraps
import logging
from requests.exceptions import HTTPError

logger = logging.getLogger(__name__)

def handle_authentication(func):
    """
    Decorator to handle authentication for operations.
    Retries the operation if a 401 Unauthorized error occurs.
    """
    @wraps(func)
    def wrapper(consumer, *args, **kwargs):
        try:
            return func(consumer, *args, **kwargs)
        except HTTPError as e:
            if e.response.status_code == 401:
                logger.info("Token expired. Re-authenticating...")
                consumer.login()
                logger.info("Re-authentication successful. Retrying operation...")
                return func(consumer, *args, **kwargs)
            else:
                logger.error("Operation failed")
                logger.debug(f"Error: {e}")
                raise e
        except Exception as e:
            logger.error("Unexpected error occurred")
            logger.debug(f"Error: {e}")
            raise e
    return wrapper
