from functools import wraps
import logging
from requests.exceptions import HTTPError


def handle_authentication(func):
    """
    Decorator to handle authentication for operations.
    Retries the operation if a 401 Unauthorized error occurs.
    """
    @wraps(func)
    def wrapper(operator, *args, **kwargs):
        try:
            return func(operator, *args, **kwargs)
        except HTTPError as e:
            if e.response.status_code == 401:
                logging.info("Token expired. Re-authenticating...")
                operator.consumer.login()
                logging.info("Re-authentication successful. Retrying operation...")
                return func(operator, *args, **kwargs)
            else:
                logging.error("Operation failed")
                logging.debug(f"Error: {e}")
                raise e
        except Exception as e:
            logging.error("Unexpected error occurred")
            logging.debug(f"Error: {e}")
            raise e
    return wrapper
