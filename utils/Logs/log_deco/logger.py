import os
import logging
from functools import wraps

def logs(func):
    """
    Decorator function to set up logging for other functions.
    """
    log_directory = "history_log"
    log_file = "all_log.log"

    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            # Ensure the log directory exists
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)
                print(f"Created directory: {log_directory}")

            # Set up logging
            log_path = os.path.join(log_directory, log_file)
            logging.basicConfig(
                format='%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filemode='a',
                filename=log_path,
                level=logging.DEBUG
            )

            # Verify logging setup
            if os.path.exists(log_path):
                print(f"Log file created: {log_path}")
            else:
                print(f"Failed to create log file: {log_path}")
                return

            # Get logger
            logger = logging.getLogger(func.__name__)

            # Log function start
            logger.info("Function '{}' started with args: {}, kwargs: {}".format(func.__name__, args, kwargs))

            # Call the decorated function
            result = func(*args, **kwargs)

            # Log function end
            logger.info("Function '{}' finished with result: {}".format(func.__name__, result))

            return result

        except Exception as e:
            # Log and raise any exceptions
            logger = logging.getLogger(func.__name__)
            logger.error(f'Error in function {func.__name__}: {e}')
            raise e

    return wrapper