import os
import logging
from functools import wraps

def logs(func,log_directory="history_log", log_file="all_log.log"):
    """
    Decorator function to set up logging for other functions.
    """
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
                # Initialisation du logger
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)

            logging.basicConfig(
                format='%(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filemode='a',
                filename=os.path.join(log_directory, log_file),
                level=logging.INFO
            )

                # Enregistrement du timestamp de début d'exécution
            logger = logging.getLogger(func.__name__)
            logger.info("Function '{}' started.".format(func.__name__))

                # Appel de la fonction décorée
            result = func(*args, **kwargs)

                # Enregistrement du timestamp de fin d'exécution
            logger.info("Function '{}' finished.".format(func.__name__))
            return result

        except Exception as e:
                # Enregistrement des erreurs dans les logs
            logging.error(f'Error in function {func.__name__}: {e}')
            raise e

    return wrapper

