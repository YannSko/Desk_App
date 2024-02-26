import logging
from datetime import datetime

def setup_logger(log_file):
    
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=log_file,
                        filemode='a')

def log_interaction(func):
    def wrapper(*args, **kwargs):
        # initialisation date et heure
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # enregistrement dans le journal
        logging.info(f'Date/Time: {current_time}, Data: {args}, Action: {func.__name__}')
        return func(*args, **kwargs)
    return wrapper
