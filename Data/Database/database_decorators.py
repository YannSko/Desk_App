import os
import logging
from datetime import datetime
import subprocess
from back_up_func import make_backup_and_export_data

# Bdd source
HOST_SRC = 'localhost'
PORT_SRC = 5432
USER_SRC = 'postgres'
PASSWORD_SRC = 'Yann'
DATABASE_SRC = 'First_test'
BACKUP_DIR_SRC = r'C:\Users\yskon\Desktop\Desk_App\Data\Database\back_up'

# BDD back up
HOST_DEST = 'localhost'
PORT_DEST = 5432
USER_DEST = 'postgres'
PASSWORD_DEST = 'Yann'
DATABASE_DEST = 'back_up'


# Configuration du logger
def setup_logger(log_file):
    log_directory = r'C:\Users\yskon\Desktop\Desk_App\log'
    if not os.path.exists(log_directory):
        os.makedirs(log_directory)
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename=os.path.join(log_directory, log_file),
                        filemode='a')

# Décorateur de log et de sauvegarde de base de données
def log_and_backup(func):
    def wrapper(*args, **kwargs):
        # Initialisation du logger
        setup_logger('log_file.log')

        try:
            # Enregistrement du timestamp de début d'exécution
            start_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logging.info(f'Début de l\'exécution de la fonction {func.__name__} à {start_time}')

            make_backup_and_export_data(
                nom_base_donnees=DATABASE_SRC,
                dossier_sortie=BACKUP_DIR_SRC,
                host_src=HOST_SRC,
                port_src=PORT_SRC,
                user_src=USER_SRC,
                password_src=PASSWORD_SRC,
                host_dest=HOST_DEST,
                port_dest=PORT_DEST,
                db_name_dest=DATABASE_DEST,
                user_dest=USER_DEST,
                password_dest=PASSWORD_DEST
            )

            # Appel de la fonction décorée
            result = func(*args, **kwargs)

            # Enregistrement du timestamp de fin d'exécution
            end_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            logging.info(f'Fin de l\'exécution de la fonction {func.__name__} à {end_time}')

            return result
        except Exception as e:
            # Enregistrement des erreurs dans les logs
            logging.error(f'Erreur lors de l\'exécution de la fonction {func.__name__}: {e}')
            raise e

    return wrapper




