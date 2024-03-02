import os
import logging
from datetime import datetime
import subprocess
import  psycopg2
from test_opti import run_backup_process
import queue
from functools import wraps

# Informations de connexion pour la base de données source
HOST_SRC = 'localhost'
PORT_SRC = 5432
USER_SRC = 'postgres'
PASSWORD_SRC = 'Yann'
DATABASE_SRC = 'First_test'
BACKUP_DIR_SRC = r'C:\Users\yskon\Desktop\Desk_App\Data\Database\back_up'

# Informations de connexion pour la base de données de sauvegarde
HOST_DEST = 'localhost'
PORT_DEST = 5432
USER_DEST = 'postgres'
PASSWORD_DEST = 'Yann'
DATABASE_DEST = 'back_up'

# Connexion à la base de données source
conn_src = psycopg2.connect(
    dbname=DATABASE_SRC,
    user=USER_SRC,
    password=PASSWORD_SRC,
    host=HOST_SRC,
    port=PORT_SRC
)

# Connexion à la base de données de sauvegarde
conn_dest = psycopg2.connect(
    dbname=DATABASE_DEST,
    user=USER_DEST,
    password=PASSWORD_DEST,
    host=HOST_DEST,
    port=PORT_DEST
)


    
"""
def backup(func):
    
    
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
                # Create a queue
            q = queue.Queue()

                # Run the backup process
            run_backup_process(conn_src, BACKUP_DIR_SRC, conn_dest, q)

                # Appel de la fonction décorée
            result = func(*args, **kwargs)
            return result

        except Exception as e:
            # Enregistrement des erreurs dans les logs
            logging.error(f'Erreur lors de l\'exécution de la fonction {func.__name__}: {e}')
            raise e

    return wrapper
    
"""






