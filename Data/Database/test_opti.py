import os
import pandas as pd
import psycopg2
import threading
import multiprocessing
import queue
from functools import partial

from utils.concurrency.threading import Threder




def delete_csv_files(directory, queue):
    for file in os.listdir(directory):
        if file.endswith('.csv'):
            try:
                os.remove(os.path.join(directory, file))
                queue.put(f"Deleted {file}")
            except Exception as e:
                queue.put(f"Error deleting {file}: {e}")

def create_tables_to_csv(conn_src, dossier_sortie, queue):
    cursor_src = conn_src.cursor()

    cursor_src.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cursor_src.fetchall()

    for table in tables:
        nom_table = table[0]
        nom_fichier_sortie = os.path.join(dossier_sortie, f"{nom_table}.csv")

        cursor_src.execute(f"SELECT * FROM {nom_table}")
        rows = cursor_src.fetchall()

        df = pd.DataFrame(rows)
        df.to_csv(nom_fichier_sortie, index=False, header=False)
        queue.put(f"Exported table {nom_table} to {nom_fichier_sortie}")

    cursor_src.close()

def import_csv_to_backup_db(filepath, queue):
    # Créez une nouvelle connexion dans chaque processus enfant
    conn_dest = psycopg2.connect(
        dbname=DATABASE_DEST,
        user=USER_DEST,
        password=PASSWORD_DEST,
        host=HOST_DEST,
        port=PORT_DEST
    )
    cursor_dest = conn_dest.cursor()

    try:
        df = pd.read_csv(filepath, header=None)

        # Get the number of columns in the CSV file
        num_columns = len(df.columns)

        table_name = os.path.splitext(os.path.basename(filepath))[0]

        # Adjust the query based on the number of columns
        cols = ",".join([f"col{i+1} TEXT" for i in range(num_columns)])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})"
        cursor_dest.execute(create_table_query)

        placeholders = ",".join(["%s"] * num_columns)
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"

        cursor_dest.executemany(insert_query, df.values.tolist())
        conn_dest.commit()

        queue.put(f"Successfully imported data from {filepath} into table {table_name}")

    except Exception as e:
        queue.put(f"Error importing data from {filepath}: {e}")

    cursor_dest.close()
    conn_dest.close()

def run_backup_process(conn_src, BACKUP_DIR_SRC, conn_dest, q):
    # Step 1: Delete all CSV files in the backup directory
    delete_csv_files(BACKUP_DIR_SRC, q)

    # Step 2: Create CSV files from the source database
    create_tables_to_csv(conn_src, BACKUP_DIR_SRC, q)

     # Step 3: Import all CSV files into the destination database
    csv_files = [os.path.join(BACKUP_DIR_SRC, f) for f in os.listdir(BACKUP_DIR_SRC) if f.endswith('.csv')]

    # Define the import function with partial for arguments other than filepath
    import_func = partial(import_csv_to_backup_db, queue=q)

    # Create Threder instance
    threder = Threder()

    # Run import_csv_to_backup_db function in parallel using Threder
    threder.run_in_parallel([partial(import_func, filepath) for filepath in csv_files])

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

# Create a queue
q = queue.Queue()

# Run the backup process
run_backup_process(conn_src, BACKUP_DIR_SRC, conn_dest, q)

# Print queue messages
while not q.empty():
    print(q.get())

# Fermeture des connexions
conn_src.close()
conn_dest.close()
