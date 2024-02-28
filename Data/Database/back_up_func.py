import os
import pandas as pd
import psycopg2


def make_backup_and_export_data(nom_base_donnees, dossier_sortie, host_src, port_src, user_src, password_src,
                                host_dest, port_dest, db_name_dest, user_dest, password_dest):
    # Delete all files in the backup directory
    for file in os.listdir(dossier_sortie):
        os.remove(os.path.join(dossier_sortie, file))

    # Connexion à la base de données source
    conn_src = psycopg2.connect(
        dbname=nom_base_donnees,
        user=user_src,
        password=password_src,
        host=host_src,
        port=port_src
    )
    cursor_src = conn_src.cursor()

    # Récupération des noms de toutes les tables de la base de données source
    cursor_src.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cursor_src.fetchall()

    # Exporter chaque table dans un fichier CSV séparé
    for table in tables:
        nom_table = table[0]
        nom_fichier_sortie = os.path.join(dossier_sortie, f"{nom_table}.csv")

        # Exécution de la requête pour récupérer toutes les données de la table
        cursor_src.execute(f"SELECT * FROM {nom_table}")
        rows = cursor_src.fetchall()

        # Création du DataFrame à partir des données
        df = pd.DataFrame(rows)

        # Exporter le DataFrame vers un fichier CSV
        df.to_csv(nom_fichier_sortie, index=False, header=False)

    # Fermeture de la connexion à la base de données source
    cursor_src.close()
    conn_src.close()

    # Connexion à la base de données cible
    conn_dest = psycopg2.connect(
        dbname=db_name_dest,
        user=user_dest,
        password=password_dest,
        host=host_dest,
        port=port_dest
    )

    # Création du curseur
    cursor_dest = conn_dest.cursor()

    # Suppression de toutes les tables dans la base de données cible
    cursor_dest.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables_dest = cursor_dest.fetchall()
    for table_dest in tables_dest:
        nom_table_dest = table_dest[0]
        drop_table_query = f"DROP TABLE IF EXISTS {nom_table_dest} CASCADE"
        cursor_dest.execute(drop_table_query)

    # Parcourir le répertoire contenant les fichiers CSV
    for filename in os.listdir(dossier_sortie):
        if filename.endswith(".csv"):
            table_name = os.path.splitext(filename)[0]
            filepath = os.path.join(dossier_sortie, filename)

            # Lecture du fichier CSV dans un DataFrame
            df = pd.read_csv(filepath, header=None)

            # Création de la table dans la base de données cible
            cols = ",".join([f"col{i+1} TEXT" for i in range(len(df.columns))])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})"
            cursor_dest.execute(create_table_query)

            # Construction de la requête d'insertion
            placeholders = ",".join(["%s"] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"

            # Insertion des données dans la base de données cible
            cursor_dest.executemany(insert_query, df.values.tolist())
            conn_dest.commit()

            print(f"Données importées avec succès depuis le fichier '{filename}' dans la table '{table_name}'")

    # Fermeture de la connexion à la base de données cible
    cursor_dest.close()
    conn_dest.close()

# Informations de connexion pour la base de données source
HOST_SRC = 'localhost'
PORT_SRC = 5432
USER_SRC = 'postgres'
PASSWORD_SRC = 'Yann'
DATABASE_SRC = 'First_test'
BACKUP_DIR_SRC = r'C:\Users\yskon\Desktop\Desk_App\Data\Database\back_up'

# Informations de connexion pour la base de données cible
HOST_DEST = 'localhost'
PORT_DEST = 5432
USER_DEST = 'postgres'
PASSWORD_DEST = 'Yann'
DATABASE_DEST = 'back_up'

# Exporter les données depuis la base de données source vers des fichiers CSV et les importer dans la base de données cible
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
