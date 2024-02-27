import os
import pandas as pd
import psycopg2

def exporter_donnees_csv(nom_base_donnees, dossier_sortie, host, port, user, password):
    # Connexion à la base de données source
    conn = psycopg2.connect(
        dbname=nom_base_donnees,
        user=user,
        password=password,
        host=host,
        port=port
    )
    cursor = conn.cursor()

    # Récupération des noms de toutes les tables de la base de données source
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cursor.fetchall()

    # Création du dossier de sortie s'il n'existe pas
    if not os.path.exists(dossier_sortie):
        os.makedirs(dossier_sortie)

    # Exporter chaque table dans un fichier CSV séparé
    for table in tables:
        nom_table = table[0]
        nom_fichier_sortie = os.path.join(dossier_sortie, f"{nom_table}.csv")

        # Exécution de la requête pour récupérer toutes les données de la table
        cursor.execute(f"SELECT * FROM {nom_table}")
        rows = cursor.fetchall()

        # Création du DataFrame à partir des données
        df = pd.DataFrame(rows)

        # Exporter le DataFrame vers un fichier CSV
        df.to_csv(nom_fichier_sortie, index=False, header=False)

    # Fermeture de la connexion à la base de données source
    cursor.close()
    conn.close()

def importer_donnees_csv(directory, host, port, db_name, user, password):
    # Connexion à la base de données cible
    conn = psycopg2.connect(
        dbname=db_name,
        user=user,
        password=password,
        host=host,
        port=port
    )

    # Création du curseur
    cursor = conn.cursor()

    # Parcourir le répertoire contenant les fichiers CSV
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            table_name = os.path.splitext(filename)[0]
            filepath = os.path.join(directory, filename)

            # Lecture du fichier CSV dans un DataFrame
            df = pd.read_csv(filepath, header=None)

            # Création de la table dans la base de données cible
            cols = ",".join([f"col{i+1} TEXT" for i in range(len(df.columns))])
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({cols})"
            cursor.execute(create_table_query)

            # Construction de la requête d'insertion
            placeholders = ",".join(["%s"] * len(df.columns))
            insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"

            # Insertion des données dans la base de données cible
            cursor.executemany(insert_query, df.values.tolist())
            conn.commit()

            print(f"Données importées avec succès depuis le fichier '{filename}' dans la table '{table_name}'")

    # Fermeture de la connexion à la base de données cible
    cursor.close()
    conn.close()

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

# Exporter les données depuis la base de données source vers des fichiers CSV
exporter_donnees_csv(
    nom_base_donnees=DATABASE_SRC,
    dossier_sortie=BACKUP_DIR_SRC,
    host=HOST_SRC,
    port=PORT_SRC,
    user=USER_SRC,
    password=PASSWORD_SRC
)

# Importer les données depuis les fichiers CSV dans la base de données cible
importer_donnees_csv(
    directory=BACKUP_DIR_SRC,
    host=HOST_DEST,
    port=PORT_DEST,
    db_name=DATABASE_DEST,
    user=USER_DEST,
    password=PASSWORD_DEST
)
