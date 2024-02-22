import psycopg2
from psycopg2 import sql
import re 
import json

hostname = 'localhost'
database = 'First_test'
username = 'postgres'
pwd = 'Yann'
port_id = 5432

def connect_to_database():
    try:
        conn = psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id
        )
        return conn
    except Exception as error:
        print(error)
        return None

# Function to sanitize column names
def sanitize_column_name(name):
    # Replace non-alphanumeric characters with underscores
    return re.sub(r'\W+', '_', name)

def create_table_from_json():
    
    conn = connect_to_database()
    table_name = input("Enter the table name: ")
    while not isinstance(table_name, str):
        print("Table name must be a string.")
        table_name = input("Enter the table name: ")
    file_path = input("Enter the path to the JSON file: ")

    try:
        with open(file_path, 'r') as file:
            json_input = json.load(file)
    except FileNotFoundError:
        print("File not found. Please enter a valid file path.")
        
    if conn:
        try:
            with conn.cursor() as cur:
                # Sanitize and validate column names
                columns = set()
                for key, value in json_input.items():
                    if isinstance(value, list):
                        for item in value:
                            for inner_key in item.keys():
                                columns.add(sanitize_column_name(inner_key))
                    else:
                        columns.add(sanitize_column_name(key))

                # Construct SQL query
                columns_definition = [sql.Identifier(column) + sql.SQL(" VARCHAR") for column in columns]
                query = sql.SQL("CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, {});").format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(columns_definition)
                )

                # Execute query
                cur.execute(query)
                conn.commit()

                print(f"Table {table_name} created successfully.")
        except Exception as error:
            print("Error:", error)
        finally:
            conn.close()

def check_and_drop(table_name):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                # Récupérer les noms de toutes les colonnes de la table
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table_name,))
                column_names = [row[0] for row in cur.fetchall()]

                # Vérifier chaque colonne
                for column_name in column_names:
                    # Sélectionner 10 lignes aléatoires de la colonne
                    cur.execute("SELECT {} FROM {} TABLESAMPLE SYSTEM(50)".format(column_name, table_name))
                    values = [row[0] for row in cur.fetchall()]

                    # Vérifier si toutes les valeurs sont nulles
                    if all(value is None for value in values):
                        # Supprimer la colonne si toutes les valeurs sont nulles
                        cur.execute("ALTER TABLE {} DROP COLUMN {}".format(table_name, column_name))
                        print("Column {} dropped.".format(column_name))

            conn.commit()
        except Exception as error:
            print(error)
        finally:
            conn.close()

def insert_nested_data():
    table_name = input("Enter the table name: ")
    while not isinstance(table_name, str):
        print("Table name must be a string.")
        table_name = input("Enter the table name: ")

    file_path = input("Enter the path to the JSON file: ")

    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
    except FileNotFoundError:
        print("File not found. Please enter a valid file path.")
        return

    # Determine which columns contain only None values
    empty_columns = set()
    for item in data.get("data", []):
        for key, value in item.items():
            if value is None:
                empty_columns.add(key)

    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                for item in data.get("data", []):
                    # Exclude columns that contain only None values
                    columns = [key for key, value in item.items() if key not in empty_columns]
                    placeholders = ", ".join(["%s"] * len(columns))
                    values = tuple(item[column] for column in columns)
                    columns = ", ".join(map(sanitize_column_name, columns))
                    query = sql.SQL("INSERT INTO {} ({}) VALUES ({});").format(
                        sql.Identifier(table_name),
                        sql.SQL(columns),
                        sql.SQL(placeholders)
                    )
                    cur.execute(query, values)
                
                # Supprimer les colonnes ayant des valeurs nulles
                for column in empty_columns:
                    cur.execute(sql.SQL("ALTER TABLE {} DROP COLUMN {}").format(
                        sql.Identifier(table_name),
                        sql.Identifier(column)
                    ))
                
                conn.commit()
                print("Data inserted successfully.")
                check_and_drop(table_name)
        except Exception as error:
            print(error)
        finally:
            conn.close()



def get_data(limit=None):
    table_name = input("Enter the table name: ")
    while not isinstance(table_name, str):
        print("Table name must be a string.")
        table_name = input("Enter the table name: ")
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                if limit:
                    cur.execute(sql.SQL("SELECT * FROM public.{} LIMIT %s;").format(sql.Identifier(table_name)), (limit,))
                else:
                    cur.execute(sql.SQL("SELECT * FROM public.{};").format(sql.Identifier(table_name)))
                rows = cur.fetchall()
                return rows
        except Exception as error:
            print(error)
        finally:
            conn.close()
def get_data_x():
    table_name = input("Enter the table name: ")
    while not isinstance(table_name, str):
        print("Table name must be a string.")
        table_name = input("Enter the table name: ")

    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                # Récupérer les noms de colonnes de la table
                cur.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_name = %s;"), (table_name,))
                columns = [row[0] for row in cur.fetchall()]

                print("Colonnes disponibles dans la table {}:".format(table_name))
                for column in columns:
                    print(column)

                # Demander à l'utilisateur de spécifier les colonnes
                selected_columns = input("Entrez les noms des colonnes séparés par des virgules : column espace virgule espace column  (ou laissez vide pour toutes les colonnes) : ")
                selected_columns = [col.strip() for col in selected_columns.split(",")] if selected_columns else None

                # Vérifier si les colonnes sélectionnées sont parmi celles disponibles
                if selected_columns:
                    for column in selected_columns:
                        if column not in columns:
                            print(f"La colonne '{column}' n'est pas disponible dans la table.")
                            return None

                # Récupérer le nombre total de lignes dans la table
                cur.execute(sql.SQL("SELECT count(*) FROM public.{};").format(sql.Identifier(table_name)))
                total_rows = cur.fetchone()[0]
                print(f"nombre total de data =  {total_rows}")

                # Demander à l'utilisateur le nombre total de données à récupérer
                total_data = input("Entrez le nombre total de données que vous souhaitez récupérer (0 pour tout récupérer) : ")
                try:
                    total_data = int(total_data)
                    if total_data <= 0 or total_data > total_rows:
                        total_data = total_rows
                except ValueError:
                    print("Veuillez entrer un nombre entier valide.")
                    return None

                # Demander à l'utilisateur l'ID de départ
                start_id = input("Entrez l'ID de départ (1 pour commencer) : ")
                try:
                    start_id = int(start_id)
                    if start_id <= 0:
                        start_id = 1
                except ValueError:
                    print("Veuillez entrer un nombre entier valide.")
                    return None

                # Demander à l'utilisateur l'ID de fin si le nombre total n'est pas 1
                if total_data != 1:
                    end_id = start_id + total_data
                    try:
                        end_id = int(end_id)
                        if end_id <= 0 or end_id > total_rows:
                            end_id = total_rows
                    except ValueError:
                        print("Veuillez entrer un nombre entier valide.")
                        return None
                else:
                    end_id = start_id

                # Construire la requête SQL en fonction des intervalles d'IDs et des colonnes sélectionnées
                if selected_columns:
                    columns_sql = sql.SQL(", ").join(map(sql.Identifier, selected_columns))
                    query = sql.SQL("SELECT {} FROM public.{} WHERE id BETWEEN %s AND %s;").format(columns_sql, sql.Identifier(table_name))
                else:
                    query = sql.SQL("SELECT * FROM public.{} WHERE id BETWEEN %s AND %s;").format(sql.Identifier(table_name))

                cur.execute(query, (start_id, end_id))
                rows = cur.fetchall()
                return rows
        except Exception as error:
            print(error)
        finally:
            conn.close()





def update_data( id, data):
    table_name = input("Enter the table name: ")
    while not isinstance(table_name, str):
        print("Table name must be a string.")
        table_name = input("Enter the table name: ")
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                # Sanitize column names and values
                set_values = ", ".join(["{} = %s".format(sanitize_column_name(key)) for key in data.keys()])
                values = tuple(data.values()) + (id,)
                cur.execute("UPDATE {} SET {} WHERE id = %s;".format(table_name, set_values), values)
                conn.commit()
                print("Data updated successfully.")
        except Exception as error:
            print(error)
        finally:
            conn.close()

def delete_data(table_name, id):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM {} WHERE id = %s;".format(table_name), (id,))
                conn.commit()
                print("Data deleted successfully.")
        except Exception as error:
            print(error)
        finally:
            conn.close()