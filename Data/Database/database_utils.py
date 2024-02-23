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
# Fonction pour nettoyer les noms de colonnes
def sanitize_column_name(name):
    # Remplacer les caractères spéciaux et les espaces par des underscores
    sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    return sanitized_name

# Créer une table à partir d'un fichier JSON
def create_table():
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
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
                    return
                
                # Fonction pour récursivement créer des colonnes
                def process_json(data):
                    for key, value in data.items():
                        if isinstance(value, dict):
                            process_json(value)
                        else:
                            columns.add(sanitize_column_name(key))
                
                time_series_data = None
                if "Time Series (Digital Currency Monthly)" in json_input:
                    time_series_data = json_input["Time Series (Digital Currency Monthly)"]
                elif "Time Series FX (Monthly)" in json_input:
                    time_series_data = json_input["Time Series FX (Monthly)"]
                    meta_data = json_input.get("Meta Data", {})
                    from_symbol = meta_data.get("2. From Symbol", "")
                    to_symbol = meta_data.get("3. To Symbol", "")
                elif "data" in json_input:
                    time_series_data = {entry["date"]: {"value": entry["value"]} for entry in json_input["data"]}

                if time_series_data is None:
                    print("No time series data found in the JSON.")
                    return

                columns = set()
                
                process_json(time_series_data)

                columns.add(sanitize_column_name("From Symbol"))
                columns.add(sanitize_column_name("To Symbol"))
                columns.add(sanitize_column_name("Digital Currency Code"))
                columns.add(sanitize_column_name("Digital Currency Name"))

                columns_definition = [sql.Identifier(column) + sql.SQL(" VARCHAR") for column in columns]
                query = sql.SQL("CREATE TABLE IF NOT EXISTS {} (id SERIAL PRIMARY KEY, date DATE, {});").format(
                    sql.Identifier(table_name),
                    sql.SQL(", ").join(columns_definition)
                )

                cur.execute(query)
                conn.commit()

                print(f"Table {table_name} created successfully.")
        except Exception as error:
            print("Error:", error)
        finally:
            conn.close()

# Vérifier et supprimer les colonnes vides de la table
def check_and_drop(table_name):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table_name,))
                column_names = [row[0] for row in cur.fetchall()]

                for column_name in column_names:
                    cur.execute("SELECT {} FROM {} TABLESAMPLE SYSTEM(70)".format(column_name, table_name))
                    values = [row[0] for row in cur.fetchall()]

                    if all(value is None for value in values):
                        cur.execute("ALTER TABLE {} DROP COLUMN {}".format(table_name, column_name))
                        print("Column {} dropped.".format(column_name))

            conn.commit()
        except Exception as error:
            print(error)
        finally:
            conn.close()


# Fonction pour insérer des données depuis un JSON
# Fonction pour insérer des données depuis un JSON

import re
def insert_forex_data_json():
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                table_name = input("Entrez le nom de la table : ")
                while not isinstance(table_name, str):
                    print("Le nom de la table doit être une chaîne de caractères.")
                    table_name = input("Entrez le nom de la table : ")

                file_path = input("Entrez le chemin du fichier JSON : ")

                try:
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        print("Données JSON :", data)
                except FileNotFoundError:
                    print("Fichier non trouvé. Veuillez entrer un chemin de fichier valide.")
                    return

                # Récupérer les noms de colonnes de la table dans la base de données
                cur.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_name = %s;"), (table_name,))
                column_names = [row[0] for row in cur.fetchall()]
                print("Noms de colonnes de la base de données :", column_names)

                def insert_data(data, to_symbol, from_symbol):
                    for date, values in data.items():
                        try:
                            sanitized_columns = ['date'] + [sanitize_column_name(column) for column in values.keys()]
                            sanitized_columns.extend(['To_Symbol', 'From_Symbol'])
                            columns = ', '.join(map(str, sanitized_columns))
                            placeholders = ', '.join(['%s'] * len(sanitized_columns))
                            
                            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                                sql.Identifier(table_name),
                                sql.SQL(', ').join(map(sql.Identifier, sanitized_columns)),
                                sql.SQL(placeholders)
                            )
                            
                            # Vérification de la requête SQL avant l'exécution
                            print("Requête SQL avant exécution :", cur.mogrify(query, [date] + list(values.values()) + [to_symbol, from_symbol]))
                            
                            # Exécution de la requête
                            cur.execute(query, [date] + list(values.values()) + [to_symbol, from_symbol])
                        except Exception as e:
                            print(f"Erreur lors de l'insertion des données pour la date {date}: {e}")
                            continue





                



                
                meta_data = data.get("Meta Data", {})
                to_symbol = meta_data.get("3. To Symbol")
                from_symbol = meta_data.get("2. From Symbol")

                if "Time Series FX (Monthly)" in data:
                    time_series_data = data["Time Series FX (Monthly)"]
                    
                    insert_data(time_series_data, to_symbol, from_symbol)
                    conn.commit()
                    print("Données insérées avec succès.")
                else:
                    print("Aucune donnée 'Time Series FX (Monthly)' trouvée dans le JSON.")


        except Exception as error:
            print(error)
        finally:
            conn.close()

################################################ CRYPTO####################################################
            
def insert_crypto_data_json():
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                table_name = input("Enter the table name: ")
                while not isinstance(table_name, str):
                    print("Table name must be a string.")
                    table_name = input("Enter the table name: ")

                file_path = input("Enter the JSON file path: ")

                try:
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        print("JSON data:", data)
                except FileNotFoundError:
                    print("File not found. Please enter a valid file path.")
                    return

                cur.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_name = %s;"), (table_name,))
                column_names = [row[0] for row in cur.fetchall()]
                print("Column names in the database:", column_names)

                def insert_data(data, digital_currency_code, digital_currency_name):
                    for date, values in data.items():
                        try:
                            sanitized_columns = ['date'] + [sanitize_column_name(column) for column in values.keys()]
                            sanitized_columns.extend(['Digital_Currency_Code', 'Digital_Currency_Name'])
                            columns = ', '.join(map(str, sanitized_columns))
                            placeholders = ', '.join(['%s'] * len(sanitized_columns))
                            
                            query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                                sql.Identifier(table_name),
                                sql.SQL(', ').join(map(sql.Identifier, sanitized_columns)),
                                sql.SQL(placeholders)
                            )
                            
                            print("Query before execution:", cur.mogrify(query, [date] + list(values.values()) + [digital_currency_code, digital_currency_name]))
                            
                            cur.execute(query, [date] + list(values.values()) + [digital_currency_code, digital_currency_name])
                        except psycopg2.Error as error:
                            if error.pgcode == '42P01':  # Syntax error
                                print("Syntax error in SQL query:", error)
                            elif error.pgcode == '23505':  # Constraint violation
                                print("Constraint violation:", error)
                            elif error.pgcode == '55P03':  # Concurrent lock error
                                print("Concurrent lock error:", error)
                            elif error.pgcode == '08000':  # Connection error
                                print("Connection error:", error)
                            elif error.pgcode == '25P02':  # Transaction parameter error
                                print("Transaction parameter error:", error)
                            else:
                                print("Other database error:", error)
                        except Exception as e:
                            print(f"Error inserting data for date {date}: {e}")
                            continue
                
                meta_data = data.get("Meta Data", {})
                digital_currency_code = meta_data.get("2. Digital Currency Code")
                digital_currency_name = meta_data.get("3. Digital Currency Name")
                

                if "Time Series (Digital Currency Monthly)" in data:
                    time_series_data = data["Time Series (Digital Currency Monthly)"]
                    
                    insert_data(time_series_data, digital_currency_code, digital_currency_name)
                    conn.commit()
                    print("Data inserted successfully.")
                    print("Column names in the database:", column_names)
                else:
                    print("No 'Time Series (Digital Currency Monthly)' data found in the JSON.")

        except psycopg2.Error as error:
            if error.pgcode == '42P01':  # Syntax error
                print("Syntax error in SQL query:", error)
            elif error.pgcode == '23505':  # Constraint violation
                print("Constraint violation:", error)
            elif error.pgcode == '55P03':  # Concurrent lock error
                print("Concurrent lock error:", error)
            elif error.pgcode == '08000':  # Connection error
                print("Connection error:", error)
            elif error.pgcode == '25P02':  # Transaction parameter error
                print("Transaction parameter error:", error)
            else:
                print("Other database error:", error)
        except Exception as error:
            print("An unexpected error occurred:", error)
        finally:
            conn.close()


#### JSON CLASSIQUE as Commodities_datas

def insert_data_json():
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                table_name = input("Enter the table name: ")
                while not isinstance(table_name, str):
                    print("Table name must be a string.")
                    table_name = input("Enter the table name: ")

                file_path = input("Enter the JSON file path: ")

                try:
                    with open(file_path, 'r') as file:
                        data = json.load(file)
                        print("JSON data:", data)
                except FileNotFoundError:
                    print("File not found. Please enter a valid file path.")
                    return

                cur.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_name = %s;"), (table_name,))
                column_names = [row[0] for row in cur.fetchall()]
                print("Column names in the database:", column_names)

                def insert_data(data):
                    for item in data['data']:
                        try:
                            date = item['date']
                            value = item['value']
                            query = sql.SQL("INSERT INTO {} (id, date, value) VALUES (DEFAULT, %s, %s)").format(
                                sql.Identifier(table_name))
                            
                            print("Query before execution:", cur.mogrify(query, (date, value)))
                            
                            cur.execute(query, (date, value))
                        except psycopg2.Error as error:
                            if error.pgcode == '42P01':  # Syntax error
                                print("Syntax error in SQL query:", error)
                            elif error.pgcode == '23505':  # Constraint violation
                                print("Constraint violation:", error)
                            elif error.pgcode == '55P03':  # Concurrent lock error
                                print("Concurrent lock error:", error)
                            elif error.pgcode == '08000':  # Connection error
                                print("Connection error:", error)
                            elif error.pgcode == '25P02':  # Transaction parameter error
                                print("Transaction parameter error:", error)
                            else:
                                print("Other database error:", error)
                        except Exception as e:
                            print(f"Error inserting data for date {date}: {e}")
                            continue
                
                if 'data' in data:
                    insert_data(data)
                    conn.commit()
                    print("Data inserted successfully.")
                    print("Column names in the database:", column_names)
                else:
                    print("No 'data' found in the JSON.")
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





def update_data():
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                print("Options disponibles :")
                print("1. Modifier le nom d'une table.")
                print("2. Modifier le nom d'une colonne.")
                print("3. Modifier la valeur d'une cellule dans une colonne.")

                choice = input("Entrez le numéro correspondant à votre choix : ")

                if choice == '1':
                    # Modifier le nom d'une table
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                    tables = [row[0] for row in cur.fetchall()]
                    print("Tables disponibles :")
                    for table in tables:
                        print(table)

                    old_table_name = input("Entrez le nom de la table à modifier : ")
                    new_table_name = input("Entrez le nouveau nom pour la table : ")

                    if old_table_name in tables:
                        cur.execute("ALTER TABLE {} RENAME TO {};".format(old_table_name, new_table_name))
                        print("Le nom de la table {} a été modifié avec succès en {}.".format(old_table_name, new_table_name))
                        conn.commit()
                    else:
                        print("La table spécifiée n'existe pas.")
                    
                elif choice == '2':
                    # Modifier le nom d'une colonne
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                    tables = [row[0] for row in cur.fetchall()]
                    print("Tables disponibles :")
                    for table in tables:
                        print(table)
                    table_name = input("Entrez le nom de la table : ")

                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (table_name,))
                    columns = [row[0] for row in cur.fetchall()]
                    print("Colonnes disponibles dans la table {} :".format(table_name))
                    for column in columns:
                        print(column)

                    old_column_name = input("Entrez le nom de la colonne à modifier : ")
                    new_column_name = input("Entrez le nouveau nom pour la colonne : ")

                    if old_column_name in columns:
                        cur.execute("ALTER TABLE {} RENAME COLUMN {} TO {};".format(table_name, old_column_name, new_column_name))
                        print("Le nom de la colonne {} de la table {} a été modifié avec succès en {}.".format(old_column_name, table_name, new_column_name))
                        conn.commit()
                    else:
                        print("La colonne spécifiée n'existe pas dans la table {}.".format(table_name))

                elif choice == '3':
                    # Modifier la valeur d'une cellule dans une colonne
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                    tables = [row[0] for row in cur.fetchall()]
                    print("Tables disponibles :")
                    for table in tables:
                        print(table)
                    table_name = input("Entrez le nom de la table : ")

                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (table_name,))
                    columns = [row[0] for row in cur.fetchall()]
                    print("Colonnes disponibles dans la table {} :".format(table_name))
                    for column in columns:
                        print(column)

                    column_name = input("Entrez le nom de la colonne à modifier : ")
                    id_to_update = int(input("Entrez l'ID de la ligne à modifier : "))

                    if column_name in columns:
                        cur.execute("SELECT {} FROM {} WHERE id = %s;".format(column_name, table_name), (id_to_update,))
                        old_value = cur.fetchone()[0]
                        print("La valeur actuelle de la cellule dans la colonne {} et l'ID {} est : {}".format(column_name, id_to_update, old_value))
                        new_value = input("Entrez la nouvelle valeur pour la cellule : ")

                        # Vérifier la compatibilité du type de valeur avec le type de la colonne
                        cur.execute("SELECT data_type FROM information_schema.columns WHERE table_name = %s AND column_name = %s;", (table_name, column_name))
                        column_data_type = cur.fetchone()[0]

                        if column_data_type == 'integer':
                            try:
                                new_value = int(new_value)
                            except ValueError:
                                print("La colonne '{}' accepte uniquement des valeurs entières.".format(column_name))
                                return
                        elif column_data_type == 'numeric':
                            try:
                                new_value = float(new_value)
                            except ValueError:
                                print("La colonne '{}' accepte uniquement des valeurs numériques.".format(column_name))
                                return

                        cur.execute("UPDATE {} SET {} = %s WHERE id = %s;".format(table_name, column_name), (new_value, id_to_update))
                        print("La valeur de la cellule dans la colonne {} et l'ID {} a été modifiée avec succès.".format(column_name, id_to_update))
                        conn.commit()
                    else:
                        print("La colonne spécifiée n'existe pas dans la table {}.".format(table_name))

                else:
                    print("Choix invalide. Veuillez choisir une option valide (1, 2 ou 3).")

        except Exception as error:
            print(error)
        finally:
            conn.close()


def delete_data():
    print("Options disponibles :")
    print("1. Supprimer une ou plusieurs tables spécifiques.")
    print("2. Supprimer une ou plusieurs colonnes spécifiques d'une table.")
    print("3. Supprimer une ou plusieurs lignes spécifiques d'une table en fonction de leur ID.")

    choice = input("Entrez le numéro correspondant à votre choix : ")

    if choice == '1':
        # Supprimer une ou plusieurs tables spécifiques
        conn = connect_to_database()
        if conn:
            try:
                with conn.cursor() as cur:
                    # Récupérer les noms de toutes les tables disponibles
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                    tables = [row[0] for row in cur.fetchall()]
                    print("Tables disponibles :")
                    for table in tables:
                        print(table)

                    table_names = input("Entrez les noms des tables à supprimer, séparés par des virgules : ").split(',')
                    for table_name in table_names:
                        if table_name.strip() not in tables:
                            print("La table '{}' n'existe pas.".format(table_name.strip()))
                            return

                    for table_name in table_names:
                        cur.execute("DROP TABLE IF EXISTS {};".format(table_name.strip()))
                        print("La table {} a été supprimée avec succès.".format(table_name.strip()))
                    conn.commit()
            except Exception as error:
                print("Erreur lors de la suppression des tables :", error)
            finally:
                conn.close()

    elif choice == '2':
        # Supprimer une ou plusieurs colonnes spécifiques d'une table
        conn = connect_to_database()
        if conn:
            try:
                with conn.cursor() as cur:
                    # Récupérer les noms de toutes les tables disponibles
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                    tables = [row[0] for row in cur.fetchall()]
                    print("Tables disponibles :")
                    for table in tables:
                        print(table)
                    table_name = input("Entrez le nom de la table : ")
                    # Récupérer les noms de toutes les colonnes de la table spécifiée
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (table_name,))
                    columns = [row[0] for row in cur.fetchall()]
                    print("Colonnes disponibles dans la table {} :".format(table_name))
                    for column in columns:
                        print(column)

                    column_names = input("Entrez les noms des colonnes à supprimer, séparés par des virgules : ").split(',')
                    for column_name in column_names:
                        if column_name.strip() not in columns:
                            print("La colonne '{}' n'existe pas dans la table '{}'.".format(column_name.strip(), table_name))
                            return

                    for column_name in column_names:
                        cur.execute("ALTER TABLE {} DROP COLUMN {};".format(table_name, column_name.strip()))
                        print("La colonne {} de la table {} a été supprimée avec succès.".format(column_name.strip(), table_name))
                    conn.commit()
            except Exception as error:
                print("Erreur lors de la suppression des colonnes :", error)
            finally:
                conn.close()

    elif choice == '3':
        # Supprimer une ou plusieurs lignes spécifiques d'une table en fonction de leur ID
        conn = connect_to_database()
        if conn:
            try:
                with conn.cursor() as cur:
                    # Récupérer les noms de toutes les tables disponibles
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                    tables = [row[0] for row in cur.fetchall()]
                    print("Tables disponibles :")
                    for table in tables:
                        print(table)
                    table_name = input("Entrez le nom de la table : ")
                    # Récupérer les noms de toutes les colonnes de la table spécifiée
                    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s;", (table_name,))
                    columns = [row[0] for row in cur.fetchall()]
                    print("Colonnes disponibles dans la table {} :".format(table_name))
                    for column in columns:
                        print(column)

                    ids = input("Entrez les ID des lignes à supprimer, séparés par des virgules : ").split(',')
                    for id in ids:
                        cur.execute("DELETE FROM {} WHERE id = %s;".format(table_name), (id.strip(),))
                        print("La ligne avec l'ID {} de la table {} a été supprimée avec succès.".format(id.strip(), table_name))
                    conn.commit()
            except Exception as error:
                print("Erreur lors de la suppression des lignes :", error)
            finally:
                conn.close()

    else:
        print("Choix invalide. Veuillez choisir une option valide (1, 2 ou 3).")

