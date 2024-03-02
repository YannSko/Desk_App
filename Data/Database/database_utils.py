

import psycopg2
from psycopg2 import sql
import re 
import json
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import pandas as pd
from tkinter import Tk
from tkinter import filedialog
import csv
import openpyxl
import xml.etree.ElementTree as ET
from tkinter import Tk, filedialog
import psycopg2
import os

#from database_decorators import  backup
import logging


from log_deco import logs






HOSTNAME='localhost'
DATABASE='First_test'
username='postgres'
PWD='Yann'
PORT_ID=5432


def connect_to_database():
    try:
        conn = psycopg2.connect(
            host=HOSTNAME,
            dbname=DATABASE,
            user=username,
            password=PWD,
            port=PORT_ID
        )
        return conn
    except Exception as error:
        print(error)
        return None


def df_to_sql_j():
    conn = connect_to_database()
    root = Tk()
    root.withdraw()  # Masquer la fenêtre principale
    json_file_path = filedialog.askopenfilename(initialdir="C:/Users/YourUsername/Desktop", title="Select JSON file", filetypes=(("JSON files", "*.json"), ("All files", "*.*")))
    table_name = input("Enter the table name: ")
    try:
        # Read JSON file into a DataFrame
        with open(json_file_path, 'r') as file:
            data = json.load(file)
            df = pd.DataFrame.from_dict(data)

        # Add 'id' column with sequential values
        df.insert(0, 'id', range(1, 1 + len(df)))

        # Function to flatten nested dictionaries
        def flatten_dict(d):
            flat_dict = {}
            for key, value in d.items():
                if isinstance(value, dict):
                    flat_dict.update(flatten_dict(value))
                else:
                    flat_dict[key] = value
            return flat_dict

        # Flatten nested dictionaries in DataFrame
        for col in df.columns:
            df[col] = df[col].apply(lambda x: flatten_dict(x) if isinstance(x, dict) else x)

        # Split columns with nested dictionaries into separate columns
        for col in df.columns:
            if isinstance(df[col][0], dict):
                expanded_df = df[col].apply(pd.Series)
                expanded_df.columns = [f"{col}_{subcol}" for subcol in expanded_df.columns]
                df = pd.concat([df, expanded_df], axis=1)
                df.drop(columns=[col], inplace=True)

        # Convert DataFrame to a list of tuples (each tuple represents a row)
        data_list = [tuple(row) for row in df.to_numpy()]

        # Create table if it does not exist
        with conn.cursor() as cur:
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} VARCHAR' for col in df.columns])})"
            cur.execute(create_table_query)

            # Insert data into the table
            for row in data_list:
                row = list(row)
                row[-1] = str(row[-1])  # Convert last element (nested dict) to string
                insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(df.columns))})"
                cur.execute(insert_query, row)

        # Prompt user for commit
        user_choice = input("Voulez-vous sauvegarder les données dans la base de données ? (y/n): ")
        if user_choice.lower() == 'y':
            conn.commit()
            print(f"DataFrame from {json_file_path} successfully saved to table {table_name} in the database.")
        elif user_choice.lower() == 'n':
            print("Transaction annulée à la demande de l'utilisateur.")
            conn.rollback()
        else:
            print("Choix invalide. La transaction sera conservée.")
            conn.rollback()

    except Exception as e:
        print("Error:", e)
    finally:
        if conn:
            conn.close()


#################DF_sql big

def create_table_from_df(df, table_name, conn):
    try:
        # Mapping des types de données Pandas vers les types de données PostgreSQL
        type_mapping = {
            "int64": "INTEGER",
            "float64": "NUMERIC",
            "object": "TEXT"
        }
        
        # Récupérer les noms et types de colonnes
        column_names = df.columns
        column_types = [type_mapping.get(df[col].dtype.name, "TEXT") for col in df.columns]
        
        # Ajouter la colonne 'id' à la liste des noms de colonnes et 'INTEGER' à la liste des types de colonnes
        column_names = ['id'] + list(column_names)
        column_types = ['SERIAL'] + list(column_types)
        
        # Créer la requête de création de table
        columns = ", ".join([f"{name} {type}" for name, type in zip(column_names, column_types)])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        
        # Exécuter la requête pour créer la table
        with conn.cursor() as cur:
            cur.execute(create_table_query)
        
        print(f"Table {table_name} created successfully.")
    except Exception as e:
        print("Error:", e)

def df_to_sql_big():
    json_file_path = input("Enter the path to the JSON file: ")
    table_name = input("Enter the table name: ")
    try:
        conn = connect_to_database()
        
        # Charger les données JSON dans un DataFrame
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)
        data = json_data['data']['rows']
        df = pd.DataFrame(data)
        
        # Créer la table si elle n'existe pas
        create_table_from_df(df, table_name, conn)
        
        # Ajouter la colonne 'id' avec des valeurs séquentielles
        df.insert(0, 'id', range(1, 1 + len(df)))
        
        # Convertir le DataFrame en liste de tuples
        records = df.values.tolist()

        # Créer une chaîne de requête d'insertion
        placeholders = ", ".join(["%s"] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"

        # Exécuter la requête d'insertion pour chaque ligne dans le DataFrame
        with conn.cursor() as cur:
            cur.executemany(insert_query, records)
        
        # Valider la transaction
        conn.commit()
        print(f"DataFrame successfully saved to table {table_name} in the database.")
    except Exception as e:
        print("Error:", e)
    finally:
        if conn:
            conn.close()

def sanitize_column_name(name):
    # If column name is a date, wrap it in double quotes
    if re.match(r'^\d{4}-\d{2}-\d{2}$', name):
        return f'"{name}"'
    # Otherwise, sanitize the name to comply with SQL naming conventions
    sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
    sanitized_name = sanitized_name.strip('_')
    return sanitized_name

def df_to_sql_cxe():
    conn = connect_to_database()
    root = Tk()
    root.withdraw()  # Hide the main window
    file_path = filedialog.askopenfilename(initialdir="C:/Users/YourUsername/Desktop", title="Select file",
                                           filetypes=(("CSV files", "*.csv"), ("XML files", "*.xml"), ("Excel files", "*.xlsx")))
    table_name = input("Enter the table name: ")
    try:
        if file_path.endswith('.csv'):
            df = pd.read_csv(file_path)
        elif file_path.endswith('.xml'):
            tree = ET.parse(file_path)
            root = tree.getroot()
            data = []
            for elem in root:
                data.append(elem.attrib)
            df = pd.DataFrame(data)
        elif file_path.endswith('.xlsx'):  # Add handling for Excel files
            df = pd.read_excel(file_path)
        else:
            print("Unsupported file format.")
            return

        # Add 'id' column with sequential values
        df.insert(0, 'id', range(1, 1 + len(df)))

        # Sanitize column names to comply with SQL naming conventions
        df.columns = [sanitize_column_name(col) for col in df.columns]

        # Convert DataFrame to a list of tuples (each tuple represents a row)
        data_list = [tuple(row) for row in df.to_numpy()]

        # Create table if it does not exist
        with conn.cursor() as cur:
            # Handle column names with special characters or spaces
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col} VARCHAR' for col in df.columns])})"
            cur.execute(create_table_query)

            # Insert data into the table
            for row in data_list:
                row = list(row)
                row[-1] = str(row[-1])  # Convert last element (nested dict) to string
                # Handle column names with special characters or spaces
                insert_query = f"INSERT INTO {table_name} ({', '.join([f'{col}' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(df.columns))})"
                cur.execute(insert_query, row)

        conn.commit()
        print(f"DataFrame from {file_path} successfully saved to table {table_name} in the database.")
    except Exception as e:
        print("Error:", e)
    finally:
        if conn:
            conn.close()
    

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


######### Pour stocks data
            
# Fonction pour créer la table
# Fonction pour créer la table à partir du JSON
# Fonction pour lire les données JSON à partir d'un fichier

def read_json_file(file_path):
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        print("File not found. Please enter a valid file path.")
        return None
    except json.JSONDecodeError as e:
        print("Invalid JSON format:", e)
        return None

import logging
import re

# Configure logging
logging.basicConfig(filename='error.log', level=logging.ERROR)

# Function to format column names
def format_column_name(name):
    # Replace spaces with underscores
    formatted_name = name.replace(' ', '_')
    # Remove special characters except underscores
    formatted_name = re.sub(r'[^a-zA-Z0-9_]', '', formatted_name)
    # If the name starts with a number, prefix it with an underscore
    if formatted_name[0].isdigit():
        formatted_name = '_' + formatted_name
    return formatted_name



import time

def create_tables_from_json():
    file_path = input("Enter the JSON file path: ")
    json_data = read_json_file(file_path)
    if json_data is None:
        return

    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                # Create table for company characteristics
                company_table_name = input("Enter the company table name: ")
                
                company_columns = 'id SERIAL PRIMARY KEY, ' + ", ".join(f'"{format_column_name(key)}" TEXT' for key in json_data['A'].keys())
                company_query = f"CREATE TABLE {format_column_name(company_table_name)} ({company_columns})"
                print(company_query)
                cur.execute(company_query)
                print("Company table creation initiated.")

                # Wait until the company table is created
                while True:
                    cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{format_column_name(company_table_name)}')")
                    table_exists = cur.fetchone()[0]
                    if table_exists:
                        print("Company table created successfully.")
                        break
                    time.sleep(1)  # Wait for 1 second before checking again

                # Create table for employees
                employees_table_name = input("Enter the employees table name: ")
                employees_columns = 'id SERIAL PRIMARY KEY, fk_company_id INTEGER, ' + ", ".join(f'"{format_column_name(key)}" TEXT' for key in json_data['A']['companyOfficers'][0].keys())
                employees_query = f"CREATE TABLE {format_column_name(employees_table_name)} ({employees_columns}, FOREIGN KEY (fk_company_id) REFERENCES {format_column_name(company_table_name)}(id))"
                print(employees_query)
                cur.execute(employees_query)
                print("Employees table creation initiated.")

                # Wait until the employees table is created
                while True:
                    cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{format_column_name(employees_table_name)}')")
                    table_exists = cur.fetchone()[0]
                    if table_exists:
                        print("Employees table created successfully.")
                        break
                    time.sleep(1)  # Wait for 1 second before checking again

                # Create table for financial data
                financials_table_name = input("Enter the financials table name: ")
                financials_columns = 'id SERIAL PRIMARY KEY, fk_company_id INTEGER, ' + ", ".join(f'"{format_column_name(key)}" TEXT' for key, value in json_data['A'].items() if key != 'companyOfficers' and key != '52WeekChange')
                financials_query = f"CREATE TABLE {format_column_name(financials_table_name)} ({financials_columns}, FOREIGN KEY (fk_company_id) REFERENCES {format_column_name(company_table_name)}(id))"
                print(financials_query)
                cur.execute(financials_query)
                print("Financials table creation initiated.")

                # Wait until the financials table is created
                while True:
                    cur.execute(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{format_column_name(financials_table_name)}')")
                    table_exists = cur.fetchone()[0]
                    if table_exists:
                        print("Financials table created successfully.")
                        break
                    time.sleep(1)  # Wait for 1 second before checking again

                print("Data inserted successfully.")
        except psycopg2.Error as e:
            print("Error:", e)
            logging.error(e)  # Log the error
        finally:
            conn.close()















# Fonction pour insérer les données depuis le JSON

def insert_data_from_json():
    table_name = input("Enter the table name: ")
    file_path = input("Enter the JSON file path: ")
    json_data = read_json_file(file_path)
    if json_data is None:
        return

    try:
        json_data = json.loads(json_data)
    except json.JSONDecodeError as e:
        print("Invalid JSON format:", e)
        return

    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                for key, value in json_data.items():
                    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
                        sql.Identifier(table_name),
                        sql.SQL(', ').join(sql.Identifier(name) for name in value.keys()),
                        sql.SQL(', ').join(sql.Literal(val) for val in value.values())
                    )
                    cur.execute(query)
                print("Data inserted successfully.")
        except psycopg2.Error as e:
            print("Error inserting data:", e)
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
                column_names = [desc[0] for desc in cur.description]
                
                df = pd.DataFrame(rows, columns=column_names)
                return df
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

