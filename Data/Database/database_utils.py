import psycopg2
from psycopg2 import sql
import re

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

def create_table_from_json(table_name, json_input):
    conn = connect_to_database()
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

def insert_nested_data(table_name, data):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                for item in data.get("data", []):
                    columns = ", ".join(map(sanitize_column_name, item.keys()))
                    placeholders = ", ".join(["%s"] * len(item))
                    values = tuple(item.values())
                    query = sql.SQL("INSERT INTO {} ({}) VALUES ({});").format(
                        sql.Identifier(table_name),
                        sql.SQL(columns),
                        sql.SQL(placeholders)
                    )
                    cur.execute(query, values)
                conn.commit()
                print("Data inserted successfully.")
        except Exception as error:
            print(error)
        finally:
            conn.close()

def get_data(table_name, limit=None):
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
def get_data_x(table_name):
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

                # Récupérer le nombre total de lignes dans la table
                cur.execute(sql.SQL("SELECT count(*) FROM public.{};").format(sql.Identifier(table_name)))
                total_rows = cur.fetchone()[0]

                # Demander à l'utilisateur le nombre total de données à récupérer
                total_data = int(input("Entrez le nombre total de données que vous souhaitez récupérer (0 pour tout récupérer) : "))
                if total_data <= 0 or total_data > total_rows:
                    total_data = total_rows

                # Demander à l'utilisateur l'ID de départ
                start_id = int(input("Entrez l'ID de départ (1 pour commencer) : "))
                if start_id <= 0:
                    start_id = 1

                # Demander à l'utilisateur l'ID de fin si le nombre total n'est pas 1
                if total_data != 1:
                    end_id = int(input("Entrez l'ID de fin (0 pour terminer) : "))
                    if end_id <= 0 or end_id > total_rows:
                        end_id = total_rows
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





def update_data(table_name, id, data):
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