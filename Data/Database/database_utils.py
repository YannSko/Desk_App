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