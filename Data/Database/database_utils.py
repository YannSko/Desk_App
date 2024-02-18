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

# Function to create table dynamically from JSON keys
def create_table(table_name, json_input):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                # Sanitize and validate column names
                columns = set()
                for key, value in json_input.items():
                    if isinstance(value, dict):
                        for inner_key in value.keys():
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



def insert_data(table_name, **kwargs):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                columns = ", ".join(kwargs.keys())
                placeholders = ", ".join(["%s"] * len(kwargs))
                values = tuple(kwargs.values())
                cur.execute("INSERT INTO {} ({}) VALUES ({});".format(table_name, columns, placeholders), values)
                conn.commit()
        except Exception as error:
            print(error)
        finally:
            conn.close()

def get_data_x(table_name, x=None, columns=None):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                if columns:
                    cur.execute("SELECT {} FROM {} LIMIT %s;".format(", ".join(columns), table_name), (x,))
                else:
                    if x:
                        cur.execute("SELECT * FROM {} LIMIT %s;".format(table_name), (x,))
                    else:
                        cur.execute("SELECT * FROM {};".format(table_name))
                rows = cur.fetchall()
                return rows
        except Exception as error:
            print(error)
        finally:
            conn.close()

def update_data_x(table_name, x=None, id=None, **kwargs):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                if id and kwargs:
                    set_values = ", ".join(["{} = %s".format(key) for key in kwargs.keys()])
                    values = tuple(kwargs.values()) + (id,)
                    if x:
                        cur.execute("UPDATE {} SET {} WHERE id = %s LIMIT %s;".format(table_name, set_values), values + (x,))
                    else:
                        cur.execute("UPDATE {} SET {} WHERE id = %s;".format(table_name, set_values), values)
                    conn.commit()
                else:
                    print("Missing parameters.")
        except Exception as error:
            print(error)
        finally:
            conn.close()


def delete_data_x(x=None, id=None):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                if id:
                    if x:
                        cur.execute("DELETE FROM test_table WHERE id = %s LIMIT %s;", (id, x))
                    else:
                        cur.execute("DELETE FROM test_table WHERE id = %s;", (id,))
                    conn.commit()
                else:
                    print("Missing parameters.")
        except Exception as error:
            print(error)
        finally:
            conn.close()

def aggregate_data(function, column, x=None):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                if x:
                    cur.execute(f"SELECT {function}({column}) FROM test_table LIMIT %s;", (x,))
                else:
                    cur.execute(f"SELECT {function}({column}) FROM test_table;")
                result = cur.fetchone()[0]
                return result
        except Exception as error:
            print(error)
        finally:
            conn.close()


def drop_table(table_name):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS {};".format(table_name))
                conn.commit()
                print("Table {} supprimée avec succès.".format(table_name))
        except Exception as error:
            print("Erreur lors de la suppression de la table {} :".format(table_name), error)
        finally:
            conn.close()
