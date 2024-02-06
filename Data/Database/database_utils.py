import psycopg2

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

def create_table():
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS test_table (id SERIAL PRIMARY KEY, name VARCHAR, age INTEGER);")
                conn.commit()
        except Exception as error:
            print(error)
        finally:
            conn.close()

def insert_data(name, age):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO test_table (name, age) VALUES (%s, %s);", (name, age))
                conn.commit()
        except Exception as error:
            print(error)
        finally:
            conn.close()

def get_data_x(x=None, columns=None):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                if columns:
                    cur.execute("SELECT {} FROM test_table LIMIT %s;".format(", ".join(columns)), (x,))
                else:
                    if x:
                        cur.execute("SELECT * FROM test_table LIMIT %s;", (x,))
                    else:
                        cur.execute("SELECT * FROM test_table;")
                rows = cur.fetchall()
                return rows
        except Exception as error:
            print(error)
        finally:
            conn.close()

def update_data_x(x=None, id=None, new_name=None, new_age=None):
    conn = connect_to_database()
    if conn:
        try:
            with conn.cursor() as cur:
                if id and new_name is not None and new_age is not None:
                    if x:
                        cur.execute("UPDATE test_table SET name = %s, age = %s WHERE id = %s LIMIT %s;", (new_name, new_age, id, x))
                    else:
                        cur.execute("UPDATE test_table SET name = %s, age = %s WHERE id = %s;", (new_name, new_age, id))
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
