import customtkinter as ctk
from Data.Database.database_utils import *
from tkinter import messagebox, simpledialog
from customtkinter import CTk, CTkFrame, CTkButton, CTkLabel, CTkEntry
from tkinter import simpledialog, messagebox
import psycopg2
from psycopg2.sql import SQL, Identifier
from psycopg2 import sql


class DBManagement(ctk.CTkFrame):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        
        # Button to connect to the database
        self.btn_connect = ctk.CTkButton(self, text="Connect to Database", command=self.connect_to_database)
        self.btn_connect.pack(pady=10)
        # Button to load and process JSON
        self.btn_load_json = ctk.CTkButton(self, text="Load and Process JSON", command=self. insert_data_ui)
        self.btn_load_json.pack(pady=5)
        # Button to load and process other
        
        self.btn_load_json = ctk.CTkButton(self, text="complex_data json", command=self. insert_data_complex)
        self.btn_load_json.pack(pady=5)
        
        self.btn_load_json = ctk.CTkButton(self, text="Load and Process other(csv,xlsx,xml)", command=self. insert_data_other)
        self.btn_load_json.pack(pady=5)
        

        # Button to create a table
        self.btn_create_table = ctk.CTkButton(self, text="Read Table", command=self.read_table_ui)
        self.btn_create_table.pack(pady=10)
        # Text widget to display table data
        self.text = tk.Text(self, wrap=tk.WORD, height=10, width=50)
        self.text.pack(pady=10)



        # Button to create a table
        self.btn_create_table = ctk.CTkButton(self, text="Delete Table", command=self.delete_data)
        self.btn_create_table.pack(pady=10)

        self.btn_export_data = ctk.CTkButton(self, text="Export Data", command=self.export_data_ui)
        self.btn_export_data.pack(pady=10)

        # Status label
        self.label_status = ctk.CTkLabel(self, text="Status: Not connected")
        self.label_status.pack(pady=10)
        
    def connect_to_database(self):
        # Here, you'll need to implement the actual logic to connect to your database.
        # For demonstration, let's assume you have a function `connect_to_database` that returns True if connection is successful.
        success = connect_to_database()
        if success:
            self.label_status.configure(text="Status: Connected")
            messagebox.showinfo("Success", "Connected to the database successfully.")
        else:
            self.label_status.configure(text="Status: Connection Failed")
            messagebox.showerror("Error", "Failed to connect to the database.")
  

  

    def create_table_ui(self):
        # This method should open a dialog to select a JSON file and then create a table based on its contents
        json_file_path = filedialog.askopenfilename(filetypes=[("JSON files", "*.json")])
        if json_file_path:
            with open(json_file_path, 'r') as file:
                data = json.load(file)
            # Assuming you have a function to create a table from a DataFrame or directly from JSON data
            table_name = simpledialog.askstring("Table Name", "Enter the table name:")
            if table_name:
                create_table_from_df(data, table_name, self.conn)  # Adapt this call to match your actual function
                messagebox.showinfo("Success", f"Table '{table_name}' created successfully.")
    

    ###################################INSERT DATA
    def insert_data_ui(self):
        

        try:
            df_to_sql_j()
            messagebox.showinfo("Success", "Data inserted successfully.")
        

        except:
            messagebox.showerror("Error", "All data insertion methods failed. Check the console for more details.")
    
    def insert_data_complex(self):
        

        try:
            df_to_sql_big()
            messagebox.showinfo("Success", "Data inserted successfully.")
        

        except:
            messagebox.showerror("Error", "All data insertion methods failed. Check the console for more details.")
    
   
    def insert_data_other(self):
        

        try:
            df_to_sql_cxe()
            messagebox.showinfo("Success", "Data inserted successfully.")
            

        except:
            messagebox.showerror("Error", "All data insertion methods failed. Check the console for more details.")
       

            
            
    
    ##########################################GET DATA
    def read_table_ui(self):
        table_name = simpledialog.askstring("Input", "Enter the table name:")
        if table_name:
            df = self.get_data(table_name)
            if df is not None:
                self.display_data_as_text(df)
            else:
                messagebox.showinfo("Info", f"No data found in table '{table_name}'.")

    def get_data(self, table_name):
        conn = connect_to_database()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("SELECT * FROM public.{};").format(sql.Identifier(table_name)))
                    rows = cur.fetchall()
                    column_names = [desc[0] for desc in cur.description]
                    df = pd.DataFrame(rows, columns=column_names)
                    return df
            except Exception as e:
                messagebox.showerror("Error", f"Failed to fetch data: {e}")
            finally:
                conn.close()
        else: 
            messagebox.showinfo("no_connection")
        

    def display_data_as_text(self, df):
        # Clear previous data
        self.text.delete('1.0', tk.END)
        # Display DataFrame as text
        text_to_display = df.to_string(index=False)
        self.text.insert(tk.END, text_to_display)
    

    ##### delete
    
    def delete_data(self):
        options = ["Delete specific tables", "Delete specific columns from a table", "Delete specific rows from a table"]
        choice = simpledialog.askstring("Delete Data", "Choose option:\n1. " + "\n2. ".join(options))
        
        if choice == '1':
            self.delete_tables()
        elif choice == '2':
            self.delete_columns()
        elif choice == '3':
            self.delete_rows()
        else:
            messagebox.showerror("Error", "Invalid choice.")

    def delete_tables(self):
        conn = connect_to_database()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                    tables = [row[0] for row in cur.fetchall()]
                    table_names = simpledialog.askstring("Delete Tables", "Available tables: " + ", ".join(tables) +
                                                         "\nEnter table names to delete (comma separated):").split(',')
                    for table_name in table_names:
                        if table_name.strip() in tables:
                            cur.execute(SQL("DROP TABLE IF EXISTS {}").format(Identifier(table_name.strip())))
                            conn.commit()
                            messagebox.showinfo("Success", f"Table {table_name.strip()} deleted successfully.")
                        else:
                            messagebox.showerror("Error", f"Table {table_name.strip()} does not exist.")
            except Exception as e:
                messagebox.showerror("Error", str(e))
            finally:
                conn.close()

    def delete_columns(self):
        conn = connect_to_database()
        if conn:
            try:
                with conn.cursor() as cur:
                    # Fetch and display available tables first
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                    tables = [row[0] for row in cur.fetchall()]
                    if not tables:
                        messagebox.showinfo("Info", "No tables found in the database.")
                        return
                    
                    table_name = simpledialog.askstring("Delete Columns", "Available tables: " + ", ".join(tables) + "\nEnter the table name:")
                    if table_name not in tables:
                        messagebox.showerror("Error", "The specified table does not exist.")
                        return

                    # Fetch and display available columns for the chosen table
                    cur.execute(sql.SQL("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = {}").format(sql.Literal(table_name)))
                    columns = [row[0] for row in cur.fetchall()]
                    if not columns:
                        messagebox.showerror("Error", f"No columns found for table {table_name}.")
                        return
                    
                    column_names_input = simpledialog.askstring("Delete Columns", "Available columns in " + table_name + ": " + ", ".join(columns) +
                                                                "\nEnter column names to delete (comma separated):")
                    if not column_names_input:
                        messagebox.showinfo("Info", "No columns specified for deletion.")
                        return
                    
                    column_names = column_names_input.split(',')
                    valid_column_names = [column_name.strip() for column_name in column_names if column_name.strip() in columns]
                    
                    if not valid_column_names:
                        messagebox.showerror("Error", "None of the specified columns exist in the table.")
                        return
                    
                    # Execute deletion for each valid column name
                    for column_name in valid_column_names:
                        cur.execute(sql.SQL("ALTER TABLE {} DROP COLUMN IF EXISTS {} CASCADE").format(sql.Identifier(table_name), sql.Identifier(column_name)))
                    conn.commit()
                    messagebox.showinfo("Success", f"Columns deleted successfully from table {table_name}.")
                    
            except Exception as e:
                messagebox.showerror("Error", str(e))
            finally:
                conn.close()




    def delete_rows(self):
        conn = connect_to_database()
        if conn:
            try:
                with conn.cursor() as cur:
                    # Fetch and display available tables
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                    tables = [row[0] for row in cur.fetchall()]
                    if not tables:
                        messagebox.showinfo("Info", "No tables found in the database.")
                        return
                    table_name = simpledialog.askstring("Delete Rows", "Available tables: " + ", ".join(tables) + "\nEnter the table name:")
                    if table_name not in tables:
                        messagebox.showerror("Error", "The specified table does not exist.")
                        return

                    # Fetch the total number of rows before deletion
                    cur.execute(SQL("SELECT COUNT(*) FROM {}").format(Identifier(table_name)))
                    total_rows = cur.fetchone()[0]
                    messagebox.showinfo("Info", f"Total rows in table {table_name}: {total_rows}")

                    # Ask for IDs to delete
                    ids_to_delete = simpledialog.askstring("Delete Rows", "Enter the IDs of rows to delete (comma separated):").split(',')
                    if not ids_to_delete:
                        messagebox.showinfo("Info", "No IDs specified for deletion.")
                        return

                    # Create a SQL condition for deletion
                    condition = SQL("id IN (") + SQL(",").join([SQL("%s") for _ in ids_to_delete]) + SQL(")")
                    cur.execute(SQL("DELETE FROM {} WHERE ").format(Identifier(table_name)) + condition, tuple(ids_to_delete))
                    conn.commit()
                    messagebox.showinfo("Success", f"Rows deleted successfully from table {table_name}.")
            except Exception as e:
                messagebox.showerror("Error", str(e))
            finally:
                conn.close()

    ####export csv
                
     # Method for exporting data UI interaction
    def export_data_ui(self):
    # Fetch and display available tables
        conn = connect_to_database()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                    tables = [row[0] for row in cur.fetchall()]
                    if not tables:
                        messagebox.showinfo("Info", "No tables found in the database.")
                        return
            except Exception as e:
                messagebox.showerror("Error", str(e))
                return
            finally:
                conn.close()

            # Create a dropdown menu to select the table
            table_name_dropdown = ctk.CTkOptionMenu(self, values=tables)
            table_name_dropdown.pack(pady=20)
            
            # Create an export button
            export_button = ctk.CTkButton(self, text="Export Selected Table", command=lambda: self.on_table_select(table_name_dropdown.get()))
            export_button.pack(pady=10)

    def on_table_select(self, selected_table):
        if selected_table:
            self.export_data(selected_table)

    # Method to perform the export operation
    def export_data(self, table_name):
        # Get the file location for saving the CSV file
        file_path = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
        if not file_path:  # If no file is selected, return early
            messagebox.showinfo("Export Data", "Export cancelled.")
            return

        conn = connect_to_database()
        if conn:
            try:
                # Build the query string using psycopg2.sql
                query = SQL("SELECT * FROM {}").format(Identifier(table_name)).as_string(conn)
                # Use the query string with pandas.read_sql
                df = pd.read_sql(query, conn)
                df.to_csv(file_path, index=False)
                messagebox.showinfo("Export Data", f"Data from table '{table_name}' has been exported successfully to {file_path}.")
            except Exception as e:
                messagebox.showerror("Export Data", f"Failed to export data: {e}")
            finally:
                conn.close()



    

   

