import customtkinter as ctk
import pandas as pd
import matplotlib.pyplot as plt
from tkinter import messagebox
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from psycopg2.sql import SQL, Identifier
from Data.Database.database_utils import *
from Data.Database.data_process import infer_data_types, convert_columns
from tkinter import simpledialog

class DataVisualization(ctk.CTkFrame):
    
    def __init__(self, parent):
        super().__init__(parent)
        self.pack(fill="both", expand=True)

        # Create a frame for the controls on the left
        self.controls_frame = ctk.CTkFrame(self)
        self.controls_frame.pack(side="left", fill="y")

        # Create a frame for the canvas on the right
        self.canvas_frame = ctk.CTkFrame(self)
        self.canvas_frame.pack(side="right", fill="both", expand=True)
        
        self.limit_entry = ctk.CTkEntry(self.controls_frame, placeholder_text="Enter limit (e.g., 100)")
        self.limit_entry.pack(pady=5)

        self.limit_direction = ctk.CTkOptionMenu(self.controls_frame, values=["Top", "Bottom"], command=None)
        self.limit_direction.pack(pady=5)

        self.aggregation_dropdown = ctk.CTkOptionMenu(self.controls_frame, values=["None", "Mean", "Median"], command=None)
        self.aggregation_dropdown.pack(pady=5)

        # Dropdown to select table from database
        self.table_dropdown = ctk.CTkOptionMenu(self.controls_frame, values=[], command=self.on_table_select)
        self.table_dropdown.pack(pady=20)
        
        # Dropdowns to select X and Y axes
        self.axis_x_dropdown = ctk.CTkOptionMenu(self.controls_frame, values=[])
        self.axis_y_dropdown = ctk.CTkOptionMenu(self.controls_frame, values=[])
        self.axis_x_dropdown.pack(pady=5)
        self.axis_y_dropdown.pack(pady=5)

        # Dropdown to select plot type
        self.plot_type_dropdown = ctk.CTkOptionMenu(self.controls_frame, values=["Line", "Scatter", "Bar"])
        self.plot_type_dropdown.pack(pady=5)

        # Button to load data from database and plot
        self.load_data_button = ctk.CTkButton(self.controls_frame, text="Load Data and Plot", command=self.load_data_and_plot)
        self.load_data_button.pack(pady=5)

        # Canvas for matplotlib graph
        self.fig, self.ax = plt.subplots()
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.canvas_frame)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack(fill="both", expand=True)

        # Fetch tables from the database initially
        self.fetch_tables()

    def fetch_tables(self):
        conn = connect_to_database()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                    tables = [row[0] for row in cur.fetchall()]
                    self.update_dropdown(self.table_dropdown, tables)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to fetch tables: {e}")
            finally:
                conn.close()


    def update_dropdown(self, dropdown, values):
        dropdown.configure(values=values)
        if values:
            dropdown.set(values[0])
        else:
            dropdown.set('')

    def on_table_select(self, event=None):
        selected_table = self.table_dropdown.get()
        self.df = self.get_data(selected_table)
        if self.df is not None:
            # Infer data types and convert columns
            inferred_types = infer_data_types(self.df)
            self.df = convert_columns(self.df, inferred_types)
            columns = self.df.columns.tolist()
            self.update_dropdown(self.axis_x_dropdown, columns)
            self.update_dropdown(self.axis_y_dropdown, columns)


    def get_data(self, table_name):
        # This method fetches all the data from the selected table
        conn = connect_to_database()
        if conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(SQL("SELECT * FROM {}").format(Identifier(table_name)))
                    rows = cur.fetchall()
                    column_names = [desc[0] for desc in cur.description]
                    return pd.DataFrame(rows, columns=column_names)
            except Exception as e:
                messagebox.showerror("Error", f"Failed to fetch data: {e}")
                return None
            finally:
                conn.close()

    def load_data_and_plot(self):
        # This method is called when the 'Load Data and Plot' button is clicked
        x_col = self.axis_x_dropdown.get()
        y_col = self.axis_y_dropdown.get()
        plot_type = self.plot_type_dropdown.get()

        if not x_col or not y_col or not plot_type:
            messagebox.showinfo("Info", "Please select both X and Y columns and a plot type.")
            return

        if self.df is not None and not self.df.empty:
            limit = self.limit_entry.get()
            if limit:
                try:
                    limit = int(limit)
                    if self.limit_direction.get() == "Top":
                        self.df = self.df.head(limit)
                    elif self.limit_direction.get() == "Bottom":
                        self.df = self.df.tail(limit)
                except ValueError:
                    messagebox.showerror("Error", "Limit must be an integer")
                    return
            
            aggregation_method = self.aggregation_dropdown.get()
            if aggregation_method in ["Mean", "Median"]:
                if aggregation_method == "Mean":
                    self.df[y_col] = self.df[y_col].mean()
                elif aggregation_method == "Median":
                    self.df[y_col] = self.df[y_col].median()
            
            self.plot_data(x_col, y_col, plot_type)
        else:
            messagebox.showinfo("Info", "No data loaded.")

    def plot_data(self, x_col, y_col, plot_type):
        # This method plots the data on the canvas
        self.ax.clear()
        if plot_type == "Line":
            self.ax.plot(self.df[x_col], self.df[y_col], marker='o')
        elif plot_type == "Scatter":
            self.ax.scatter(self.df[x_col], self.df[y_col])
        elif plot_type == "Bar":
            self.ax.bar(self.df[x_col], self.df[y_col])
            plt.setp(self.ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

        self.ax.set_xlabel(x_col)
        self.ax.set_ylabel(y_col)
        self.ax.set_title(f"{plot_type} Plot: {y_col} vs {x_col}")
        self.canvas.draw()
    

# Ensure you replace 'connect_to_database' with your actual database connection function.
