import customtkinter as ctk
import pandas as pd
import matplotlib.pyplot as plt
from tkinter import filedialog
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

class DataVisualization(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        self.pack(fill="both", expand=True)

        # Bouton pour charger les données
        self.load_data_button = ctk.CTkButton(self, text="Load Data", command=self.load_data)
        self.load_data_button.pack(pady=20)

        # Emplacement pour le graphique matplotlib
        self.fig, self.ax = plt.subplots()
        self.canvas = FigureCanvasTkAgg(self.fig, master=self)
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack(fill="both", expand=True)

    def load_data(self):
        file_path = filedialog.askopenfilename(title="Open Data File", filetypes=[("CSV Files", "*.csv"), ("Excel Files", "*.xlsx")])
        if file_path:
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith('.xlsx'):
                df = pd.read_excel(file_path)
            
            # Supposons que df a des colonnes 'A' et 'B' que nous voulons tracer
            # C'est ici que vous adapteriez le code pour gérer vos données spécifiques
            self.plot_data(df, 'A', 'B')

    def plot_data(self, df, x_col, y_col):
        self.ax.clear()  # Effacer les données précédentes
        self.ax.plot(df[x_col], df[y_col])
        self.ax.set_xlabel(x_col)
        self.ax.set_ylabel(y_col)
        self.ax.set_title(f"{y_col} vs {x_col}")
        self.canvas.draw()

