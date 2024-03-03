import tkinter as tk
from tkinter import filedialog, messagebox
import customtkinter as ctk
import pandas as pd
import logging
logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class FileSelection(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        self.load_file_button = ctk.CTkButton(self, text="Load File", command=self.load_file)
        self.load_file_button.pack(pady=20)

        self.file_content_textbox = ctk.CTkTextbox(self, width=400, height=200)
        self.file_content_textbox.pack(pady=10)

    def load_file(self):
        logging.info("Load_file Begin")
        file_path = filedialog.askopenfilename(title="Open File", filetypes=[("All Files", "*.*")])
        if file_path:
            try:
                df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path) if file_path.endswith(('.xlsx', '.xls')) else pd.read_xml(file_path) if file_path.endswith('.xml') else pd.read_json(file_path)
                self.file_content_textbox.delete("1.0", "end")
                self.file_content_textbox.insert("1.0", df.to_string())
                logging.info("file loaded")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to load file: {e}")
                logging.error("failed to load")
