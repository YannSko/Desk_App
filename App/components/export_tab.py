import customtkinter as ctk
from tkinter import scrolledtext, filedialog, messagebox
import tkinter as tk
import logging
import threading
import time


logging.basicConfig(filename='app.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ExportAndLogViewer(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        self.pack(fill="both", expand=True)

        #  widget export log
        export_button = ctk.CTkButton(self, text="Export app.log", command=self.export_log)
        export_button.pack(pady=20)

        #  widget daffichage log
        self.log_display = scrolledtext.ScrolledText(self, wrap=tk.WORD, height=10)
        self.log_display.pack(padx=10, pady=10, fill="both", expand=True)

        #  thread  mise à jour log
        self.update_log_thread = threading.Thread(target=self.update_log, daemon=True)
        self.update_log_thread.start()

    def read_log(self):
        log_file_path = 'app.log'
        with open(log_file_path, 'r') as file:
            return file.read()

    def update_log(self):
        while True:
            try:
                current_content = self.read_log()
                self.log_display.delete(1.0, tk.END)  
                self.log_display.insert(tk.END, current_content)  
                self.log_display.see(tk.END)  
                time.sleep(1) 
            except Exception as e:
                logging.error("Erreur lors de la mise à jour du log: {}".format(e))


    def export_log(self):
        log_content = self.read_log()
        export_file_path = filedialog.asksaveasfilename(defaultextension=".log", filetypes=[("Log files", "*.log"), ("All files", "*.*")])
        if export_file_path:
            with open(export_file_path, 'w') as file:
                file.write(log_content)
            messagebox.showinfo("Success", "Log file exported successfully.")