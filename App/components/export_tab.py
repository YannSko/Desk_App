import customtkinter as ctk

class Export(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        label = ctk.CTkLabel(self, text="Export Data")
        label.pack(pady=20)
        
        # Ajoutez ici votre logique pour l'exportation des données
