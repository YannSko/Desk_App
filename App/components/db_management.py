import customtkinter as ctk

class DBManagement(ctk.CTkFrame):
    def __init__(self, parent):
        super().__init__(parent)
        label = ctk.CTkLabel(self, text="Database Management")
        label.pack(pady=20)
        
        # Ajoutez ici votre logique pour la gestion de la base de données
