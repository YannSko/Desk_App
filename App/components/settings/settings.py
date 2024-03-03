import customtkinter as ctk
import json
import os
import psycopg2
import queue
from  Data.Database.test_opti import run_backup_process, restore_backup
from tkinter import  messagebox

class SettingsManager:
    def __init__(self, filepath="settings.json"):
        self.filepath = filepath
        self.settings = self.load_settings()

    def load_settings(self):
        if os.path.exists(self.filepath):
            with open(self.filepath, 'r') as file:
                return json.load(file)
        else:
            # Default settings
            return {
                "font_size": 12, 
                "color_theme": "blue", 
                "appearance_mode": "System",  # Default appearance mode
                "ui_scaling": "100%"  # Default UI scaling
            }

    def save_settings(self, settings):
        with open(self.filepath, 'w') as file:
            json.dump(settings, file, indent=4)
        self.settings = settings

    def get_setting(self, key):
        return self.settings.get(key)

    def update_setting(self, key, value):
        self.settings[key] = value
        self.save_settings(self.settings)

class Settings(ctk.CTkFrame):
    def __init__(self, parent, app):
        super().__init__(parent)
        self.app = app  # Reference to the main app to call update method
        self.settings_manager = SettingsManager()
        
        ctk.CTkLabel(self, text="Settings & Customization").pack(pady=20)
        
        # Sidebar configuration omitted for brevity
        
        # Additional UI elements for appearance mode and UI scaling
        self.appearance_mode_label = ctk.CTkLabel(self, text="Appearance Mode:", anchor="w")
        self.appearance_mode_label.pack(padx=20, pady=(10, 0))
        self.appearance_mode_optionmenu = ctk.CTkOptionMenu(self, values=["Light", "Dark", "System"],
                                                            command=self.change_appearance_mode_event)
        self.appearance_mode_optionmenu.pack(padx=20, pady=(10, 10))
        
        self.scaling_label = ctk.CTkLabel(self, text="UI Scaling:", anchor="w")
        self.scaling_label.pack(padx=20, pady=(10, 0))
        self.scaling_optionmenu = ctk.CTkOptionMenu(self, values=["80%", "90%", "100%", "110%", "120%"],
                                                    command=self.change_scaling_event)
        self.scaling_optionmenu.pack(padx=20, pady=(10, 20))

        # Backup and Restore Buttons
        self.backup_button = ctk.CTkButton(self, text="Backup", command=self.backup_database)
        self.backup_button.pack(pady=10)
        
        self.restore_button = ctk.CTkButton(self, text="Restore", command=self.restore_database)
        self.restore_button.pack(pady=10)

        # Load and apply existing settings
        self.load_and_apply_settings()

    def change_appearance_mode_event(self, new_appearance_mode: str):
        ctk.set_appearance_mode(new_appearance_mode)
        self.settings_manager.update_setting("appearance_mode", new_appearance_mode)

    def change_scaling_event(self, new_scaling: str):
        new_scaling_float = int(new_scaling.replace("%", "")) / 100
        ctk.set_widget_scaling(new_scaling_float)
        self.settings_manager.update_setting("ui_scaling", new_scaling)

    def apply_settings(self):
    # Load settings from the settings manager
        appearance_mode = self.settings_manager.get_setting("appearance_mode")
        ui_scaling = self.settings_manager.get_setting("ui_scaling")
        
        # Ensure appearance_mode is not None and is a valid value before applying
        if appearance_mode in ["Light", "Dark", "System"]:
            self.change_appearance_mode_event(appearance_mode)
        else:
            # Log an error or set to a default value if appearance_mode is invalid
            print("Invalid appearance mode in settings:", appearance_mode)
            self.change_appearance_mode_event("System")  # Default to system mode
        
        # Similarly, ensure UI scaling is valid before applying
        if ui_scaling and "%" in ui_scaling:
            self.change_scaling_event(ui_scaling)
        else:
            # Log an error or set to a default value if ui_scaling is invalid
            print("Invalid UI scaling in settings:", ui_scaling)
            self.change_scaling_event("100%")  # Default to 100%



    def load_and_apply_settings(self):
        # Load settings from the settings manager
        self.settings_manager.load_settings()
        # Apply settings
        self.apply_settings()
    def backup_database(self):
        # Trigger the backup process
        # Informations de connexion pour la base de données source
        HOST_SRC = 'localhost'
        PORT_SRC = 5432
        USER_SRC = 'postgres'
        PASSWORD_SRC = 'Yann'
        DATABASE_SRC = 'First_test'
        BACKUP_DIR_SRC = r'C:\Users\yskon\Desktop\Desk_App\Data\Database\back_up'

        # Informations de connexion pour la base de données de sauvegarde
        HOST_DEST = 'localhost'
        PORT_DEST = 5432
        USER_DEST = 'postgres'
        PASSWORD_DEST = 'Yann'
        DATABASE_DEST = 'back_up'

        # Connexion à la base de données source
        conn_src = psycopg2.connect(
            dbname=DATABASE_SRC,
            user=USER_SRC,
            password=PASSWORD_SRC,
            host=HOST_SRC,
            port=PORT_SRC
        )

        # Connexion à la base de données de sauvegarde
        conn_dest = psycopg2.connect(
            dbname=DATABASE_DEST,
            user=USER_DEST,
            password=PASSWORD_DEST,
            host=HOST_DEST,
            port=PORT_DEST
        )

        # Create a queue
        q = queue.Queue()
        run_backup_process(conn_src, BACKUP_DIR_SRC, conn_dest, q)
        messagebox.showinfo("Backup", "Database backup completed successfully.")

    def restore_database(self):
        # Trigger the restore process
        # Informations de connexion pour la base de données source
        # Informations de connexion pour la base de données source
        HOST_SRC = 'localhost'
        PORT_SRC = 5432
        USER_SRC = 'postgres'
        PASSWORD_SRC = 'Yann'
        DATABASE_SRC = 'First_test'
        BACKUP_DIR_SRC = r'C:\Users\yskon\Desktop\Desk_App\Data\Database\back_up'

        # Informations de connexion pour la base de données de sauvegarde
        HOST_DEST = 'localhost'
        PORT_DEST = 5432
        USER_DEST = 'postgres'
        PASSWORD_DEST = 'Yann'
        DATABASE_DEST = 'back_up'

        # Connexion à la base de données source
        conn_src = psycopg2.connect(
            dbname=DATABASE_SRC,
            user=USER_SRC,
            password=PASSWORD_SRC,
            host=HOST_SRC,
            port=PORT_SRC
        )

        # Connexion à la base de données de sauvegarde
        conn_dest = psycopg2.connect(
            dbname=DATABASE_DEST,
            user=USER_DEST,
            password=PASSWORD_DEST,
            host=HOST_DEST,
            port=PORT_DEST
        )

        # Create a queue
        q = queue.Queue()
        restore_backup(conn_src, BACKUP_DIR_SRC)  
        messagebox.showinfo("Restore", "Database restored successfully.")

