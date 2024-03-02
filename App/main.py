import tkinter as tk
from customtkinter import set_appearance_mode, set_default_color_theme, CTk, CTkFrame, CTkButton
from components.file_select import FileSelection
from components.data_viz import DataVisualization
from components.export_tab import Export
from components.db_management import DBManagement
from App.components.settings.settings import Settings, SettingsManager
import customtkinter
set_appearance_mode("System")  # "light", "dark", or "system"
set_default_color_theme("blue")  # blue, dark-blue, green

class App(customtkinter.CTk):
    def __init__(self):
        super().__init__()

        # Initialize SettingsManager and load settings
        self.settings_manager = SettingsManager()
        

        self.title("CustomTkinter Data App")
        self.geometry("1200x700")

        self.tab_buttons_frame = customtkinter.CTkFrame(self, width=1200, height=50)
        self.tab_buttons_frame.pack(pady=10)

        self.init_tabs()

    def init_tabs(self):
        self.current_tab = None
        self.tabs = {
            "file_selection": FileSelection(self),
            "data_visualization": DataVisualization(self),
            "export": Export(self),
            "db_management": DBManagement(self),
            "settings": Settings(self, app=self),  # Pass self to settings for callback
        }

        for tab_name, tab_frame in self.tabs.items():
            button = customtkinter.CTkButton(self.tab_buttons_frame, text=tab_name.replace("_", " ").title(),
                               command=lambda f=tab_frame: self.raise_frame(f))
            button.pack(side="left", padx=10)

        self.raise_frame(self.tabs["file_selection"])

    def raise_frame(self, frame):
        if self.current_tab is not None:
            self.current_tab.pack_forget()  # Hide the current tab
        self.current_tab = frame
        frame.pack(fill="both", expand=True)
    
    def change_appearance_mode_event(self, new_appearance_mode: str):
        customtkinter.set_appearance_mode(new_appearance_mode)

    def change_scaling_event(self, new_scaling: str):
        new_scaling_float = int(new_scaling.replace("%", "")) / 100
        customtkinter.set_widget_scaling(new_scaling_float)

if __name__ == "__main__":
    app = App()
    app.mainloop()
