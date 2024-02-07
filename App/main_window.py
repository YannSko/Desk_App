import tkinter as tk
from ui_enhancements.ui_components import *

class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Data App")
        self.configure(background="grey")  # Définir le fond d'écran général
        self.create_ui()

    def create_ui(self):
        # Utilisation de la classe Components pour créer un menu
        custom_components = CustomComponents(self)
        menu = custom_components.create_menu()
        self.config(menu=menu)

        # Création d'un bouton avec une taille spécifique
        button1 = custom_components.create_button("Custom Button 1", width=20, height=2)
        button1.pack()

        # Création d'un bouton avec une couleur de fond différente
        button2 = custom_components.create_button("Custom Button 2", bg="blue")
        button2.pack()

        # Création d'une étiquette avec une police de caractères différente
        label = custom_components.create_label("Custom Label", font=("Arial", 14))
        label.pack(side=tk.BOTTOM, padx=10, pady=10)

         # Création d'un cadre avec un fond de couleur différente et positionnement
        frame = custom_components.create_frame(bg="green", width=200, height=100)
        frame.pack(side=tk.BOTTOM, padx=10, pady=10)  # Positionné en bas avec des marges


if __name__ == "__main__":
    app = MainWindow()
    app.mainloop()
