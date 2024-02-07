import tkinter as tk

class StatusBar(tk.Label):
    def __init__(self, parent):
        super().__init__(parent, text="Ready", bd=1, relief=tk.SUNKEN, anchor=tk.W)
        self.pack(side=tk.BOTTOM, fill=tk.X)

    def set_status(self, text):
        self.config(text=text)
