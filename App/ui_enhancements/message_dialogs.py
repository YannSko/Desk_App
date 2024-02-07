import tkinter.messagebox as messagebox

def show_success_message(message):
    messagebox.showinfo("Success", message)

def show_error_message(message):
    messagebox.showerror("Error", message)
