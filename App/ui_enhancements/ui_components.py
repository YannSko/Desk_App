import tkinter as tk

class Components:
    def __init__(self, parent, font=("Helvetica", 12), bg_color="white", fg_color="black", width=10, height=1):
        self.parent = parent
        self.font = font
        self.bg_color = bg_color
        self.fg_color = fg_color
        self.width = width
        self.height = height     # Hauteur par défaut

    def set_font(self, font):
        self.font = font

    def set_bg_color(self, color):
        self.bg_color = color

    def set_fg_color(self, color):
        self.fg_color = color

    def set_size(self, width, height):
        self.width = width
        self.height = height

class CustomComponents(Components):
    def __init__(self, parent):
        super().__init__(parent)


    def create_button(self, text, command=None, **kwargs):
        button = tk.Button(self.parent, text=text, command=command, **kwargs)
        return button

    def create_label(self, text, **kwargs):
        label = tk.Label(self.parent, text=text,  **kwargs)
        return label

    def create_entry(self, **kwargs):
        entry = tk.Entry(self.parent, **kwargs)
        return entry

    def create_text(self, **kwargs):
        text_widget = tk.Text(self.parent,  **kwargs)
        return text_widget

    def create_frame(self, **kwargs):
        frame = tk.Frame(self.parent, **kwargs)
        return frame

    def create_canvas(self, **kwargs):
        canvas = tk.Canvas(self.parent,  **kwargs)
        return canvas
    
    def create_menu(self, **kwargs):
        menu = tk.Menu(self.parent, **kwargs)
        return menu

    def create_checkbutton(self, text, **kwargs):
        checkbutton = tk.Checkbutton(self.parent, text=text,  **kwargs)
        return checkbutton

    def create_radiobutton(self, text, **kwargs):
        radiobutton = tk.Radiobutton(self.parent, text=text,  **kwargs)
        return radiobutton

    def create_listbox(self, **kwargs):
        listbox = tk.Listbox(self.parent, **kwargs)
        return listbox

    def create_menu(self, **kwargs):
        menu = tk.Menu(self.parent,  **kwargs)
        return menu
    
    def create_scale(self, **kwargs):
        scale = tk.Scale(self.parent,  **kwargs)
        return scale

    def create_spinbox(self, **kwargs):
        spinbox = tk.Spinbox(self.parent,  **kwargs)
        return spinbox

    def create_combobox(self, values, **kwargs):
        combobox = tk.Combobox(self.parent, values=values,  **kwargs)
        return combobox

    def create_message(self, **kwargs):
        message = tk.Message(self.parent,  **kwargs)
        return message

    def create_progressbar(self, **kwargs):
        progressbar = tk.Progressbar(self.parent,  **kwargs)
        return progressbar

    def create_scrollbar(self, **kwargs):
        scrollbar = tk.Scrollbar(self.parent,  **kwargs)
        return scrollbar

    def create_separator(self, **kwargs):
        separator = tk.Separator(self.parent,**kwargs)
        return separator
    
    def create_menu_button(self, text, **kwargs):
        menu_button = tk.Menubutton(self.parent, text=text, **kwargs)
        return menu_button

    def create_label_frame(self, text, **kwargs):
        label_frame = tk.LabelFrame(self.parent, text=text,  **kwargs)
        return label_frame

    def create_message_box(self, **kwargs):
        message_box = tk.Message(self.parent,**kwargs)
        return message_box

    def create_option_menu(self, variable, default, *values, **kwargs):
        option_menu = tk.OptionMenu(self.parent, variable, default, *values, **kwargs)
        return option_menu

    def create_paned_window(self, **kwargs):
        paned_window = tk.PanedWindow(self.parent, **kwargs)
        return paned_window

    def create_text_widget(self, **kwargs):
        text_widget = tk.Text(self.parent,**kwargs)
        return text_widget

    def create_treeview(self, **kwargs):
        treeview = tk.Treeview(self.parent,**kwargs)
        return treeview
    
    def create_combobox(self, values, **kwargs):
        combobox = tk.Combobox(self.parent, values=values, **kwargs)
        return combobox

    def create_message(self, **kwargs):
        message = tk.Message(self.parent,**kwargs)
        return message
