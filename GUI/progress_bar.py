import tkinter as tk
from tkinter import ttk

class Prog_bar():
    def __init__(self, label):
        self.root = tk.Tk()
        self.root.wm_title("Progress")
        self.root.geometry("300x100")

        self.label = label
        tk.Label(self.root, text='Processing', font=16).pack()

        
        self.progress = ttk.Progressbar(self.root, orient = 'horizontal',length = 100, mode = 'determinate')
        self.progress.pack()
        
        tk.Label(self.root, text=self.label, font=14).pack() 
        #self.root.mainloop()

    def setp(self, val):
        self.progress['value'] += val
        self.root.update_idletasks()  

    def set_label(self, label):
        self.label = label

    def close_bar(self):
        self.root.destroy()