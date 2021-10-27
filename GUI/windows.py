import tkinter as tk
from aux_func import *
from configs import *


def openGraphWindow(values):    
    graph = tk.Tk() 
    min_plot_val = get_min_plot_nr()
    def leave_graph():
        graph.destroy()

    graph.wm_title(get_graph_title())  
    graph.geometry(get_graph_geo())

    
    x,y = remove_vals(values, min_plot_val)
    print(x,y)
    a1.bar(x, y, 0.35)
    

    fig1.autofmt_xdate()
    
    canvas = FigureCanvasTkAgg(fig1, graph)
    canvas.draw()

    toolbar = NavigationToolbar2Tk(canvas, graph)
    toolbar.update()
    canvas._tkcanvas.pack(side = tk.TOP, fill = tk.BOTH, expand = True)
 
    graph.mainloop()

def open_settings_window():
    setting_window = tk.TK()