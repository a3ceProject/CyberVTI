from matplotlib.figure import Figure
import matplotlib.animation as animation
from matplotlib import style
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
import tkinter as tk
from tkinter import ttk
from api_requests import *
import tkinter.filedialog
from configs import *
from datetime import datetime, timedelta
from dialog_msg import *
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from aux_func import *
from settings_window import *
import numpy as np
import plotly
import plotly.graph_objs as go
from pandas.io.json import json_normalize
from tkinter import messagebox
import sys
from save_func import *
from progress_bar import *
from metrics_window import get_metrics


matplotlib.use("TKAgg")
style.use("ggplot")

FONT = (config.get('Style', 'font'), get_int_cfg('Style', 'font_size'))
FONT_LARGE = (config.get('Style', 'font'),
              get_int_cfg('Style', 'font_large_size'))
DATE_FORMAT = config.get('File_parms', 'date_format')


fig = Figure(figsize=(5, 5), dpi=70)
a = fig.add_subplot(111)

fig1 = Figure(figsize=(get_int_cfg('GraphWindow', 'fig_size'),
                       get_int_cfg('GraphWindow', 'fig_size')), dpi=get_int_cfg('GraphWindow', 'fig_dpi'))
a1 = fig1.add_subplot(111)


def restart_fig():
    global fig
    global a
    fig = Figure(figsize=(4, 4), dpi=50)
    a = fig.add_subplot(111)


class popupWindow(object):
    def __init__(self,master):
        top=self.top=tk.Toplevel(master)
        self.l=tk.Label(top,text="Username")
        self.l.pack()
        self.e=tk.Entry(top)
        self.e.pack()        
        self.b=tk.Button(top,text='Ok',command=self.cleanup)
        self.b.pack()

    def cleanup(self):
        self.value=self.e.get()        
        insert_user(self.value)                     
        self.top.destroy()
        

def openGraphWindow(values):
    graph = tk.Tk()
    min_plot_val = get_int_cfg('GraphWindow', 'min_plot_nr')

    def leave_graph():
        a1 = fig1.add_subplot(111)
        graph.destroy()

    graph.wm_title(config.get('GraphWindow', 'title'))
    graph.geometry(config.get('GraphWindow', 'geo'))

    x, y = remove_vals(values, min_plot_val)

    a1.bar(x, y, 0.35)

    if int(config.get('GraphWindow', 'log_scale')) == 1:
        a1.set_yscale('log')

    a1.set_title('Activity Graph')
    a1.set_xlabel('Date')
    a1.set_ylabel('Nº of activities')
    fig1.autofmt_xdate()

    canvas = FigureCanvasTkAgg(fig1, graph)
    canvas.draw()

    toolbar = NavigationToolbar2Tk(canvas, graph)
    toolbar.update()
    canvas._tkcanvas.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

    graph.mainloop()


def show_results(file_s):
    # Window
    root = tk.Tk()
    root.wm_title("Result Info")
    root.geometry("1000x750")


    lb_file = tk.Label(
        root, text=file_s, font=FONT_LARGE).grid(row=1, column=1)
    # std configs
    std = get_conf_std()

    def make_heatmap(file_s, conf_id, picked, view):

        heat = tk.Tk()
        heat.wm_title('HeatMap')
        heat.geometry(config.get('Heatmap', 'geo'))

        data_heatmap = get_heatmap_data(file_s, conf_id, picked, view)
       
        x = data_heatmap['columns']
        y = data_heatmap['index']
        z = np.array(data_heatmap['data'])
        fig2, ax = plt.subplots()

        im = ax.imshow(z, cmap=config.get(
            "Heatmap", "cmap"), interpolation='none')
        if int(config.get("Heatmap", "colorbar")) == 1:
            cbar = fig2.colorbar(im, ax=ax, location='right',
                                 anchor=(0, 0.5), shrink=0.7)
            cbar.ax.get_yaxis().labelpad = 15
            cbar.ax.set_ylabel('Normalised weights', rotation=270)
        if int(config.get("Heatmap", "notes")) == 1:
            for i in range(len(y)):
                for j in range(len(x)):
                    text = ax.text(j, i, round(
                        z[i, j], 2), ha="center", va="center", color="w")

        ax.set_xticks(np.arange(len(x)))
        ax.set_yticks(np.arange(len(y)))
        ax.set_xticklabels(x)
        ax.set_yticklabels(y)

        # Rotate the tick labels and set their alignment.
        plt.setp(ax.get_xticklabels(), rotation=45, ha="right",
                 rotation_mode="anchor")
        plt.grid(None)

        ax.set_title('Heatmap - ' + picked)
        ax.set_xlabel('Features')
        ax.set_ylabel('Cluster Number')

        fig2.tight_layout()

        canvas = FigureCanvasTkAgg(fig2, heat)
        canvas.get_tk_widget().pack()

        toolbar = NavigationToolbar2Tk(canvas, heat)
        toolbar.update()
        canvas._tkcanvas.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        heat.mainloop()

    # get list of clusters timestamp
    def list_clusters(conf_id, tw, view, n_max):
        #time_lbox.delete(0, 'end')

        results = get_result_clusters(file_s, conf_id, view, int(n_max))
        timestamps = []
        clusters_lst = []
        for r in results:
            timestamps.append(r['timestamp'])
            clusters_lst.append(r['cluster_number'])

        my_tree = ttk.Treeview(root)

        # Define Columns
        my_tree['columns'] = (
            "Timestamp", "Cluster Number", "No_machine", 'IPs')
        # Formate Columns
        my_tree.column("#0", width=0, stretch='NO')
        my_tree.column("Timestamp", anchor='w', width=150)
        my_tree.column("Cluster Number", anchor='center', width=120)
        my_tree.column("No_machine", anchor='center', width=130)
        my_tree.column("IPs", anchor='center', width=300)

        # Headings
        my_tree.heading("#0", text="", anchor='w')
        my_tree.heading("Timestamp", text="Timestamp", anchor='w')
        my_tree.heading("Cluster Number",
                        text="Cluster Number", anchor='center')
        my_tree.heading("No_machine", text="No. of machines", anchor='center')
        my_tree.heading("IPs", text="IPs", anchor='center')
        # Add Data

        white_list = get_ips_list_to_str('Results', 'white_list')

        for count, v in enumerate(results):
            aux_ips = ''
            flag_insert = True
            for ip in v['ips']:
                # Check Show withe list in mark
                if config.get('Results', 'Show_white_list') == 1:
                    aux_ips = aux_ips + ip + '; '
                else:
                    if ip not in white_list:
                        # check if ip is on white list
                        aux_ips = aux_ips + ip + '; '
                    else:
                        aux_ips = ''
            if aux_ips == '':
                pass
            else:
                aux = (v['timestamp'], v['cluster_number'],
                       v['n_elements'], aux_ips)
                my_tree.insert(parent='', index='end', iid=count, values=aux)

        my_tree.grid(row=5, columns=3, padx=30, pady=20)

        # drop donw menu to make a heatmaps
        unique_timestamps = []
        for t in timestamps:
            if t not in unique_timestamps:
                unique_timestamps.append(t)
        if not unique_timestamps:
            unique_timestamps = ['None']


        # GET HEATMAP
        lb_heatmap = tk.Label(root, text="Get HeatMap").grid(row=6, column=0)
        heatmap_date = tk.StringVar(root)
        heatmap_date.set(unique_timestamps[0])
        box_heatmap_date = tk.OptionMenu(
            root, heatmap_date, *unique_timestamps)
        box_heatmap_date.grid(row=6, column=1, sticky="N", padx=10, pady=10)

         
        btn_make_heatmap = tk.Button(root, text="Heatmap", command=lambda: make_heatmap(
            file_s, conf_id, heatmap_date.get(), view))
        btn_make_heatmap.grid(row=6, column=2)

        # SAVE Clustering
        lb_heatmap = tk.Label(
            root, text="Save Clusters to csv").grid(row=7, column=0)

               
        btn_save_clusters = tk.Button(root, text="Save", command=lambda: save_clusters_to_csv(
            file_s, conf_id, view))
        btn_save_clusters.grid(row=7, column=1, sticky="N", padx=10, pady=10)

        # GET Metrics
            
        lb_metrics = tk.Label(
            root, text="Get metrics").grid(row=8, column=0)

        btn_metrics = tk.Button(root, text="Metrics",  command=lambda: get_metrics(
            conf_id, file_s, view
        ))
        btn_metrics.grid(row=8, column=1, sticky="N", padx=10, pady=10)

    configs = get_configs_extracted(file_s)

    my_tree_config = ttk.Treeview(root)

    # Define Columns
    my_tree_config['columns'] = ('Timewindow', "Port analysis method",
                                 'N_ports', "Port List")

    my_tree_config.column("#0", width=0, stretch='NO')
    my_tree_config.column("Timewindow", anchor='center', width=120)
    my_tree_config.column("Port analysis method", anchor='center', width=200)
    my_tree_config.column("N_ports", anchor='center', width=120)
    my_tree_config.column("Port List", anchor='center', width=300)

    # Headings
    my_tree_config.heading("#0", text="", anchor='w')
    my_tree_config.heading("Timewindow", text="Timewindow", anchor='center')
    my_tree_config.heading("Port analysis method",
                           text="Port analysis method", anchor='center')
    my_tree_config.heading("N_ports", text="No. of Ports", anchor='center')
    my_tree_config.heading("Port List", text="Port List", anchor='center')

    for count, c in enumerate(configs):
        method_name, port_lis, n_ports = '', '', ''
        if c['method'] == 0:
            method_name = 'OutGene'
            port_lis = c['outgene_ports']
            n_ports = len(port_lis)
        elif c['method'] == 1:
            method_name = 'Port List'
            port_lis = c['port_day']
            n_ports = len(port_lis)
        elif c['method'] == 2:
            method_name = 'Flowhacker'
            port_lis = c['flowhacker_ports']
            n_ports = len(port_lis)
        elif c['method'] == 3:
            method_name = 'Dyn_3x'
            n_ports = c['nr_ports']
        elif c['method'] == 4:
            method_name = 'Dyn_3x + OutGene'
            port_lis = c['outgene_ports']
            n_ports = int(c['nr_ports']) + len(port_lis)

        aux = (c['time_window'], method_name, n_ports, port_lis)
        my_tree_config.insert(parent='', index='end', iid=count, values=aux)
    my_tree_config.grid(row=4, columns=3, padx=30, pady=5)

# view
    selected_view = tk.StringVar(root)
    selected_view.set('Int')
    lb_view = tk.Label(
        root, text="View").grid(row=2, column=3)
    drop_menu_view = tk.OptionMenu(
        root, selected_view, *['Int', 'Ext'])
    drop_menu_view.grid(
        row=2, column=4, sticky="N", padx=10, pady=10)

# Max entities in clusters
    lb_max_clus = tk.Label(
        root, text="Max. Elem.").grid(row=2, column=0)
    max_clus = tk.Entry(root)
    max_clus.insert(0, 1)
    max_clus.grid(row=2, column=1)

    def click_config(event):
        item = my_tree_config.selection()[0]
        conf_id = configs[int(item)]['config_id']
        tw = configs[int(item)]['time_window']
        list_clusters(conf_id, tw,
                      selected_view.get(), max_clus.get())

    my_tree_config.bind("<Double-1>", click_config)


class CyberVisualTool(tk.Tk):
    def __init__(self, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)
        self.file_selected = tk.StringVar()
        tk.Tk.wm_geometry(self, '%dx%d+%d+%d' % get_conf_geo())
        tk.Tk.wm_title(self, "Cyber Threat Visualization Tool")

        self.container = tk.Frame(self)
        self.container.pack(side="top", fill="both", expand=True)
        self.container.grid_rowconfigure(0, weight=1)
        self.container.grid_columnconfigure(0, weight=1)

            
        # Top Menu
        menu_bar = tk.Menu(self.container)
        # File Menu
        file_menu = tk.Menu(menu_bar, tearoff=0)
        file_menu.add_command(label="Activity to csv",
                              command=lambda: act_to_csv())
        file_menu.add_command(label="Clusters to csv", command=lambda: showerror(
            title="Help", message="Wait"))

        file_menu.add_command(label="Exit", command=lambda: quit)
        menu_bar.add_cascade(label="File", menu=file_menu)

        # Settings Menu
        settings_menu = tk.Menu(menu_bar, tearoff=0)
        settings_menu.add_command(
            label="Files", command=lambda: open_settings_file())
        settings_menu.add_command(
            label="Features", command=lambda: open_settings_features())
        settings_menu.add_command(
            label="Clustering", command=lambda: open_settings_clustering())
        settings_menu.add_command(
            label="History", command=lambda: open_settings_history())
        settings_menu.add_command(
            label="Results", command=lambda: open_settings_results())

        menu_bar.add_cascade(label="Settings", menu=settings_menu)

        # View Menu
        view_menu = tk.Menu(menu_bar, tearoff=0)
        view_menu.add_command(
            label="Main", command=lambda: open_settings_main_view())
        view_menu.add_command(
            label="Graph", command=lambda: open_settings_graph())
        view_menu.add_command(
            label="Heatmap", command=lambda: open_settings_heatmap())

        menu_bar.add_cascade(label='View', menu=view_menu)

        # Help Menu
        help_menu = tk.Menu(menu_bar, tearoff=0)
        help_menu.add_command(label="Exit", command=lambda: quit)
        menu_bar.add_cascade(label="Help", menu=help_menu)

        tk.Tk.config(self, menu=menu_bar)

        self.frames = {}

        # PAGES
        for fr in (MainPage, AnalysisPage):
            frame = fr(self.container, self)
            self.frames[fr] = frame

            frame.grid(row=0, column=0, sticky="nsew")

        self.show_frame(MainPage)
    
    def show_frame(self, cont):
        frame = self.frames[cont]
        frame.tkraise()

    def set_file_selected(self, file_name):
        self.file_selected = file_name

    def get_page(self, page_class):
        return self.frames[page_class]

    def refresh_frame(self, cont):
        self.frames[cont].destroy()
        frame = cont(self.container, self)
        self.frames[cont] = frame

        frame.grid(row=0, column=0, sticky="nsew")

    def delete_file(self, file_name):
        delete_file_request(file_name)

    def on_closing(self):
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            self.destroy()
            sys.exit()


def select_upload_file():
    res = ''
    file_path = tk.filedialog.askopenfilename(initialdir=config.get("File_parms", "path"), title="Select file",
                                              filetypes=(("csv files", "*.csv"), ("all files", "*.*")))

    Bar = Prog_bar('Uploading')
    res = upload_file(str(file_path))
    Bar.close_bar()


class MainPage(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        self.controller = controller
        self.b = {}        
        self.times = {}
        
        page = 1

        user_name = config.get('User','username')
        while user_name == '0':                        
            self.put_username()
            user_name = config.get('User', 'username')

        files = get_files()


        if len(files) > 0:
            label_select_file = tk.Label(
                self, text="Select file", font=FONT_LARGE).pack()
        else:
            label_select_file = tk.Label(
                self, text="Insert file", font=FONT_LARGE).pack()

        # Creating Listbox
        Lb = tk.Listbox(self, width=40, height=15, font=10)

        # Inserting items in Listbox
        for count, f in enumerate(files):
            Lb.insert(count, f)
            Lb.pack()

        def clicked_file(event):
            cs = Lb.curselection()
            file_index = int(cs[0]) * page

            # check file time line
            file_info = get_file_info(files[file_index])
            start_date = datetime.strptime(
                file_info[0], '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(
                file_info[1], '%Y-%m-%d %H:%M:%S')

            self.controller.file_selected.set(files[file_index] * page)

            controller.refresh_frame(AnalysisPage)
            controller.show_frame(AnalysisPage)

        # Binding double click with left mouse
        # button with go function
        Lb.bind('<Double-1>', clicked_file)

        #Buttons
        self.icon2 = tk.PhotoImage(file='Img/refresh_btn.png').subsample(10, 10)
        button_refresh = tk.Button(self, text="Refresh", image=self.icon2, compound="left",
                                   command=lambda: controller.refresh_frame(MainPage))
        button_refresh.pack()
        
        self.icon1 = tk.PhotoImage(file='Img/upload_btn.png').subsample(30, 30)
        button_upload_file = tk.Button(self, text="Upload new file", image=self.icon1, compound="left",
                                       command=select_upload_file)
        button_upload_file.pack( pady=15)



        file_to_delete = tk.StringVar(self, value='None')
        if len(files) > 0:
            drop_menu_delete_file = tk.OptionMenu(
                self, file_to_delete, *files).pack()
            self.icon3 = tk.PhotoImage(file='Img/delete_btn.png').subsample(10, 10)
            button_refresh = tk.Button(self, text="Delete file", image=self.icon3, compound="left",
                                    command=lambda: [controller.delete_file(file_to_delete.get()), controller.refresh_frame(MainPage)])
            button_refresh.pack(pady=5)


        '''
        button_analysis = tk.Button(self, text="Analysis File",
                                    command=lambda: controller.show_frame(AnalysisPage))
        button_analysis.pack()
        '''

    def set_file_selected(self, file_s):
        print('set:' + file_s)
        self.file_selected = file_s

    def get_file_selected(self):
        return self.file_selected

    def put_username(self):        
        self.w=popupWindow(self.master)
        self.b["state"] = "with_user" 
        self.master.wait_window(self.w.top)
        self.b["state"] = "no_user"

    def entryValue(self):
        return self.w.value



class AnalysisPage(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        self.controller = controller
        file_s = controller.file_selected.get()

        if file_s == '':
            pass
        else:
            str_start_date, str_end_date = get_file_info(file_s)
            start_date = datetime.strptime(str_start_date, '%Y-%m-%d %H:%M:%S')
            end_date = datetime.strptime(str_end_date, '%Y-%m-%d %H:%M:%S')

            list_days = []

            if end_date.date() == start_date.date():
                # Only 1 day
                list_days.append(str(start_date.date()))
            else:
                file_len_days = end_date.date() - start_date.date()
                print(file_len_days)
                for i in range(file_len_days.days):
                    list_days.append(str(start_date.date()))
                    start_date = start_date + timedelta(days=1)

            label_welcome = tk.Label(self, text=file_s, font=FONT_LARGE)
            label_welcome.grid(row=1, column=5, sticky="N", padx=10, pady=5)

            # Select Start Date (yyyy-mm-dd)
            start_date_label = tk.Label(self, text="Start date")
            start_date_label.grid(row=5, column=3, sticky="N", padx=10, pady=5)
            clicked_start_date = tk.StringVar()
            clicked_start_date.set(list_days[0])
            drop_menu_start_date = tk.OptionMenu(
                self, clicked_start_date, *list_days)
            drop_menu_start_date.grid(
                row=6, column=3, sticky="N", padx=30, pady=20)

            msg1 = tk.Label(self, text="Time (hh:mm:ss)")
            msg1.grid(row=5, column=4, sticky="N", padx=10, pady=3)

            #time_start_selected = tk.StringVar('default')
            entry_time_start = tk.Entry(self)
            entry_time_start.insert(-1, 'default')
            entry_time_start.grid(row=6, column=4)

            # Select End Date (hh:mm:ss)
            end_date_label = tk.Label(self, text="End date")
            end_date_label.grid(row=5, column=7, sticky="N", padx=10, pady=5)
            clicked_end_date = tk.StringVar()
            clicked_end_date.set(list_days[len(list_days)-1])
            drop_menu_end_date = tk.OptionMenu(
                self, clicked_end_date, *list_days)
            drop_menu_end_date.grid(
                row=6, column=7, sticky="N", padx=30, pady=20)

            msg1 = tk.Label(self, text="Time (hh:mm:ss)")
            msg1.grid(row=5, column=8, sticky="N", padx=10, pady=3)

            #time_end_selected = tk.StringVar('default')
            entry_end_start = tk.Entry(self)
            entry_end_start.insert(-1, 'default')
            entry_end_start.grid(row=6, column=8)

            # Radio buttuns to set Timef_analysiss
            label_time_f_analysis = tk.Label(self, text="Select Time Wimdows")
            label_time_f_analysis.grid(row=8, column=5, sticky="N")

            time_w_values = tk.StringVar()
            entry_time_window = tk.Entry(self, textvariable=time_w_values)
            entry_time_window.grid(row=9, column=5)

            # Analysis Method
            label_method = tk.Label(self, text="Analysis Method")
            label_method.grid(row=10, column=5, sticky="N")

            method = tk.IntVar()
            '''0: get_port_outgene, 1: get_port_day, 2: get_port_FlowHacker,
                        3: get_port_DYN3_x, 4: get_port_get_port_DYN3_x_outgene '''

            radio_btn_method_0 = tk.Radiobutton(
                self, text="OutGene", variable=method, value=0)
            radio_btn_method_0.grid(
                row=11, column=2, sticky="N", padx=10, pady=5)

            radio_btn_method_2 = tk.Radiobutton(
                self, text="FlowHacker", variable=method, value=2)
            radio_btn_method_2.grid(
                row=11, column=3, sticky="N", padx=10, pady=5)

            radio_btn_method_1 = tk.Radiobutton(
                self, text="Port list", variable=method, value=1)
            radio_btn_method_1.grid(
                row=11, column=4, sticky="N", padx=10, pady=5)

            radio_btn_method_3 = tk.Radiobutton(
                self, text="DNY3_x", variable=method, value=3)
            radio_btn_method_3.grid(
                row=11, column=5, sticky="N", padx=10, pady=5)

            radio_btn_method_4 = tk.Radiobutton(
                self, text="DYN3_x + OutGene", variable=method, value=4)
            radio_btn_method_4.grid(
                row=11, column=6, sticky="N", padx=10, pady=5)

            values = get_file_activity(file_s)
            a.bar(values['times'], values['values'])
            fig.autofmt_xdate()

            # Go back BTN
            icon = tk.PhotoImage(file='Img/back_btn.png').subsample(7, 7)

            back_btn = tk.Button(self, image=icon, text="Back", bg='#DCDCDC',
                                 command=lambda: [controller.show_frame(MainPage), restart_fig], width=40, height=40)
            back_btn.image = icon
            back_btn.grid(row=12, column=2)

            # Run extract Features + clustering BTN
            self.icon1 = tk.PhotoImage(file='Img/analysis_btn.png').subsample(5, 5)
            extract_features_button = tk.Button(
                self, text="Start Analysis",  image=self.icon1, compound="left", command=lambda: make_requests(file_s, time_w_values.get(), method.get(), clicked_start_date.get(),
                                                                           entry_time_start.get(), clicked_end_date.get(), entry_end_start.get()))
            extract_features_button.grid(
                row=12, column=5, sticky="N", padx=20, pady=70)


            canvas = FigureCanvasTkAgg(fig, self)
            canvas.get_tk_widget().grid(row=2, column=5, sticky="N", padx=5, pady=5)

            # Graph Page btn
            self.icon2 = tk.PhotoImage(file='Img/graph_btn.png').subsample(7, 7)
            button_graph = tk.Button(self, text="Graph", image=self.icon2, compound="left",
                                     command=lambda: openGraphWindow(values))
            button_graph.grid(row=12, column=6, sticky="N", padx=10, pady=70)

            # Results Btn
            self.icon3 = tk.PhotoImage(file='Img/results_btn.png').subsample(7, 7)
            button_results = tk.Button(self, text="Results",  image=self.icon3, compound="left",
                                       command=lambda: show_results(file_s))
            button_results.grid(row=12, column=7, sticky="N", padx=10, pady=70)

            controller.protocol("WM_DELETE_WINDOW", controller.on_closing)


app = CyberVisualTool()
app.mainloop()
