import tkinter as tk
import json
from configs import *
from dialog_msg import *
from tkinter import ttk
from datetime import datetime
import re


FONT = (config.get('Style', 'font'), get_int_cfg('Style', 'font_size'))
FONT_LARGE = (config.get('Style', 'font'),
              get_int_cfg('Style', 'font_large_size'))


def open_settings_main_view():

    def save_settings():
        style_keys = ['width', 'height', 'color',
                      'font', 'font_size', 'font_large_size']
        style_values = [box_width.get(), box_height.get(), box_color.get(),
                        box_font.get(), box_font_size.get(), box_font_large_size.get()]

        conf_view = {'Style': {'key': style_keys, 'value': style_values}}

        save_cfg_file(conf_view)
        setting_window.destroy()

    # WINDOW
    setting_window = tk.Tk()
    setting_window.geometry("450x400")
    setting_window.wm_title("Main View")

    # lb_settings = tk.Label(setting_window, text="Settings", font=FONT_LARGE)
    # lb_settings.grid(row=0, column=2)
    lb_style = tk.Label(setting_window, text="Window Style", font=FONT_LARGE)
    lb_style.grid(row=1, column=2)

    # SETTINGS VIEW

    lb_width = tk.Label(setting_window, text="Width").grid(row=2, column=0)
    box_width = tk.Entry(setting_window)
    box_width.insert(0, config.get('Style', 'width'))
    box_width.grid(row=2, column=2)

    lb_height = tk.Label(setting_window, text="Height").grid(row=3, column=0)
    box_height = tk.Entry(setting_window)
    box_height.insert(0, config.get('Style', 'height'))
    box_height.grid(row=3, column=2)

    lb_color = tk.Label(setting_window, text="Color").grid(row=4, column=0)
    box_color = tk.Entry(setting_window)
    box_color.insert(0, config.get('Style', 'color'))
    box_color.grid(row=4, column=2)

    lb_font = tk.Label(setting_window, text="Font").grid(row=5, column=0)
    box_font = tk.Entry(setting_window)
    box_font.insert(0, config.get('Style', 'font'))
    box_font.grid(row=5, column=2)

    lb_font_size = tk.Label(
        setting_window, text="Font Size").grid(row=6, column=0)
    box_font_size = tk.Entry(setting_window)
    box_font_size.insert(0, config.get('Style', 'font_size'))
    box_font_size.grid(row=6, column=2)

    lb_font_large_size = tk.Label(
        setting_window, text="Font Large Size").grid(row=7, column=0)
    box_font_large_size = tk.Entry(setting_window)
    box_font_large_size.insert(0, config.get('Style', 'font_large_size'))
    box_font_large_size.grid(row=7, column=2)

    btn_save = tk.Button(setting_window, text="Save",  command=save_settings)
    btn_save.grid(row=9, column=3)


def open_settings_heatmap():
    cmap_list = ['viridis', 'plasma', 'inferno', 'magma', 'cividis',
                 'Greys', 'Purples', 'Blues', 'Greens', 'Oranges', 'Reds',
                 'YlOrBr', 'YlOrRd', 'OrRd', 'PuRd', 'RdPu', 'BuPu',
                 'GnBu', 'PuBu', 'YlGnBu', 'PuBuGn', 'BuGn', 'YlGn']

    def save_settings():

        graph_window_keys = ['geo', 'cmap', 'colorbar', 'notes']
        graph_window_values = [box_h_geo.get(), selected_cmap.get(),
                               check_btn_colorbar.get(), check_btn_notes.get()]

        conf_graph = {'Heatmap': {
            'key': graph_window_keys, 'value': graph_window_values}}

        save_cfg_file(conf_graph)
        setting_window.destroy()

    setting_window = tk.Tk()
    setting_window.geometry("450x400")
    setting_window.wm_title("View Heatmap")

    # Geometry
    lb_h_geo = tk.Label(setting_window, text="Geometry").grid(row=0, column=0)
    box_h_geo = tk.Entry(setting_window)
    box_h_geo.insert(0, config.get('Heatmap', 'geo'))
    box_h_geo.grid(row=0, column=2)

    # Color Map
    lb_cmap = tk.Label(
        setting_window, text="Color Map").grid(row=1, column=0)

    selected_cmap = tk.StringVar(setting_window)
    selected_cmap.set(config.get('Heatmap', 'cmap'))

    box_cmap = tk.OptionMenu(setting_window, selected_cmap, *cmap_list)

    box_cmap.grid(row=1, column=2, sticky="N", padx=10, pady=10)

    # Colorbar btn
    lb_colorbar = tk.Label(setting_window, text="Color Bar").grid(
        row=2, column=0)
    check_btn_colorbar = tk.IntVar(setting_window)
    check_btn_colorbar.set(int(config.get('Heatmap', 'colorbar')))
    tk.Checkbutton(setting_window, text="Visible", variable=check_btn_colorbar,
                   onvalue=1, offvalue=0).grid(row=2, column=2)

    # Notes btn
    lb_notes = tk.Label(setting_window, text="Notes").grid(
        row=3, column=0)
    check_btn_notes = tk.IntVar(setting_window)
    check_btn_notes.set(int(config.get('Heatmap', 'notes')))
    tk.Checkbutton(setting_window, text="Visible", variable=check_btn_notes,
                   onvalue=1, offvalue=0).grid(row=3, column=2)

    # Save BTN
    btn_save = tk.Button(setting_window, text="Save",  command=save_settings)
    btn_save.grid(row=5, column=3)

    setting_window.mainloop()


def open_settings_graph():

    def save_settings():
        graph_window_keys = ['title', 'geo', 'fig_size',
                                      'fig_dpi', 'time_window', 'min_plot_nr', 'log_scale']
        graph_window_values = [box_g_title.get(), box_g_geo.get(), box_g_fig_size.get(),
                               box_g_fig_dpi.get(), box_g_tw.get(), box_g_min_plot.get(),
                               check_btn_log_scale.get()]

        conf_graph = {'GraphWindow': {
            'key': graph_window_keys, 'value': graph_window_values}}

        save_cfg_file(conf_graph)
        setting_window.destroy()

    setting_window = tk.Tk()
    setting_window.geometry("450x400")
    setting_window.wm_title("View Graph")

    # GRAPH WINDOW SETTINGS

    lb_style = tk.Label(
        setting_window, text="Graph Window Style", font=FONT_LARGE)
    lb_style.grid(row=1, column=2)

    lb_g_title = tk.Label(
        setting_window, text="Window Title").grid(row=2, column=0)
    box_g_title = tk.Entry(setting_window)
    box_g_title.insert(0, config.get('GraphWindow', 'title'))
    box_g_title.grid(row=2, column=2)

    lb_g_geo = tk.Label(setting_window, text="Geometry").grid(row=3, column=0)
    box_g_geo = tk.Entry(setting_window)
    box_g_geo.insert(0, config.get('GraphWindow', 'geo'))
    box_g_geo.grid(row=3, column=2)

    lb_g_fig_size = tk.Label(
        setting_window, text="Figure Size").grid(row=4, column=0)
    box_g_fig_size = tk.Entry(setting_window)
    box_g_fig_size.insert(0, config.get('GraphWindow', 'fig_size'))
    box_g_fig_size.grid(row=4, column=2)

    lb_g_fig_dpi = tk.Label(
        setting_window, text="Figure DPI").grid(row=5, column=0)
    box_g_fig_dpi = tk.Entry(setting_window)
    box_g_fig_dpi.insert(0, config.get('GraphWindow', 'fig_dpi'))
    box_g_fig_dpi.grid(row=5, column=2)

    lb_g_tw = tk.Label(setting_window, text="Time Window").grid(
        row=6, column=0)
    box_g_tw = tk.Entry(setting_window)
    box_g_tw.insert(0, config.get('GraphWindow', 'time_window'))
    box_g_tw.grid(row=6, column=2)

    lb_g_min_plot = tk.Label(setting_window, text="Min. Number to plot").grid(
        row=7, column=0)
    box_g_min_plot = tk.Entry(setting_window)
    box_g_min_plot.insert(0, config.get('GraphWindow', 'min_plot_nr'))
    box_g_min_plot.grid(row=7, column=2)

    # Scale Button
    lb_log_scale = tk.Label(setting_window, text="Scale").grid(
        row=8, column=0)
    check_btn_log_scale = tk.IntVar(setting_window)
    check_btn_log_scale.set(int(config.get('GraphWindow', 'log_scale')))
    tk.Checkbutton(setting_window, text="Log", variable=check_btn_log_scale,
                   onvalue=1, offvalue=0).grid(row=8, column=2)

    # SAVE BTN
    btn_save = tk.Button(setting_window, text="Save",  command=save_settings)
    btn_save.grid(row=10, column=3)

    setting_window.mainloop()


def open_settings_file():
    method_list = ['OutGene Port List', 'Day Ports List',
                   'FlowHacker Ports List', 'DYN3_x', 'DYN3_x + OutGene']

    def save_settings():
        method_aux = method_list.index(selected_method.get())
        file_parms_keys = ['freq', 'date_format',
                           'default_time_windows', 'default_method', 'path']
        file_parms_values = [box_freq_update.get(), box_date_format.get(),
                             box_default_tw.get(), method_aux,
                             box_path.get()]
        conf_parms = {'File_parms': {
            'key': file_parms_keys, 'value': file_parms_values}}
        save_cfg_file(conf_parms)

        setting_window.destroy()

    setting_window = tk.Tk()
    setting_window.geometry("450x250")
    setting_window.wm_title("File Settings")

    # [File_parms]

    lb_parms = tk.Label(
        setting_window, text="Upload File Parameter", font=FONT_LARGE)
    lb_parms.grid(row=1, column=2)

    lb_freq_update = tk.Label(
        setting_window, text="Frequency").grid(row=2, column=0)
    lb_freq_update_2 = tk.Label(
        setting_window, text="(for activity analysis)").grid(row=3, column=0)
    box_freq_update = tk.Entry(setting_window)
    box_freq_update.insert(0, config.get('File_parms', 'freq'))
    box_freq_update.grid(row=2, column=2)

    lb_date_format = tk.Label(
        setting_window, text="Date Format").grid(row=4, column=0)
    box_date_format = tk.Entry(setting_window)
    box_date_format.insert(0, config.get('File_parms', 'date_format'))
    box_date_format.grid(row=4, column=2)

    lb_default_tw = tk.Label(
        setting_window, text="Defaul Time Windows").grid(row=5, column=0)
    box_default_tw = tk.Entry(setting_window)
    box_default_tw.insert(0, config.get('File_parms', 'default_time_windows'))
    box_default_tw.grid(row=5, column=2)

    # Default Method
    lb_default_method = tk.Label(
        setting_window, text="Defaul Method").grid(row=6, column=0)

    selected_method = tk.StringVar(setting_window)
    selected_method.set(
        method_list[int(config.get('File_parms', 'default_method'))])

    box_default_method = tk.OptionMenu(
        setting_window, selected_method, *method_list)

    box_default_method.grid(row=6, column=2, sticky="N", padx=10, pady=10)

    lb_path = tk.Label(
        setting_window, text="Path").grid(row=7, column=0)
    box_path = tk.Entry(setting_window)
    box_path.insert(0, config.get('File_parms', 'path'))
    box_path.grid(row=7, column=2)

    # Save BTN
    btn_save = tk.Button(setting_window, text="Save",  command=save_settings)
    btn_save.grid(row=8, column=3)

    setting_window.mainloop()


def open_settings_features():

    def save_settings():
        extract_features_keys = ['n_ports', 'ip_to_match',
                                 'port_list', 'outgene_ports', 'flowhacker_ports']
        extract_features_values = [box_extract_ports.get(), box_extract_ip_to_match.get(), box_extract_port_list.get(),
                                   box_extract_outgene_ports.get(), box_extract_flowhacker_ports.get()]

        conf_parms = {'Extract_features': {
            'key': extract_features_keys, 'value': extract_features_values}}

        save_cfg_file(conf_parms)

        setting_window.destroy()

    setting_window = tk.Tk()
    setting_window.geometry("450x180")
    setting_window.wm_title("Features Settings")

    # [Extract_features]
    lb_parms = tk.Label(
        setting_window, text="Extract features", font=FONT_LARGE)
    lb_parms.grid(row=1, column=2)

    '''
    lb_extract_tw = tk.Label(
        setting_window, text="Time Windows").grid(row=5, column=0)
    box_extract_tw = tk.Entry(setting_window)
    box_extract_tw.insert(0, config.get(
        'Extract_features', 'time_window'))
    box_extract_tw.grid(row=5, column=2)
    '''
    lb_extract_ports = tk.Label(
        setting_window, text="Nr. Ports").grid(row=2, column=0)
    box_extract_ports = tk.Entry(setting_window)
    box_extract_ports.insert(0, config.get(
        'Extract_features', 'n_ports'))
    box_extract_ports.grid(row=2, column=2)

    '''lb_extract_method = tk.Label(
        setting_window, text="Method").grid(row=7, column=0)
    box_extract_method = tk.Entry(setting_window)
    box_extract_method.insert(0, config.get(
        'Extract_features', 'method'))
    box_extract_method.grid(row=7, column=2)
    '''
    lb_extract_ip_to_match = tk.Label(
        setting_window, text="IP to match").grid(row=3, column=0)
    box_extract_ip_to_match = tk.Entry(setting_window)
    box_extract_ip_to_match.insert(0, config.get(
        'Extract_features', 'ip_to_match'))
    box_extract_ip_to_match.grid(row=3, column=2)

    lb_extract_port_list = tk.Label(
        setting_window, text="Ports List").grid(row=4, column=0)
    box_extract_port_list = tk.Entry(setting_window)
    box_extract_port_list.insert(
        0, config.get('Extract_features', 'port_list'))
    box_extract_port_list.grid(row=4, column=2)

    # outgene_ports
    lb_extract_outgene_ports = tk.Label(
        setting_window, text="OutGene Ports").grid(row=5, column=0)
    box_extract_outgene_ports = tk.Entry(setting_window)
    box_extract_outgene_ports.insert(
        0,  config.get('Extract_features', 'outgene_ports'))
    box_extract_outgene_ports.grid(row=5, column=2)

    lb_extract_flowhacker_ports = tk.Label(
        setting_window, text="FlowHacker Ports").grid(row=6, column=0)
    box_extract_flowhacker_ports = tk.Entry(setting_window)
    box_extract_flowhacker_ports.insert(
        0,  config.get('Extract_features', 'flowhacker_ports'))
    box_extract_flowhacker_ports .grid(row=6, column=2)

    # Save BTN
    btn_save = tk.Button(setting_window, text="Save",  command=save_settings)
    btn_save.grid(row=8, column=3)

    setting_window.mainloop()


def open_settings_clustering():

    def save_settings():
        clusters_keys = ['k_means_clusters',
                         'elbow_method_k_min', 'elbow_method_k_max']
        clusters_values = [
            box_k_means_clusters.get(), box_elbow_min.get(), box_elbow_max.get()]

        conf_parms = {'Cluster_parms': {
            'key': clusters_keys, 'value': clusters_values}}

        save_cfg_file(conf_parms)

        setting_window.destroy()

    setting_window = tk.Tk()
    setting_window.geometry("450x160")
    setting_window.wm_title("Clustering Settings")

    # [Cluster_parms]
    lb_parms = tk.Label(
        setting_window, text="Cluster Parameter", font=FONT_LARGE)
    lb_parms.grid(row=1, column=2)

    lb_k_means_clusters = tk.Label(
        setting_window, text="K (k-means)").grid(row=2, column=0)
    box_k_means_clusters = tk.Entry(setting_window)
    box_k_means_clusters.insert(0, config.get(
        'Cluster_parms', 'k_means_clusters'))
    box_k_means_clusters.grid(row=2, column=2)

    lb_elbow_min = tk.Label(
        setting_window, text="Elbow Method Min. K").grid(row=3, column=0)
    box_elbow_min = tk.Entry(setting_window)
    box_elbow_min.insert(0, config.get('Cluster_parms', 'elbow_method_k_min'))
    box_elbow_min.grid(row=3, column=2)

    lb_elbow_max = tk.Label(
        setting_window, text="Elbow Method Max. K").grid(row=4, column=0)
    box_elbow_max = tk.Entry(setting_window)
    box_elbow_max.insert(0, config.get('Cluster_parms', 'elbow_method_k_max'))
    box_elbow_max.grid(row=4, column=2)

    # Save BTN
    btn_save = tk.Button(setting_window, text="Save",  command=save_settings)
    btn_save.grid(row=5, column=3)

    setting_window.mainloop()


def open_settings_history():
    def save_settings():

        history_keys = ['treshold']
        history_values = [box_treshold.get()]

        conf_parms = {'History_path': {
            'key': history_keys, 'value': history_values}}

        save_cfg_file(conf_parms)

        setting_window.destroy()

    setting_window = tk.Tk()
    setting_window.geometry("450x100")
    setting_window.wm_title("History Settings")

    # [History_path]
    lb_hist_path = tk.Label(
        setting_window, text="History Path Parameter", font=FONT_LARGE)
    lb_hist_path.grid(row=1, column=2)

    lb_treshold = tk.Label(
        setting_window, text="Treshold").grid(row=2, column=0)
    box_treshold = tk.Entry(setting_window)
    box_treshold.insert(0, config.get('History_path', 'treshold'))
    box_treshold.grid(row=2, column=2)

    # Save BTN
    btn_save = tk.Button(setting_window, text="Save",  command=save_settings)
    btn_save.grid(row=4, column=3)

    setting_window.mainloop()


def open_settings_results():
    def save_settings():

        result_keys = ['show_white_list', 'white_list']
        result_values = [check_btn_white_list.get(), box_white_list.get()]

        conf_parms = {'Results': {
            'key': result_keys, 'value': result_values}}

        save_cfg_file(conf_parms)

        setting_window.destroy()

    def add_ip_red_list():  

        v = config.items('Red_list')

        red_list_keys = ['ip_' + str(len(v)+1)]
        if not check_ip(box_ip.get()):
            showerror(title="Error", message="Wrong IP format")

        try:            
            datetime.strptime(box_ts.get(), '%Y-%m-%d %H:%M:%S')
            datetime.strptime(box_te.get(), '%Y-%m-%d %H:%M:%S')        
            

            red_list_values = [box_ip.get() + ', ' + box_ts.get() + ', ' + box_te.get()+ ', ' + s_view.get()]

            conf_parms = {'Red_list': {
                'key': red_list_keys, 'value': red_list_values}} 

            save_cfg_file(conf_parms)

            update()   
        except:
            showerror(title="Error", message="Wrong date format")

    def rmv_ip_red_list():
        id_select = 0
        try:
            id_select = int(box_id.get())
        except:
            showerror(title="Error", message="Wrong ID")
        if id_select > len(config.items('Red_list')) or id_select <= 0:
            showerror(title="Error", message="Wrong ID")
        else:
            config.remove_option('Red_list','ip_' + str(id_select) )
            #remove_cfg_section('ip_' + str(id_select))
            update()


    def update():
        setting_window.destroy()
        open_settings_results()

    setting_window = tk.Tk()
    setting_window.geometry("750x800")
    setting_window.wm_title("Results Settings")

    # [Results]
    tk.Label(setting_window, text="Results Parameters", font=FONT_LARGE).grid(row=1, column=2)

    # Show white list Button
    tk.Label(setting_window, text="Show White List").grid(
        row=2, column=0)
    check_btn_white_list = tk.IntVar(setting_window)
    check_btn_white_list.set(int(config.get('Results', 'show_white_list')))
    tk.Checkbutton(setting_window, text="Show", variable=check_btn_white_list,
                   onvalue=1, offvalue=0).grid(row=2, column=2)

    # WHITE LIST
    tk.Label(setting_window, text="White List").grid(row=3, column=0)
    box_white_list = tk.Entry(setting_window)
    box_white_list.insert(0, config.get('Results', 'white_list'))
    box_white_list.grid(row=3, column=2, padx=30, pady=10, ipadx=50, ipady=5)

    # RED LIST

    tk.Label(setting_window, text="Red List", font=FONT_LARGE).grid(row=4, column=0)
    my_tree = ttk.Treeview(setting_window)

    # Define Columns
    my_tree['columns'] = ('ID',
        "IP", "Time Start", "Time End", 'View')
    # Formate Columns
    my_tree.column("#0", width=0, stretch='NO')
    my_tree.column("ID", anchor='w', width=30)
    my_tree.column("IP", anchor='w', width=150)
    my_tree.column("Time Start", anchor='center', width=200)
    my_tree.column("Time End", anchor='center', width=200)
    my_tree.column("View", anchor='center', width=50)

    # Headings
    my_tree.heading("#0", text="", anchor='w')
    my_tree.heading("ID", text="ID", anchor='w')
    my_tree.heading("IP", text="IP", anchor='w')
    my_tree.heading("Time Start",
                    text="Time Start", anchor='center')
    my_tree.heading("Time End", text="Time End", anchor='center')
    my_tree.heading("View", text="View", anchor='center')

    # Add Data
    for count ,value in enumerate(config.items('Red_list')):
        aux = value[1].split(',')
        aux.insert(0, count+1)
        my_tree.insert(parent='', index='end', iid=count, values=aux)
    
    my_tree.grid(row=5, columns=3, padx=30, pady=20)

    # Add IP to Red List
    tk.Label(setting_window, text="Add IP to Red List").grid(row=6, column=2)
    tk.Label(setting_window, text="IP").grid(row=7, column=0)
    box_ip = tk.Entry(setting_window)
    box_ip.grid(row=7, column=2, padx=2, pady=10, ipadx=5, ipady=5)

    tk.Label(setting_window, text="Time Start").grid(row=8, column=0)
    box_ts = tk.Entry(setting_window)    
    box_ts.grid(row=8, column=2, padx=2, pady=10, ipadx=5, ipady=5)

    tk.Label(setting_window, text="Time End").grid(row=9, column=0)
    box_te = tk.Entry(setting_window)    
    box_te.grid(row=9, column=2, padx=2, pady=10, ipadx=5, ipady=5)

    tk.Label(setting_window, text="View").grid(row=10, column=0)
    s_view = tk.StringVar(setting_window)
    s_view.set('Int')
    box_view = drop_menu_view = tk.OptionMenu(
        setting_window, s_view, *['Int', 'Ext'])   
    box_view.grid(row=10, column=2, padx=2, pady=10, ipadx=5, ipady=5)

    # rmv IP from Red List
    tk.Label(setting_window, text="Remove IP from Red List").grid(row=11, column=2)

    tk.Label(setting_window, text="ID").grid(row=12, column=0)
    box_id = tk.Entry(setting_window)
    box_id.grid(row=12, column=2, padx=2, pady=10, ipadx=5, ipady=5)

    # Add BTN
    btn_add = tk.Button(setting_window, text="Add",  command=add_ip_red_list)
    btn_add.grid(row=13, column=0)

    # Rmv BTN
    btn_rmv = tk.Button(setting_window, text="Remove",  command=rmv_ip_red_list)
    btn_rmv.grid(row=13, column=1)

    # Save BTN
    btn_save = tk.Button(setting_window, text="Save",  command=save_settings)
    btn_save.grid(row=13, column=2)


    setting_window.mainloop()

def check_ip(ip):
    regex = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"
    if(re.search(regex, ip)):
        return True         
    else:
        return False