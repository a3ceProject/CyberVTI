import tkinter as tk
import pandas as pd
from api_requests import *


def act_to_csv():
    root = tk.Tk()
    root.wm_title("File Activity to csv")
    root.geometry("500x300")

    files = get_files()

    label_file = tk.Label(root, text="Select file").grid(row=1, column=0)
    clicked_file = tk.StringVar(root)
    clicked_file.set(files[0])
    drop_menu_file = tk.OptionMenu(root, clicked_file, *files)
    drop_menu_file.grid(row=2, column=0)

    label_freq = tk.Label(
        root, text="Select time of analysis").grid(row=1, column=1)
    time_freq = config.get("File_parms", "freq").split(", ")

    clicked_time = tk.StringVar(root)
    clicked_time.set(time_freq[0])
    drop_menu_time = tk.OptionMenu(root, clicked_time, *time_freq)
    drop_menu_time.grid(row=2, column=1)

    button_to_save = tk.Button(root, text="Save csv",
                               command=lambda: save_csv(clicked_file, clicked_time))
    button_to_save.grid(row=3, column=2)

    def save_csv(file_s, time):
        data = get_file_activity(file_s.get(), time.get())
        
        to_save = pd.DataFrame(data=data)        
        check_path_and_create(file_name=file_s)
        dir_path = os.path.dirname(os.path.abspath(__file__))+'/Saved/' + file_s.get() + '/Activity/'
        to_save.to_csv(dir_path + 'activity_' +time.get() + '.csv')

        root.destroy()

def save_clusters_to_csv(file_name, conf_id, view):
    
    timestamps = get_tws_from_clusters(file_name, conf_id, view)
    
    check_path_and_create(file_name)
    dir_path = os.path.dirname(os.path.abspath(__file__))+'/Saved/' + file_name + '/Clusters/'
    for t in timestamps:    
        df = pd.DataFrame.from_dict(get_all_clusters(file_name, conf_id, view, t))
        df = df.sort_values(by=['cluster_number'])
        df.to_csv(dir_path  + t + 'clusters.csv')

def check_path_and_create(file_name):
    dir_path = os.path.dirname(os.path.abspath(__file__))+'/Saved/'
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)            
        if not os.path.exists(dir_path+file_name):
            os.makedirs(dir_path+file_name)
            dir_path = dir_path+file_name
            if not os.path.exists(dir_path+'/Clusters/'):
                os.makedirs(dir_path+'/Clusters/')
                
            if not os.path.exists(dir_path+'/Activity/'):
                os.makedirs(dir_path+'/Activity/')