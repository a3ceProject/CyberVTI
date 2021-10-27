import tkinter as tk
import json
from configs import *
from dialog_msg import *
from tkinter import ttk
from api_requests import *
import pandas as pd
from progress_bar import *


FONT_LARGE = (config.get('Style', 'font'),
              get_int_cfg('Style', 'font_large_size'))


def get_metrics(config_id, file_name, view):

    bar = Prog_bar('Getting metrics')

    avg = {}
    total = {}
    list_red = []
    white_list = config.get('Results', 'white_list').split(', ')
    red_list = config.items('Red_list')

    bar.setp(5)

    for r in red_list:
        aux = r[1].split(', ')
        list_red.append({'ip': aux[0], 'time_start':  aux[1], 'time_end': aux[2],
                         'view': aux[3]})

    bar.setp(15)
    metrics_json = get_metrics_request(
        white_list, list_red, config_id, file_name, view)
    bar.setp(75)
    # print(metrics_json)
    df_metrics = pd.DataFrame.from_dict(metrics_json)

    total['tp'] = df_metrics["TP"].sum()
    total['tn'] = df_metrics["TN"].sum()
    total['fn'] = df_metrics["FN"].sum()
    total['fp'] = df_metrics["FP"].sum()

    avg['accuracy'] = round(100 * ((total['tp'] + total['tn'])/(total['tp'] + total['tn'] + total['fp'] + total['fn'])
                                   ) if (total['tp'] + total['tn'] + total['fp'] + total['fn']) != 0 else 0, 2)
    avg['precision'] = round(100 * (total['tp'] / (total['tp'] + total['fp']))
                              if (total['tp'] + total['fp']) != 0 else 0, 2)

    avg['recall'] = round(100 * (total['tp'] / (total['tp'] + total['fn']))
                          if (total['tp'] + total['fn']) != 0 else 0, 2)

    avg['f1_score'] = round((2*avg['precision']*avg['recall'] )/(avg['precision'] +
                                      avg['recall'] ) if (avg['precision']+ avg['recall'] ) != 0 else 0, 2)

    bar.close_bar()

    root = tk.Tk()
    root.geometry("1000x600")
    root.wm_title("Metrics")

    # AVGs Params
    frame_avg = tk.Frame(root, width=100, height=50)
    frame_avg.place(x=100, y=0)

    frame_total = tk.Frame(root, width=100, height=50)
    frame_total.place(x=500, y=0)

    tk.Label(frame_avg, text="Metrics AVG",
             font="Verdana 15 underline").grid(row=1, column=0)

    tk.Label(frame_avg, text="Accuracy: " +
             str(avg['accuracy']), font=FONT_LARGE).grid(row=2, column=0, padx=30)
    tk.Label(frame_avg, text="Precision: " +
             str(avg['precision']), font=FONT_LARGE).grid(row=3, column=0, padx=30)
    tk.Label(frame_avg, text="Recall: " +
             str(avg['recall']), font=FONT_LARGE).grid(row=4, column=0, padx=30)
    tk.Label(frame_avg, text="F1 Score: " +
             str(avg['f1_score']), font=FONT_LARGE).grid(row=5, column=0, padx=30)

    # TOTAL Params

    tk.Label(frame_total, text="Metrics Totals",
             font="Verdana 15 underline").grid(row=1, column=2)

    tk.Label(frame_total, text="True Positives (TP): " +
             str(total['tp']), font=FONT_LARGE).grid(row=2, column=2)
    tk.Label(frame_total, text="True Negatives (TN): " +
             str(total['tn']), font=FONT_LARGE).grid(row=3, column=2)
    tk.Label(frame_total, text="False Negatives (FN): " +
             str(total['fn']), font=FONT_LARGE).grid(row=4, column=2)
    tk.Label(frame_total, text="False Positives (FP): " +
             str(total['fp']), font=FONT_LARGE).grid(row=5, column=2)

    def show_details(df_metrics):
        # tk.Label(root, text="Metrics details",
        #         font=FONT_LARGE).grid(row=6, column=0)
        my_tree = ttk.Treeview(root)
        # Define Columns
        my_tree['columns'] = ('Time', 'Accuracy', "Precision", "Recall",
                              'F1 Score', "TP", "TN", "FN", "FP")
        # Formate Columns
        my_tree.column("#0", width=0, stretch='NO')
        my_tree.column("Time", anchor='center', width=150)
        my_tree.column("Accuracy", anchor='center', width=120)
        my_tree.column("Precision", anchor='center', width=120)
        my_tree.column("Recall", anchor='center', width=120)
        my_tree.column("F1 Score", anchor='center', width=120)
        my_tree.column("TP", anchor='center', width=60)
        my_tree.column("TN", anchor='center', width=60)
        my_tree.column("FN", anchor='center', width=60)
        my_tree.column("FP", anchor='center', width=60)

        # Headings
        my_tree.heading("#0", text="", anchor='w')
        my_tree.heading("Time", text="Time", anchor='center')
        my_tree.heading("Accuracy", text="Accuracy", anchor='center')
        my_tree.heading("Precision", text="Precision", anchor='center')
        my_tree.heading("Recall", text="Recall", anchor='center')
        my_tree.heading("F1 Score", text="F1 Score", anchor='center')
        my_tree.heading("TP", text="TP", anchor='center')
        my_tree.heading("TN", text="TN", anchor='center')
        my_tree.heading("FN", text="FN", anchor='center')
        my_tree.heading("FP", text="FP", anchor='center')

        for count, value in df_metrics.iterrows():
            aux = []
            for column in list(df_metrics.columns):
                aux.append(df_metrics.at[count, column])
            my_tree.insert(parent='', index='end', iid=count, values=aux)

        my_tree.grid(row=6, columns=3, padx=30, pady=200, ipady=50)

    # BTN details
    btn_details = tk.Button(root, text="Show details",
                            command=lambda: [show_details(df_metrics), btn_details.destroy()])
    btn_details.grid(row=7, column=0, padx=300, pady=150)
