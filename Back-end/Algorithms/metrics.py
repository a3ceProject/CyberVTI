import pandas as pd
from DataBase.cluster_db import *
from sklearn.metrics import f1_score
from datetime import datetime, timedelta
import time

def get_red_list(time, tw, red_list_dict, view):
    red_list_ips = []
    for value in red_list_dict:
        time_start = datetime.strptime(
            value['time_start'], '%Y-%m-%d %H:%M:%S')
        time_start = time_start - timedelta(minutes=tw)
        
        time_end = datetime.strptime(value['time_end'], '%Y-%m-%d %H:%M:%S')
        #time_end = time_end - timedelta(minutes=tw)
        if value['view'] == view:
            if time >= time_start and time <= time_end and value['ip'] not in red_list_ips:
                red_list_ips.append(value['ip'])
    #print(red_list_ips)
    return red_list_ips


def get_TP_TN_FN_FP(file_id, config_id, view, time, red_list_dict, white_list):    
    
    results = json.loads(get_results_from_clusters(
        file_id, config_id, view, 1))
    df_results = pd.DataFrame(results)
    df_results.drop(
        df_results.index[df_results['timestamp'] != time], inplace=True)

    tw = get_conf_by_id(config_id)['time_window']

    time_datetime = datetime.strptime(time, '%Y-%m-%d %H:%M:%S')
    red_list_to_test = get_red_list(time_datetime, tw, red_list_dict, view)

    # TN - Not in Cluster w/ 1 element & NOT in Red List
    clusters_df = get_clusters_by_time(view, time, config_id, file_id)

    ips_to_remove = []
    fp_list = []
    fn_list = []
    tp_list = []
    for i in df_results['ips']:
        # remove from white list
        if i[0] not in white_list:
            if i[0] in red_list_to_test:
                tp_list.append(i[0])
                # TP - Cluster w/ 1 element & In Red List (start_time < time < end_time)
            if i[0] not in red_list_to_test:
                ips_to_remove.append(i[0])
                fp_list.append(i[0])
            # FP - In Cluster w/ 1 element & NOT in Red List
    
    for i in red_list_to_test:    
        if i not in tp_list:
            print(red_list_to_test)
            fn_list.append(i)
            # FN - NOT in Cluster w/ 1 element & In Red List

    df_TN = clusters_df[~clusters_df.index.isin(ips_to_remove)]
    tn = len(df_TN.index)
    fp = len(fp_list)
    tp = len(tp_list)
    fn = len(fn_list)

    return tp, tn, fn, fp


def get_metrics(tp, tn, fn, fp):

    accuracy = 100 * ((tp + tn)/(tp + tn + fp + fn)
                      ) if (tp + tn + fp + fn) != 0 else 0
    precission = 100 * (tp / (tp + fp)) if (tp + fp) != 0 else 0
    recall = 100 * (tp / (tp + fn)) if (tp + fn) != 0 else 0    
    f1_score = (2*precission*recall)/(precission +
                                      recall) if (precission + recall) != 0 else 0


    return accuracy, precission, recall, f1_score


def metrics_request(white_list, red_list_dict, conf_id, file_name, user,view):
    start = time.time()
    file_id = get_file_id(file_name, user)

    list_metrics = []

    times = get_clusters_ts_with_view(conf_id, file_id, view)

    for t in times:
        tp, tn, fn, fp = get_TP_TN_FN_FP(file_id, conf_id, view, t, red_list_dict, white_list)
        
        acc, pre, rec, f1 = get_metrics(tp, tn, fn, fp)
        list_metrics.append({'timestamp': t, 'accuracy': acc, 'precission': pre,
                             'recall': rec, 'f1_score': f1, 'TP': tp, 'TN': tn, 'FN': fn, 'FP': fp})

    df_metrics = pd.DataFrame(list_metrics)
    end = time.time()
    
    return df_metrics.to_json()

