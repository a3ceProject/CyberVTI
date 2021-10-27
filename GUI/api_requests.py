import requests
import json
import os
from configs import *
from dialog_msg import *
#from cvt_gui import get_time_view_graph

url = "http://127.0.0.1:5000/api/"
date_format = str(config.get('File_parms', 'date_format'))

def get_user_id(username):
    payload={
        "username": username
    }
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("GET", url+'users', headers=headers, data=json.dumps(payload))
    print(json.loads(response.text)['user_id'])
    return json.loads(response.text)['user_id']


USER_ID = 0

def extract_features_request(file_name, TimeWindows, method, nr_ports,
                             start_date, end_date, date_format, ip_to_match,
                             port_list, outgene_ports, flowhacker_ports, unidirectional_flow):

    payload = {
        "file_name": file_name,
        "user": USER_ID,
        "TimeWindows": TimeWindows,
        "method": int(method),
        "nr_ports": int(nr_ports),
        "start_date": start_date,
        "end_date": end_date,
        "DATE_FORMAT": date_format,
        "method": method,
        "IP_TO_MATCH": ip_to_match,
        "PORT_DAY": port_list,
        "OUTGENE_PORTS": outgene_ports,
        "FLOWHACKER_PORTS": flowhacker_ports,
        "unidirectional_flow": unidirectional_flow
    }

    headers = {
        'Content-Type': 'application/json'
    }    
    response = requests.request(
        "POST", url+"features/extract", headers=headers, data=json.dumps(payload))


def get_file_info(file_in):
    
    payload = {
        "user": USER_ID
    }
    headers = {
        'Content-Type': 'application/json'
    }    

    response = requests.request(
        "GET", url + 'files/' + file_in, headers=headers, data=json.dumps(payload))

    return json.loads(response.text)['File Info']
    


def get_files(user=USER_ID):
    payload = {
        "user": user
    }
    headers = {
        'Content-Type': 'application/json'
    }    


    response = requests.request(
        "GET", url + 'files', headers=headers, data=json.dumps(payload))

    return json.loads(response.text)['Files']

def delete_file_request(file_name):
    print(file_name)
    payload = {
        "user": USER_ID
    }
    headers = {
        'Content-Type': 'application/json'
    }    


    response = requests.request(
        "DELETE", url + 'files/'+file_name, headers=headers, data=json.dumps(payload))

    #print(response.text)
    return response.text

def upload_file(file_path, user_nr=USER_ID):
    file_name = os.path.basename(file_path)
    exten = os.path.splitext(file_path)[1]
    default_tw = json.loads(config.get('File_parms', 'default_time_windows'))
    default_tw = ' '.join([str(elem) for elem in default_tw])
    default_method = int(config.get('File_parms', 'default_method'))
    ip_to_match = config.get('Extract_features', 'ip_to_match')
    unidirectional_flow = int(config.get('File_parms', 'unidirectional_flow'))

    payload = {'user': str(user_nr), 'freq': config.get('File_parms', 'Freq'),
               'date_format': date_format, 'ip_to_match': ip_to_match, 'default_tw': default_tw, 'default_method':
               default_method, 'unidirectional_flow': unidirectional_flow }
    print(payload)
    
    files = [
        ('file', (file_name, open(file_path, 'rb'), exten))
    ]
    headers = {}

    response = requests.request(
        "POST", url + 'files', headers=headers, data=payload, files=files)
    return response


def get_file_activity(file_name, time = config.get('GraphWindow', 'time_window')):
    
    payload = {
        "user": USER_ID,
        "time": time
    }
    headers = {
        'Content-Type': 'application/json'
    }    
    
    response = requests.request("GET", url+"files/" + file_name + "/file_activity", headers=headers, data=json.dumps(payload))
    
    return json.loads(response.text)['File_activity']


def clustering_request(start_time, end_time, tw_clus,
                       file_s, ip_to_match, k_max, k_min, k, conf_id):

    payload = {
        "time_start": start_time,
        "time_end": end_time,
        "time_window": tw_clus,
        "file_name": file_s,
        "user": USER_ID,
        "ip_view": ip_to_match,
        "k_max": k_max,
        "k_min": k_min,
        "k": k,
        "conf_id": conf_id
    }

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request(
        "POST", url+'clusters/fast', headers=headers, data=json.dumps(payload))


def get_config_id(file_name, tw, method, nr_ports, ip_to_match, ports_list, std_ports):

    payload = {
        "file_name": file_name,
        "user": USER_ID,
        "tw": tw,
        "method": method,
        "nr_ports": nr_ports,
        "ip_to_match": ip_to_match,
        "ports_lst": ports_list,
        "std_ports": std_ports
    }
    headers = {'Content-Type': 'application/json'}

    response = requests.request(
        "GET", url + "features/results", headers=headers, data=json.dumps(payload))

    return json.loads(response.text)['Config_id']


def get_timewindows_info(file_name):

    payload = {
        "user": USER_ID
    }
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request(
        "GET", url+"file/" + file_name + "/conf_time_windows", headers=headers, data=json.dumps(payload))
    return json.loads(response.text)['TimeWindows']

def get_heatmap_data(file_name, config_id, timestamp,  view):
    payload = {
        "user": USER_ID, 
        "config_id": config_id, 
        "view": view, 
        "timestamp": timestamp
    }
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request(
         "GET", url+"clusters/heatmap?file_name="+file_name, headers=headers, data=json.dumps(payload))
    
    return json.loads(response.text)

def get_result_clusters(file_name, config_id, view, n_max):
    payload = {
        "user": USER_ID,       
        "view": view, 
        "n_max": n_max

    }
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request(
        "GET", url+"clusters/results?file_name="+file_name +"&result_id=" +str(config_id), headers=headers, data=json.dumps(payload))
    
    return json.loads(response.text)['Results']

def get_configs_extracted(file_name):
    payload = {"user": USER_ID  }
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request(
        "GET", url+"file/" + file_name + "/results", headers=headers, data=json.dumps(payload))

    return json.loads(response.text)['Configs']

def get_all_clusters(file_name, config_id, view, timestamp):
    payload = {
        "user": USER_ID,
        "view": view, 
        }
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request(
        "GET", url+"clusters/results/all?file_name="+file_name+"&result_id=" + str(config_id)
        +"&timestamp="+timestamp, headers=headers, data=json.dumps(payload))
    
    return json.loads(response.text)['Results']

def get_tws_from_clusters(file_name, config_id, view):
    payload = {
        "user": USER_ID,        
        }
    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("GET", url+"clusters/timestamps/?file_name="+file_name+"&result_id=" + str(config_id)+ '&view=' +view ,
    headers=headers, data=json.dumps(payload))

    return json.loads(response.text)['Timestamps']

def get_metrics_request(white_list, red_list, config_id, file_name, view):
    payload = {
        "white_list": white_list,
        "red_list_dict": red_list,
        "view": view, 
        "user": USER_ID
        }
    headers = {
        'Content-Type': 'application/json'
    }
    
    response = requests.request(
        "GET", url+"metrics?file_name="+file_name+"&result_id="+str(config_id), headers=headers, data=json.dumps(payload))
    print(json.dumps(payload))
    return json.loads(response.text)

def insert_user(username):   

    payload = {'username': username}       

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request(
        "POST", url + 'users', headers=headers, data=json.dumps(payload))
    
    
    if response.status_code == 200:
        update_cfg('User', 'username', username)
        return True
    else:
        showerror(title="Error", message="Try new username")
        return False


