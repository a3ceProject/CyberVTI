from configs import *
from dialog_msg import *
from api_requests import *
from datetime import datetime
from progress_bar import *


def remove_vals(values, min_to_remove):
    index_to_remove = []
    aux = 0
    vals = values['values']
    for i, val in enumerate(vals):
        if vals[i] < min_to_remove:
            index_to_remove.append(i)
        index_to_remove.sort(reverse=True)
    for i in index_to_remove:
        values['values'].pop(i)
        values['times'].pop(i)

    return values['times'], values['values']


def make_requests(file_s, time_w_values, method, start_date, hms_start,
                  end_date, hms_end):
    std_ports = 0
    f_analysiss = [int(x)
                   for x in time_w_values.split(',') if x.strip().isdigit()]

    # get configs
    date_format = config.get('File_parms', 'date_format')
    n_ports = config.get('Extract_features', 'n_ports')
    ip_to_match = config.get('Extract_features', 'ip_to_match')
    port_list = get_list_cfg('Extract_features', 'port_list')
    outgene_ports = get_list_cfg('Extract_features', 'outgene_ports')
    flowhacker_ports = get_list_cfg('Extract_features', 'flowhacker_ports')
    k_max = int(config.get('Cluster_parms', 'elbow_method_k_max'))
    k_min = int(config.get('Cluster_parms', 'elbow_method_k_min'))
    k = int(config.get('Cluster_parms', 'k_means_clusters'))
    unidirectional_flow =  int(config.get('File_parms', 'unidirectional_flow'))

    # Check Time Window
    if f_analysiss == []:
        # TEST
        showerror(title="Error", message="Wrong Time Window")

    # Check Time Start & Time End
    elif error_time_format(hms_start) or error_time_format(hms_end):
        showerror(title="Error", message="Wrong Time Format")

    # TEST
    else:
        # by default extract a full day.
        # Starts at 00 and ends 23
        if hms_start == 'default' or hms_end == 'default':
            hms_start = '00:00:00'
            hms_end = '23:59:59'

        # Concat date times
        start_time = start_date + ' ' + hms_start
        end_time = end_date + ' ' + hms_end

        date_format = config.get('File_parms', 'date_format')
        if date_format != '%Y-%m-%d %H:%M:%S':
            datetimeobject = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
            start_time = datetimeobject.strftime('%Y-%m-%d %H:%M:%S')
            datetimeobject = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
            end_time = datetimeobject.strftime('%Y-%m-%d %H:%M:%S')

        print(start_time, end_time)
        if not port_list and sorted(outgene_ports) == [22, 25, 80, 194] and sorted(flowhacker_ports) == [22, 25, 80, 194, 6667]:
            std_ports = 1
            
        Bar = Prog_bar('Progress')    
        for tw in f_analysiss: 
            #Bar.start_loading()  
            print('Time Window: ' +str(tw))    
            print(f_analysiss)     
            Bar.setp(int(10/len(f_analysiss)))
            Bar.set_label('Extracting Features: time window -> ' + str(tw))   
        # HTTP REQUEST - Extract features
            extract_features_request(file_s, [tw], method, n_ports, start_time,
                                    end_time, date_format, ip_to_match, port_list, outgene_ports,
                                    flowhacker_ports, unidirectional_flow)
            #tw = f_analysiss[0]
            Bar.setp(int(40/len(f_analysiss)))
            conf_id = get_config_id(file_s, tw, method, int(n_ports), ip_to_match, [
                                    port_list, outgene_ports, flowhacker_ports], std_ports)

            # HTTP REQUEST - clustering        
            Bar.set_label('Clustering')
            Bar.set_label('Clustering: time window -> ' + str(tw))
            print(start_time, end_time, tw,
                            file_s, ip_to_match, k_max, k_min, k, conf_id )
            clustering_request(start_time, end_time, tw,
                            file_s, ip_to_match, k_max, k_min, k, conf_id )
            Bar.setp(int(50/len(f_analysiss)))
        Bar.close_bar()

def error_time_format(time_string):
    if time_string == 'default':
        return False
    try:
        t = time_string.split(':')
        hour, minute, seg = int(t[0]), int(t[1]), int(t[2])

        if hour > 23 or hour < 0:
            return True
        if minute > 59 or minute < 0:
            return True
        if seg > 59 or seg < 0:
            return True
        if len(t) != 3:
            return True

        return False
    except:
        return True
