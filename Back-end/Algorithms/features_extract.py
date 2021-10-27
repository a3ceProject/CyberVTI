'''
Script for  features extraction  (based on the OutGene method)
For more information run: python3 features_extract.py --help
'''

import os, time, gc
import warnings
from datetime import datetime
from sys import argv, exit, version_info  # le linha de comando
import pandas as pd
from joblib import Parallel, delayed
from pathlib import Path
from progress.bar import IncrementalBar
from tqdm import tqdm
from DataBase.features_db import *
from DataBase.files_db import get_day_start_end_date, get_day_start_end_date_by_id
from Algorithms.load_file import *



warnings.filterwarnings("ignore")  # remove warnings


def get_ports(conf, file_id, X=None):
    '''
      Ports extraction

    :param method: service name 
    :type: int
    :param X: method value (if applied)
    :return: extration port
    :rtype: list

    '''
    method, nr_ports, port_day_list, outgene_port_list, flowhacker_list = conf[
        1], conf[2], conf[4], conf[5], conf[6]

    dyn_methods = {3: get_port_DYN3_x, 4: get_port_get_port_DYN3_x_outgene}

    if method == 0:
        # return OutGene Ports List
        return outgene_port_list, outgene_port_list
    elif method == 1:
        # return Day Ports List
        return port_day_list, port_day_list
    elif method == 2:
        # return FlowHacker Ports List
        return flowhacker_list, flowhacker_list
    elif method == 3:
        # Exec and return DYN3 ports list
        return get_port_DYN3_x(X, nr_ports, file_id)
    else:
        return get_port_get_port_DYN3_x_outgene(X, nr_ports, file_id, outgene_port_list)


def get_port_DYN3_x(df, nr_ports, file_id):
    '''
    get according to df provided, the most used, the least used and the most unusual ports (most used for less than 10 IP)
    Ports analysed are between 0-49151, and for the less used and up to 1024

    :param df: dataframe with ports
    :type df: DataFrame
    :return:  ports list
    :rtype: list[int]
    '''
    '''
    if nr_ports == 100:
        ports = get_dyn3_x_ports(file_id)
        print(ports)
        return ports, ports
    '''
    x=nr_ports
    
    print("NR PORTS:     " +str(x))
    aux = df[df['Dst Port'] < 49151].groupby(['Dst Port']).nunique()  # dst port counting
    uniqueports = aux.sort_values(by=['Src IP'],
                                  ascending=False)  # orders from the most contacted to the least contacted ports
    uniqueports = uniqueports[
        uniqueports['Src IP'] < 10].index.tolist()  # list of Ports contacted by less than 10 IP
    Dst_port = df[df['Dst Port'] < 49151]
    Src_port = df[df['Src Port'] < 49151]
    most_used_ports_dst = Dst_port['Dst Port'].value_counts()  # counting the use of the ports of destination
    most_used_ports_src = Src_port['Src Port'].value_counts()  # counting the use of the ports of origin
    most_used_ports = most_used_ports_dst.append(most_used_ports_src)  # add counting 
    most_used_ports = most_used_ports.sort_values(ascending=False)  # descending  orders 
   
    port = list(set().union(
        list(most_used_ports.head(int(x / 3)).index),  # must used ports
        list(most_used_ports[most_used_ports.index.isin(uniqueports)].head(int(x/3)).index),
        # less common and more used ports
        list(most_used_ports[most_used_ports.keys() < 1024].tail(
            int(x / 3)).index)))  # less used ports and below 1024
    print(port)
    return port, port


def get_port_get_port_DYN3_x_outgene(df, nr_ports, file_id,  outgene_ports):
    '''
     Returns ports according to metogo DYN3_x and outgene, i.e. a union between the two
    :param df: ports dataframe 
    :type df: DataFrame
    :return: ports list 
    :rtype: list[int]
    '''
    port1a, port1b = get_port_DYN3_x(df, nr_ports, file_id)
    port2a, port2b = outgene_ports, outgene_ports
    porta = list(set(port1a+port2a))
    portb = list(set(port1b + port2b))
    return porta, portb


def get_port_day():
    '''
    returns ports involved in attacks on each of the days (point of view of origin and destination)
    :return: ports (org and dst)
    :rtype: list
    '''
    return PORT_DAY, PORT_DAY


def get_dst_pkts(df, list_ports):
    '''
   function to extract the values of each feature related to ports from the Destination point of view.
    Calculates the number of packages sent/received
    :param df: analysed dataframe 
    :type: dataframe
    :param list_ports: port list 
    :type: list
    :return: packege counting 
    :rtype: list
    '''
    #corrects Bwd and Fwd values
    df['Tot Bwd Pkts'] -= 1
    df['Tot Fwd Pkts'] += 1
    # remove unused ports
    df_DstTo = df[df['Dst Port'].isin(list_ports)]
    df_DstFrom = df[df['Src Port'].isin(list_ports)]
    df_DstTo = df_DstTo[['Dst IP', 'Dst Port','Tot Bwd Pkts','Tot Fwd Pkts']]
    df_DstFrom = df_DstFrom[['Dst IP', 'Src Port','Tot Bwd Pkts','Tot Fwd Pkts']]
    df_DstTo = df_DstTo.groupby(['Dst IP', 'Dst Port']).sum().reset_index()   # counts the number of occurrences according to an IP+ destination port
    df_DstTo = df_DstTo.pivot(index='Dst IP', columns='Dst Port', values='Tot Bwd Pkts')  # data cast
    df_DstTo = df_DstTo.rename(columns=lambda x: str(x) + 'DstTo')  # change name
    df_DstFrom = df_DstFrom.groupby(['Dst IP', 'Src Port']).sum().reset_index()  # ccounts the number of occurrences according to an IP+ origin port
    df_DstFrom = df_DstFrom.pivot(index='Dst IP', columns='Src Port', values='Tot Fwd Pkts')
    df_DstFrom = df_DstFrom.rename(columns=lambda x: str(x) + 'DstFrom')  # data cast
    result = pd.concat([df_DstFrom, df_DstTo], axis=1, sort=False)  #  dataframes union
    return result

def get_src_pkts(df, list_ports):
    '''
    function to extract the values of each feature related to ports from the point of view of Source
    Calculates the number of packages sent/received
    :param df: analysed dataframe 
    :type: dataframe
    :param list_ports: port list 
    :type: list
    :return: packege counting 
    :rtype: list
    '''
    #corrects Bwd and Fwd values
    df['Tot Bwd Pkts'] -= 1
    df['Tot Fwd Pkts'] += 1
    # remove unused ports
    df_SrcFrom=df[df['Dst Port'].isin(list_ports)]
    df_SrcTo=df[df['Src Port'].isin(list_ports)]
    df_SrcFrom = df_SrcFrom[['Src IP', 'Dst Port','Tot Bwd Pkts','Tot Fwd Pkts']]
    df_SrcTo = df_SrcTo[['Src IP', 'Src Port','Tot Bwd Pkts','Tot Fwd Pkts']]
    df_SrcFrom = df_SrcFrom.groupby(['Src IP', 'Dst Port']).sum().reset_index() #  counts the number of occurrences according to an IP+ destination port
    df_SrcFrom = df_SrcFrom.pivot(index='Src IP', columns='Dst Port', values='Tot Bwd Pkts')  # # data cast
    df_SrcFrom = df_SrcFrom.rename(columns=lambda x: str(x) + 'SrcFrom')  #  change name
    df_SrcTo = df_SrcTo.groupby(['Src IP', 'Src Port']).sum().reset_index()   # ccounts the number of occurrences according to an IP+ origin port
    df_SrcTo = df_SrcTo.pivot(index='Src IP', columns='Src Port', values='Tot Fwd Pkts')
    df_SrcTo = df_SrcTo.rename(columns=lambda x: str(x) + 'SrcTo')  # # data cast
    result = pd.concat([df_SrcTo, df_SrcFrom], axis=1, sort=False)  # dataframes union
    return result


def check_lists(a, b):
    if sorted(a) == sorted(b): 
        return True
    else:
        return False


def check_extract_features(file_id, tw, conf):
    # checks if there are features that need to be extracted 
    # or if they have already been extracted with these settings 
    configs = get_config_features(file_id)
    
    for c in configs:
        if c['time_window'] == tw and c['method'] == conf[1] and c['nr_ports'] == conf[2] and c['ip_to_match'] == conf[3] and check_lists(c['port_day'], conf[4]) and check_lists(c['outgene_ports'], conf[5]) and check_lists(c['flowhacker_ports'], conf[6]):
            print('features already extracted')            
            return False       
    return True


def window_features(i, dataframe,day, window_size, port_list_src, port_list_dst, conf, file_id):
    '''
      extracts the features for a window

    :param day: day
    :type day: str
    :param window_size: window size
    :type window_size: int
    :param i: timestamp 
    :type i: datetime
    :param dataframe: data
    :type dataframe: panda
    :param port_list_dst: dst ports
    :type port_list_dst: list[int]
    :param port_list_src: org ports
    :type port_list_src: list[int]
    '''
    
    ip_to_match = conf[3]    
   
    warnings.filterwarnings("ignore")  # remove  warnings
    dataframe = dataframe.drop(columns='Timestamp')  # remove Timesatmp
    # select only those flows that have internal ip or as origin, or as destination
    dataframe = dataframe[dataframe['Src IP'].str.match(
        ip_to_match) | dataframe['Dst IP'].str.match(ip_to_match)]

    if (len(list(dataframe.index)) >= 10): #minimum of 10 events
        # Src
        # Extracting separate ports used to make communications
        SrcPortUsed = dataframe[['Src Port', 'Src IP']].groupby('Src IP', axis=0, as_index=True).nunique()
        SrcPortUsed = SrcPortUsed['Src Port']
        # Extract different ports contacted
        SrcPortContacted = dataframe[['Dst Port', 'Src IP']].groupby('Src IP', axis=0, as_index=True).nunique()
        SrcPortContacted = SrcPortContacted['Dst Port']
        # Extrair diferentes IPs de destino contactados
        SrcIPContacted = dataframe[['Dst IP', 'Src IP']].groupby('Src IP', axis=0, as_index=True).nunique()
        SrcIPContacted = SrcIPContacted['Dst IP']
        # Extract different destination IPs contacted
        SrcTotLenSent = dataframe[['TotLen Fwd Pkts', 'Src IP']].groupby('Src IP', axis=0, as_index=True).sum()
        # Extract total number of received package sizes
        SrcTotLenRcv = dataframe[['TotLen Bwd Pkts', 'Src IP']].groupby('Src IP', axis=0, as_index=True).sum()
        # Extrair numero total de sessoes estabelecidas
        SrcTotConn = dataframe[['Dst IP', 'Src IP']].groupby('Src IP', axis=0, as_index=True).count()

        SrcTotalNumPkts = dataframe[['Tot Bwd Pkts', 'Tot Fwd Pkts', 'Src IP']].groupby('Src IP', axis=0,
                                                                                        as_index=True).sum()
        SrcTotalNumPkts['Tot Pckts'] = SrcTotalNumPkts['Tot Bwd Pkts'] + SrcTotalNumPkts['Tot Fwd Pkts']
        SrcTotalNumPkts = SrcTotalNumPkts['Tot Pckts']  # feature Total number of packets exchanged

        SrcTotalNumBytes = dataframe[['TotLen Bwd Pkts', 'TotLen Fwd Pkts', 'Src IP']].groupby('Src IP', axis=0,
                                                                                               as_index=True).sum()
        SrcTotalNumBytes['TotLen Pckts'] = SrcTotalNumBytes['TotLen Fwd Pkts'] + SrcTotalNumBytes['TotLen Bwd Pkts']
        SrcTotalNumBytes = SrcTotalNumBytes['TotLen Pckts']  # feature Overall sum of bytes

        SrcPktRate = dataframe[['Flow Duration', 'Src IP']].groupby('Src IP', axis=0, as_index=True).sum()
        SrcPktRate = SrcPktRate.replace(0, 0.1)  # Avoids that when FlowDuration=0 becomes SrcPktRate=Infinity
        SrcPktRate['SrcPcktRate'] = SrcTotalNumPkts / SrcPktRate['Flow Duration'] # feature Ratio of the number of packets sent and its duration
        SrcPktRate = SrcPktRate['SrcPcktRate']

        SrcAvgPktSize = SrcTotalNumBytes / SrcTotalNumPkts  # feature Average packet size



        # Dst
        #  Extracting separate ports used to make communications
        DstPortUsed = dataframe[['Dst Port', 'Dst IP']].groupby('Dst IP', axis=0, as_index=True).nunique()
        DstPortUsed = DstPortUsed['Dst Port']
        # Extract different ports contacted
        DstPortContacted = dataframe[['Src Port', 'Dst IP']].groupby('Dst IP', axis=0, as_index=True).nunique()
        DstPortContacted = DstPortContacted['Src Port']
        #   Extrair diferentes IPs de destino contactados
        DstIPContacted = dataframe[['Src IP', 'Dst IP']].groupby('Dst IP', axis=0, as_index=True).nunique()
        DstIPContacted = DstIPContacted['Src IP']
        # Extract different destination IPs contacted
        DstTotLenSent = dataframe[['TotLen Bwd Pkts', 'Dst IP']].groupby('Dst IP', axis=0, as_index=True).sum()
        #  Extract total number of received package sizes
        DstTotLenRcv = dataframe[['TotLen Fwd Pkts', 'Dst IP']].groupby('Dst IP', axis=0, as_index=True).sum()
        # Extrair numero total de sessoes estabelecidas
        DstTotConn = dataframe[['Src IP', 'Dst IP']].groupby('Dst IP', axis=0, as_index=True).count()

        DstTotalNumPkts = dataframe[['Tot Bwd Pkts', 'Tot Fwd Pkts', 'Dst IP']].groupby('Dst IP', axis=0,
                                                                                        as_index=True).sum()
        DstTotalNumPkts['Tot Pckts'] = DstTotalNumPkts['Tot Bwd Pkts'] + DstTotalNumPkts['Tot Fwd Pkts']
        DstTotalNumPkts = DstTotalNumPkts['Tot Pckts']  # feature Total number of packets exchanged

        DstTotalNumBytes = dataframe[['TotLen Bwd Pkts', 'TotLen Fwd Pkts', 'Dst IP']].groupby('Dst IP', axis=0,
                                                                                               as_index=True).sum()
        DstTotalNumBytes['TotLen Pckts'] = DstTotalNumBytes['TotLen Fwd Pkts'] + DstTotalNumBytes['TotLen Bwd Pkts']
        DstTotalNumBytes = DstTotalNumBytes['TotLen Pckts']  # feature Overall sum of bytes

        DstPktRate = dataframe[['Flow Duration', 'Dst IP']].groupby('Dst IP', axis=0, as_index=True).sum()
        DstPktRate = DstPktRate.replace(0, 0.1)  # Avoids that when FlowDuration=0 becomes SrcPktRate=Infinity
        DstPktRate['DstPcktRate'] = DstTotalNumPkts / DstPktRate['Flow Duration']
        DstPktRate = DstPktRate['DstPcktRate']  # feature Ratio of the number of packets sent and its duration

        DstAvgPktSize = DstTotalNumBytes / DstTotalNumPkts  # feature Average packet size





        # Extract the packages sent and received at each port from the point of view of origin and destination
        SrcPkt = get_src_pkts(dataframe[['Src IP', 'Dst Port', 'Src Port', 'Tot Bwd Pkts', 'Tot Fwd Pkts']],
                              port_list_src)

        DstPkt = get_dst_pkts(dataframe[['Dst IP', 'Dst Port', 'Src Port', 'Tot Bwd Pkts', 'Tot Fwd Pkts']],
                              port_list_dst)

        #  Concatenation of all features
        Tot = pd.concat(
            [SrcIPContacted, SrcPortUsed, SrcPortContacted, SrcTotLenRcv, SrcTotLenSent, SrcTotConn,SrcPktRate, SrcAvgPktSize, SrcPkt,
             DstIPContacted, DstPortUsed, DstPortContacted, DstTotLenRcv, DstTotLenSent, DstTotConn,DstPktRate,DstAvgPktSize, DstPkt],
            axis=1, sort=False)
        Tot.fillna(value=0, inplace=True)  # change values with Nan
        Tot.columns = ['SrcIPContacted', 'SrcPortUsed', 'SrcPortContacted', 'SrcTotLenRcv', 'SrcTotLenSent','SrcConn','SrcPktRate','SrcAvgPktSize'] \
                      + list(SrcPkt.columns) + \
                      ['DstIPContacted', 'DstPortUsed', 'DstPortContacted','DstTotLenRcv', 'DstTotLenSent','DstTotConn','DstPktRate','DstAvgPktSize'] + list(DstPkt.columns)

        #print(i)
        #i = datetime.strptime(str(i), '%Y-%m-%d %H:%M:%S')
        #i = i.timestamp() - (4 * 60 * 60)  # setting the time (lag)
        #i = datetime.fromtimestamp(i)
        date_time_window = str(day) + ' ' + str(i).split(' ')[-1]
        #print(date_time_window )
        file_name = get_file_name(file_id)
        file_dir = file_name[:len(file_name)-4]        
        #print(file_name, time, conf[1], conf[2], conf[3], [conf[4],conf[5], conf[6]], 1)

        conf_id = get_config_id(file_id, window_size, conf[1], conf[2], conf[3], [conf[4],conf[5], conf[6]], 1)
        #conf_id = 100
        dir_path = check_path_and_create(file_dir, conf_id, user_id=get_user_file(file_id))       
        
        insert_fixed_features(Tot, dir_path, date_time_window)

        

def exec_features_extract(file_id, TimeWindows, conf):   
    list_tws = list(map(int, TimeWindows))
    file_name = get_file_name(file_id)
    file_dir = file_name[:len(file_name)-4]    
    user = get_user_file(file_id)  
    uni_direct = int(conf[7]) 
    date_format = conf[0]
        
    data = open_file(file_name,user, conf[0], int(conf[7]))
   
    
    try:
        days, start_date, end_date = get_day_start_end_date_by_id(file_id) 
        print(days, start_date, end_date)              
        
    except KeyError:
        print('Error on dates')
      
    # remove values outside the desired dates    
    #data = data[data['Timestamp'] >= start_date]
    #data = data[data['Timestamp'] <= end_date]  

    # Get list Ports
    port_list_src, port_list_dst = get_ports(conf, file_id, data)

    # insert config parms features    
    
    for tw in list_tws:     
        if check_extract_features(file_id, tw, conf):
            insert_config_features(file_id, tw, conf)

        else: 
            # removes the time window value from the list, 
            # if features with the same settings are already extracted
            list_tws.remove(tw)             
            if not list_tws:
                return {'200': 'ok'}
         
        
        
    for day in days:


        for window_size in tqdm(iterable=list_tws, desc='time window-day'+str(day)):
            grouped = data.groupby(by=pd.Grouper(key='Timestamp', freq=str(
                int(window_size * 60)) + 's'))  # make groups according to the time window
            
            Parallel(n_jobs=-1)(delayed(window_features)(i, grouped.get_group(i), day, window_size, port_list_src, port_list_dst, conf, file_id)
                                for i in tqdm(desc='selected time window ' + str(window_size) + ' min', iterable=grouped.indices.keys()))  # performs in parallel the analysis of the various time windows
    
            gc.collect()           
    return {'200': 'ok'}
    '''
    except Exception as e:
        print('ERROR: ' +str(e))
    '''
