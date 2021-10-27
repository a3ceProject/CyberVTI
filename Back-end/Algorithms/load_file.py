import pandas as pd
from progress.bar import IncrementalBar
from DataBase.files_db import *
import os, gc
import numpy
import csv
from datetime import datetime
import time, math


def open_file(file_in, user, date_format, uni_direct):
    try:
        file = './Data_files/user_'+str(user) +'/' + file_in

    except IndexError:
        print('Error on file name')
        return NameError    
    try:
        data = pd.read_csv(file, sep=',')
        bar = IncrementalBar('Analyzing the file', max=10,
                             suffix='%(percent)d%%')

        bar.next()
        data = data.fillna(0)  # fill in fields with NaN
        bar.next()
        # remove the duplicate header lines
        data = data[data['Src Port'].map(lambda x: str(x) != "Src Port")]
        bar.next()
        # remove timestamp-free lines
        data = data[data['Timestamp'].map(lambda x: str(x) != '0')]
        bar.next()
        # convert data types
        data['Src Port'] = data['Src Port'].astype('int')
        bar.next()
        data['Dst Port'] = data['Dst Port'].astype('int')
        bar.next()
        data['Tot Fwd Pkts'] = data['Tot Fwd Pkts'].astype('int')
        bar.next()
        data['TotLen Fwd Pkts'] = data['TotLen Fwd Pkts'].astype('float')
        bar.next() 
        if uni_direct == 1:
            data['Tot Bwd Pkts'] = 0
            bar.next()
            data['TotLen Bwd Pkts'] = 0
            bar.next()            
        else:                       
            data['Tot Bwd Pkts'] = data['Tot Bwd Pkts'].astype('int')
            bar.next()
            data['TotLen Bwd Pkts'] = data['TotLen Bwd Pkts'].astype('float')
            bar.next()
            
        # check a date format    
        if date_format == 'Integer' or date_format == 'Int':
            date_format = '%Y-%m-%d %H:%M:%S' 
                    
                
        data['Timestamp'] = pd.to_datetime(
            data['Timestamp'].tolist(), format=date_format)

        bar.finish()
    except FileNotFoundError:
        print('''
        File Not Found''')

    return data
    
    


def get_files_name(user):
    files_list = []

    if not os.path.isdir("Data_files/user_"+str(user)):
        os.mkdir("Data_files/user_"+str(user))
        return files_list
    else:    
        for file in os.listdir('./Data_files/user_'+str(user)):        
            files_list.append(file)

    return files_list


def check_file(file_name, user):    
    required_columns = ['Src IP', 'Src Port', 'Dst IP', 'Dst Port', 'Timestamp',
                        'Tot Fwd Pkts', 'TotLen Fwd Pkts', 
                        'Flow Duration', 'Protocol']
    try:
        data_columns = list(pd.read_csv(
            'Data_files/user_'+ user +'/' + file_name, nrows=1).columns)
        # check columns format
         
        for k in required_columns:
            if k not in data_columns:
                print(k)
                print('wrong columns format')
                return False
        
        return True
    except:
        print('Check File Error')
        return False


def covert_int_date_to_srt(file_path):
    start = time.time()
    #conver integer date (in ms) to srt date and store a file
    data=pd.read_csv(file_path, sep=',')
    
    data['Timestamp'] = pd.to_datetime(data.Timestamp,unit='ms')
    data['Timestamp'] = data['Timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S' ) 
 
    data.to_csv(file_path, sep=',',  index = False) 
    end = time.time()
    print('Converting csv dateformat: %s sec.' %(round(end - start, 2)))
    

def get_activity_len_ports(file_in, user, freq, date_format, nr_ports=100):
    # return file len
    start=time.time()
    try:
        filename = './Data_files/user_'+str(user) +'/' + file_in

    except IndexError:
        print('Error on file name')
        return NameError
    
    if date_format == 'Integer' or date_format == 'Int':
       date_format = '%Y-%m-%d %H:%M:%S'

    start_date = None
    end_date = None
    times = {}  
    aux_times = {} 
    i = 0
    aux_chunk = None
    
   
    # open csv by chunk
    with pd.read_csv(filename, chunksize= 10 ** 6,  usecols=['Timestamp', 'Dst Port', 'Src IP', 'Src Port'] ) as reader:
        for chunk in reader:
            i+=1   
            bar = IncrementalBar('Analyzing the file, part: ' + str(i), max=5,
                                    suffix='%(percent)d%%')
            chunk['Timestamp'] = pd.to_datetime(chunk['Timestamp'], format=date_format)
            
            bar.next()
            # get activity 
            # freq = ['30 min', '1H', '3H', '1D']
            timestamps =  chunk.drop(['Dst Port', 'Src IP', 'Src Port'], axis=1)
                        
            for fq in freq:
                aux_times[fq] = timestamps.groupby(pd.Grouper(key='Timestamp', freq=fq)).size()               
                if i == 1:
                    times[fq] = aux_times[fq]
                else:                                      
                    times[fq] = times[fq].add(aux_times[fq], fill_value=0)  
            bar.next() 
            
            # get Dyn3_x ports list      
            bar.next()        
            aux = chunk[chunk['Dst Port'] < 49151].groupby(
                    ['Dst Port']).nunique()  # dst port counting
            uniqueports = aux.sort_values(by=['Src IP'],
                                        ascending=False)  # orders from the most contacted to the least contacted ports
            uniqueports = uniqueports[
                uniqueports['Src IP'] < 10].index.tolist()  # list of Ports contacted by less than 10 IP
            bar.next()
            Dst_port =chunk[chunk['Dst Port'] < 49151]
            Src_port = chunk[chunk['Src Port'] < 49151]
            # counting the use of the ports of destination
            most_used_ports_dst = Dst_port['Dst Port'].value_counts()
            # counting the use of the ports of origin
            bar.next()
            
            most_used_ports_src = Src_port['Src Port'].value_counts()
            most_used_ports = most_used_ports_dst.append(
                most_used_ports_src)  # add counting
            most_used_ports = most_used_ports_dst.append(
                aux_chunk) # adds value from previous chuck
            most_used_ports = most_used_ports.sort_values(
                ascending=False)  # descending  orders
            aux_chunk = most_used_ports
            bar.next()
            
            # get start and end date        
            aaa = chunk['Timestamp'].sort_values().reset_index()         

            aux_start_date = aaa['Timestamp'].iloc[0]
            aux_end_date = numpy.amax(aaa['Timestamp'])
            if start_date is None:                
                start_date = aux_start_date
                end_date = aux_end_date                                  
            else:  
                if start_date > aux_start_date:
                    start_date = aux_start_date
                if end_date < aux_end_date:    
                    end_date = aux_end_date 
            #print(str(start_date), str(end_date))
            bar.finish()
            
        # get ports
        port = list(set().union(
            list(most_used_ports.head(int(nr_ports / 3)).index),  # must used ports
            list(most_used_ports[most_used_ports.index.isin(
                uniqueports)].head(int(nr_ports/3)).index),
            # less common and more used ports
            list(most_used_ports[most_used_ports.keys() < 1024].tail(
                int(nr_ports/ 3)).index)))  # less used ports and below 1024
    gc.collect()                   
    end = time.time()
    
    return [str(start_date), str(end_date), port,  times]

def upload_file(user, file, freq, date_format):
    if not os.path.isdir("Data_files/user_"+str(user)):
        os.mkdir("Data_files/user_"+str(user))

    # upload file on Server local path
    file_path = "Data_files/user_"+str(user)
    file.save(os.path.join(file_path, file.filename))

    
    if date_format == 'Integer' or date_format == 'Int':
        covert_int_date_to_srt(file_path+'/'+file.filename)

    #change_datetime_format("Data_files/" + file.filename)
    valid = check_file(file.filename, user)
    
    # file already exist on db
    file_id = get_file_id(file.filename, user) 

    if file_id != 0:
        # Remove File, features, etc from db
        delete_file(file_id)

        # delete features
        
    if valid: 
        try:       
            result_act = get_activity_len_ports(file.filename,user, freq, date_format)
            start_date, end_date = result_act[0], result_act[1]            
            ports_dyn3_x = pickle.dumps(result_act[2]) 
            time_views = pickle.dumps(result_act[3])
            

            # insert file info on file_db
            insert_file(user, file.filename, time_views, start_date, end_date,ports_dyn3_x) 
        except Exception as e:
        
            print('ERROR ' + str(e))       
              
    else:
        os.remove("Data_files/user_"+str(user)+ '/' + file.filename)

    return valid