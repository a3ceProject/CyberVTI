from sqlalchemy import create_engine, Column, Float, Integer, String, ForeignKey, delete, PickleType, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.orm import scoped_session, sessionmaker
from os import path
from datetime import datetime, timedelta
from sqlalchemy.sql import func, text
import time
import pickle
from DataBase.files_db import get_file_id, get_file_name, get_user_file
import os, shutil
from shutil import rmtree
import pandas as pd
from os import listdir
from os.path import isfile, join


import sqlalchemy
from sqlalchemy import update

DATABASE_FILE = "database.sqlite"


engine = create_engine('sqlite:///%s' % (DATABASE_FILE), echo=False)
conn = engine.connect()

Base = declarative_base()

class Features_Metadata(Base):
    __tablename__ = 'Features_Metadata'
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer)
    time_window = Column(Integer)
    method = Column(Integer)
    nr_ports = Column(Integer)
    ip_to_match = Column(String)
    port_day = Column(PickleType)
    outgene_ports = Column(PickleType)
    flowhacker_ports = Column(PickleType)
    standard_ports = Column(Integer)

    def to_dictionary(self):
        return {'config_id': self.id, "file_id": self.file_id, 'time_window': self.time_window,
                "method": self.method, "nr_ports": self.nr_ports, "ip_to_match": self.ip_to_match, "port_day": self.port_day,
                "outgene_ports": self.outgene_ports, "flowhacker_ports": self.flowhacker_ports,
                "standard_ports": self.standard_ports}


Base.metadata.create_all(engine)  # Create tables for the data models

Session = sessionmaker(bind=engine)
session = scoped_session(Session)


def delete_features_tables():

    conn.execute("DROP TABLE Features_Metadata;")
    print('DROP --- Features_Metadata')
    session.commit()
    Base.metadata.create_all(engine)
    session.commit()
    shutil.rmtree('DataBase/Features') 
   
    print('db restart')


def insert_fixed_features(data, dir_path, timestamp):          
    data.to_parquet(dir_path +str(timestamp) +'_features.parquet')   
    
   
def check_path_and_create(file_dir, config_id, user_id):
    dir_path = os.path.dirname(os.path.abspath(__file__))+'/Features/'
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    if not os.path.exists(dir_path+'user_'+str(user_id)):
        os.makedirs(dir_path+'user_'+str(user_id))
        dir_path = dir_path+'user_'+str(user_id)+'/'
    else:
        dir_path = dir_path+ 'user_'+str(user_id)+ '/'   
                    
    if not os.path.exists(dir_path+file_dir):
        os.makedirs(dir_path+file_dir)
        dir_path = dir_path+file_dir+'/'
    else:
        dir_path = dir_path+file_dir+'/'

    if not os.path.exists(dir_path+'config_'+str(config_id)):
            os.makedirs(dir_path+'config_'+str(config_id))
            dir_path = dir_path+'config_'+str(config_id)+'/'
            
    
    dir_path = 'DataBase/Features/user_'+str(user_id)+ '/' + file_dir + '/config_' + str(config_id)+ '/'
    
    return dir_path



def get_features_times(t_start, t_end, file_id, config_id):       
    dates = []
    file_name = get_file_name(file_id)
    user_id = get_user_file(file_id)
    file_dir = file_name[:len(file_name)-4]
    t_start = datetime.strptime(t_start, "%Y-%m-%d %H:%M:%S")
    t_end = datetime.strptime(t_end, "%Y-%m-%d %H:%M:%S")
    features_path = 'DataBase/Features/user_'+str(user_id) +'/'+file_dir+'/config_'+str(config_id)

    for f in  listdir(features_path):
        if isfile(join(features_path, f)):
            #remove '_features.parquet'
            f_time_str = f[:len(f)-17]
            f_time =  datetime.strptime(f_time_str, "%Y-%m-%d %H:%M:%S")
            if f_time >= t_start and f_time<= t_end:

                dates.append(f_time_str)
   
    return sorted(dates)

def get_data_features_to_df(timestamp, config_id):
    start = time.time()
    confs = get_conf_by_id(config_id)
    file_name = get_file_name(confs['file_id'])
    file_dir = file_name[:len(file_name)-4]
    user = get_user_file(confs['file_id'])
    res = pd.read_parquet('DataBase/Features/user_'+str(user) + '/'+file_dir+'/config_'+str(config_id)+'/' +timestamp+'_features.parquet')
    res.index.name = 'ip'
    end = time.time()
    print('Getting Features: ' +str(round(end-start,4)) + ' sec') 
    
    return res


def get_config_id(file_id, tw, method, nr_ports, ip_to_match, ports_lst, std_ports):
    
    result = session.query(Features_Metadata).filter(
        Features_Metadata.file_id == file_id, Features_Metadata.time_window == tw,
        Features_Metadata.method == method, Features_Metadata.nr_ports == nr_ports,
        Features_Metadata.ip_to_match == ip_to_match).all()
    session.close()

    if std_ports == 1:        
        return int(result[0].to_dictionary()['config_id'])
    else:
        for res in result:
            r = res.to_dictionary()
            aux_port_day = ports_lst[0]
            aux_outgene = ports_lst[1]
            aux_flowhacker = ports_lst[2]
            if sorted(aux_port_day) == sorted(ports_lst[0]) and sorted(aux_outgene) == sorted(ports_lst[1]) and sorted(aux_flowhacker) == sorted(ports_lst[2]):
                return r['config_id']
        print('ERROR - No config_id found!')
        return None



def insert_config_features(file_id, time_window, conf):
    method, nr_ports, ip_to_match, port_day, outgene_ports, flowhacker_ports = conf[
        1], conf[2], conf[3], conf[4], conf[5], conf[6]
    # Check if ports lists are standard
    if conf[4] == [] and conf[5] == [80, 194, 25, 22] and conf[6] == [80, 194, 25, 22, 6667]:
        std_ports = 1
    else:
        std_ports = 0
    object_to_add = Features_Metadata(file_id=file_id, time_window=time_window, method=method, nr_ports=nr_ports,
                                    ip_to_match=ip_to_match, port_day=pickle.dumps(
                                        port_day), outgene_ports=pickle.dumps(outgene_ports),
                                    flowhacker_ports=pickle.dumps(
                                        flowhacker_ports),
                                    standard_ports=std_ports)

    session.add(object_to_add)
    session.commit()
    session.close()


def get_config_features(id_file):
    ret = []
    result = session.query(Features_Metadata).filter(
        Features_Metadata.file_id == id_file).all()
    for res in result:
        r = res.to_dictionary()
        aux_port_day = pickle.loads(r['port_day'])
        aux_outgene = pickle.loads(r['outgene_ports'])
        aux_flowhacker = pickle.loads(r['flowhacker_ports'])

        ret.append({'config_id': r['config_id'], 'file_id': r['file_id'], 'time_window': r['time_window'],
                    'method': r['method'], 'nr_ports': r['nr_ports'], 'ip_to_match': r['ip_to_match'], 'port_day': aux_port_day,
                    'outgene_ports': aux_outgene, 'flowhacker_ports': aux_flowhacker})
    session.close()

    return ret


def get_conf_by_id(config_id):
    result = session.query(Features_Metadata).filter(
        Features_Metadata.id == config_id).first()
    session.close()
    conf = result.to_dictionary()
    aux_port_day = pickle.loads(conf['port_day'])
    aux_outgene = pickle.loads(conf['outgene_ports'])
    aux_flowhacker = pickle.loads(conf['flowhacker_ports'])

    ret = {'file_id': conf['file_id'], 'time_window': conf['time_window'],
           'method': conf['method'], 'nr_ports': conf['nr_ports'], 'ip_to_match': conf['ip_to_match'], 'port_day': aux_port_day,
           'outgene_ports': aux_outgene, 'flowhacker_ports': aux_flowhacker}
    return ret


def get_dist_tw_on_config(file_id):
    ret = []
    result = session.query(Features_Metadata).filter(
        Features_Metadata.file_id == file_id).distinct()
    session.close()
    for r in result:
        ret.append(r.time_window)
    return ret

def remove_all_features_from_a_file(file_id, user, file_name):
    try:
        session.query(Features_Metadata).filter(Features_Metadata.file_id == file_id).delete()
        session.commit()
        session.close()

        file_dir = file_name[:len(file_name)-4]

        shutil.rmtree('DataBase/Features/user_'+str(user) + '/' +file_dir) 
        print('REMOVED:  ' +  'DataBase/Features/user_'+str(user) + '/' +file_dir)
        return True
    except:
        
        print('ERROR - removing a Features')
        return False
