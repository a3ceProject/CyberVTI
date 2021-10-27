from sqlalchemy import create_engine, Column, Integer, String, ForeignKey, delete, update, PickleType
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import SingletonThreadPool
import sqlalchemy
from sqlalchemy.sql import func, text
import pandas as pd
import pickle
import json
from DataBase.features_db import get_conf_by_id, get_file_id
import numpy as np


DATABASE_FILE = "database.sqlite"

engine = create_engine('sqlite:///%s' % (DATABASE_FILE), echo=False, poolclass=SingletonThreadPool, connect_args={'check_same_thread': False})
#autocommit_engine = engine.execution_options(isolation_level="AUTOCOMMIT")
#conn = engine.connect()

Base = declarative_base()


class Clusters(Base):
    __tablename__ = "Clusters"
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer)
    config_id = Column(Integer)
    view = Column(String)
    ip = Column(String)
    c_timestamp = Column(String)
    cluster_number = Column(Integer)
    data_to_heatmaps = Column(PickleType)

    def to_dictionary(self):
        return {"cluster_id": self.id, "file_id": self.file_id, "ip": self.ip, "view": self.view, 'c_timestamp': self.c_timestamp, "cluster_number": self.cluster_number,
                "config_id": self.config_id, "data_to_heatmaps": self.data_to_heatmaps}

    def to_dictionary_cluster(self):
        return {"ip": self.ip, "cluster_number": self.cluster_number}



Base.metadata.create_all(engine)  # Create tables for the data models

Session = sessionmaker(bind=engine)
session = scoped_session(Session)
Session = Session()



def insert_all_clusters(data_cluster, data_heatmap):

    data_cluster = data_cluster[~data_cluster.index.duplicated(keep='first')]
    data_h = aux_data_to_heatmap(data_heatmap)
    data = pd.merge(data_cluster, data_h, on="ip")
    #data.set_index('ip', inplace=True)
    
    data = data.to_dict(orient='records')    
    
    metadata = sqlalchemy.schema.MetaData(bind=engine)
    table = sqlalchemy.Table('Clusters', metadata, autoload=True)
    
    conn = engine.connect()
    conn.execute(table.insert(), data)
    
    session.commit()
    session.close()



def aux_data_to_heatmap(data):
    data = data[~data.index.duplicated(keep='first')]
    ret_pickle = []
    for i in data.index:
        d = data.loc[i]
        ret_pickle.append(pickle.dumps(d.to_json()))
    d = {'ip': data.index, 'data_to_heatmaps': ret_pickle}
    df = pd.DataFrame(data=d)
    return df


def delete_tables():
    conn = engine.connect()
    conn.execute("DROP TABLE Clusters;")

    print('DROP --- TABLE Clusters')


    session.commit()
    Base.metadata.create_all(engine)
    session.commit()
    print('db restart')


def drop_tables(table):
    conn = engine.connect()
    conn.execute("DROP TABLE " + table + ";")   
    session.commit()



def cluster_and_models_put_file_id(file_id):
    # put a file id on cluster & models table
    subquery = update(Clusters).where(
        Clusters.file_id == None).values(file_id=file_id)
    session.execute(subquery)
    session.commit()
    #session.close()

    subquery = update(Models).where(
        Models.file_id == None).values(file_id=file_id)
    session.execute(subquery)
    session.commit()
    session.close()


def get_clusters_by_time(view_c, time, config_id, file_id):
    ret_list = []
    
    result = session.query(Clusters).filter(Clusters.view == view_c, Clusters.config_id == config_id,
                                            Clusters.c_timestamp == time, Clusters.file_id == file_id).all()

    for row in result:
        ret_list.append(row.to_dictionary_cluster())

    ret_df = pd.DataFrame(ret_list)    
    
    ret_df.replace(to_replace=[None], value=0, inplace=True)
    ret_df.set_index('ip', inplace=True)
    session.close()
    ret_df.index.name = None

    return ret_df


def get_models_by_time(view, time, config_id, file_id):

    result = session.query(Models).filter(
        Models.m_timestamp == time, Models.view == view,
        Models.config_id == config_id, Models.file_id == file_id).all()

    data = pickle.loads(result[0].to_dictionary()['ml_model'])

    session.close()

    return data


def get_clusters_time_stamps(time_start, time_end, id_file):
    result = session.query(Clusters.c_timestamp).distinct().filter(
        Clusters.file_id == id_file).order_by(Clusters.c_timestamp).all()
    result = [id for id, in result]

    return result

def get_clusters_ts_with_view(config_id, id_file, view):
    result = session.query(Clusters.c_timestamp).distinct().filter(
        Clusters.file_id == id_file, Clusters.config_id == config_id, Clusters.view == view).order_by(Clusters.c_timestamp).all()
    result = [id for id, in result]
    return result

def get_clusters_time_stamps_with_view(time_start, time_end, config_id, id_file, view):
    result = session.query(Clusters.c_timestamp).distinct().filter(
        Clusters.file_id == id_file, Clusters.config_id == config_id, Clusters.view == view).order_by(Clusters.c_timestamp).all()
    result = [id for id, in result]
    return result


def get_models_time_stamps_with_view(time_start, time_end, config_id,  id_file, view):
    list_time_stamps = []
    result = session.query(Models.m_timestamp).distinct().filter(
        Models.file_id == id_file,Models.config_id == config_id, Models.view == view).order_by(Models.m_timestamp).all()
    result = [id for id, in result]

    return result


def get_all_cluster_to_df(time_start, time_end, config_id, file_id, view, ip_prefix):
    # retorna um df com os clusters de cada ip nas vária janelas de tempo
    cond = True

    dates = get_clusters_time_stamps_with_view(
        time_start, time_end, config_id, file_id, view)

    for date in dates:

        df = pd.read_sql(session.query(Clusters).distinct(Clusters.ip).filter(Clusters.ip.like(
            ip_prefix + '%'), Clusters.c_timestamp == date, Clusters.config_id == config_id, Clusters.file_id == file_id).statement, session.bind, index_col='ip')
        df.rename(columns={"cluster_number": date}, inplace=True)
        df.drop(columns=['view', 'id', 'file_id', 'c_timestamp'], inplace=True)

        if cond:
            res = df
        else:
            res = pd.merge(res, df, on='ip', how='outer')
        cond = False

    res.index.name = None
    
    return res


def check_cluster_exists(file_id, config_id):
    try:
        result = session.query(Clusters).filter(
            Clusters.file_id == file_id, Clusters.config_id == config_id).all()
        session.close()
        if not result:
            return False
        else:
            print('Clusters already exists')
            return True
    except:
        return False


def get_results_from_clusters(file_id, config_id, view, n_max):    
    connection = session.connection()
    sql = text('SELECT * From (SELECT DISTINCT Clusters.c_timestamp, Clusters.cluster_number, COUNT(Clusters.c_timestamp) as c FROM Clusters WHERE Clusters.file_id == '
    +str(file_id) +' and Clusters.view == \'' +view +'\' and Clusters.config_id == ' +str(config_id) + ' GROUP By Clusters.cluster_number, Clusters.c_timestamp order by Clusters.c_timestamp) WHERE c <= ' +str(n_max))
    result = connection.execute(sql)
    list_result = []
    for r in result:
        ips_list = []
        ips = session.query(Clusters.ip).filter(Clusters.file_id == file_id, Clusters.c_timestamp == r[0],
                                            Clusters.config_id == config_id, Clusters.view == view,
                                            Clusters.cluster_number == r[1]).all()
        for ip in ips:
            ips_list.append(ip[0])

        list_result.append({'timestamp': r[0], 'cluster_number': r[1], 'n_elements': r[2], 'ips': ips_list})

    session.close()       
    return json.dumps(list_result)


def get_cluster_to_heatmap(file_id, conf_id, time, view):    
    result = session.query(Clusters).filter(Clusters.file_id == file_id, Clusters.c_timestamp == time,
                                            Clusters.config_id == conf_id, Clusters.view == view).all()

    session.close()
    if not result:
        print('No result')
        return None

    ip_lst = []
    cluster_nr_lst = []
    data_heatmap = []

    for res in result:
        aux = []
        r = res.to_dictionary()
        ip_lst.append(r['ip'])
        cluster_nr_lst.append(r['cluster_number'])
        dict_heat = json.loads(r['data_to_heatmaps'])
        list_keys = dict_heat.keys()

        for key in list_keys:
            aux.append(dict_heat[key])
        data_heatmap.append(aux)
    
    data_heatmap = pd.DataFrame.from_dict(data_heatmap)
    data_heatmap.columns = list_keys

    df = pd.DataFrame(data={'ip': ip_lst, 'cluster_nr': cluster_nr_lst})

    data_heatmap = pd.concat([data_heatmap, df], axis=1)
    data_heatmap.set_index("ip", inplace=True)

    toz = data_heatmap.groupby('cluster_nr').mean()
    toz = toz.loc[:, (toz > 0.2).any(axis=0)]

    return toz.to_json(orient="split")


def remove_all_clusters_from_a_file(file_id):
    try:
        session.query(Clusters).filter(Clusters.file_id == file_id).delete()
        session.commit()
        session.close()
        return True
    except:
        
        print('ERROR - removing Clusters')
        return False