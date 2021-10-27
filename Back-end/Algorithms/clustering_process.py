'''
Script clustering aplication (k-means)
more inf: python3 clustering_process --help
'''

import os, json, time, gc
import warnings
from os import listdir
from sys import argv, exit, version_info
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import plotly
import plotly.graph_objs as go
from joblib import Parallel, delayed, dump  # , load
from numpy import sqrt, linspace, asarray
from pathlib import Path
from pathlib import Path
from progress.bar import Bar
from sklearn import preprocessing
from sklearn.cluster import KMeans
from kneed import KneeLocator
from tqdm import tqdm
from datetime import datetime, timedelta
from DataBase.features_db import *
from DataBase.cluster_db import *


warnings.filterwarnings("ignore")  # remove warnings

def model_train(data, k):
    '''
    creates and trains a Kmeans clustering model

    :param data: int training data
    :type data: dataframe
    :param k : number of clusters
    :type k: int
    :return: model and value of inertia
    :rtype: float
    '''
    #print('k -------> ' + str(k))    
    warnings.filterwarnings("ignore")  # remove warnings
    kmeanModel = KMeans(n_clusters=k, max_iter=1000,
                        random_state=0,  tol=1e-5).fit(data)  # 42
    return kmeanModel, kmeanModel.inertia_


def elbow_method(data, k_flag= -1):
    '''
   Apply the elbow_method to calculate the optimal cluster number to be used in K means
    As a cost measure it uses the sum of the square distances of the samples to the nearest cluster centre.
    Produces a graph of the method

    :param data : data
    :type data: dataframe
    :param k_max : max number of clusters
    :type k_max: int
    :param k_min: min number of clusters
    :type K_min: int
    :retun: optimal model
    :rtype: KMeans
    '''
    if k_flag == -1:
        K = range(k_min, k_max)
        print("WITH RANGE!")
    else: 
        print("NO    -----      RANGE!")
        k_min = 2
        K= range(2, k_flag)
    K_means, distortions = zip(*Parallel(n_jobs=-1)(delayed(model_train)(data, k) for k in Bar(
        'elbow method', suffix='%(percent)d%%').iter(K)))  # realiza em parealelo o treino dos varios modelos
    kn = KneeLocator(K, distortions, curve='convex', direction='decreasing')

    best_k = kn.knee  # obtains a value of k for which less distortion is obtained
    # obtains the classification that gives rise to the least distortion
    kmeanModel = K_means[best_k-k_min]
    plt.figure(figsize=(16, 8))
    plt.plot(K, distortions, 'bx-')
    plt.xlabel('k')
    plt.ylabel('Distortion')
    plt.title('The Elbow method method for best k ('+str(best_k)+')')
    plt.show()

    return kmeanModel, plt


def optimal_number_of_clusters(wcss):
    x1, y1 = 2, wcss[0]
    x2, y2 = 30, wcss[len(wcss)-1]
    distances = []
    for i in range(len(wcss)):
        x0 = i+2
        y0 = wcss[i]
        numerator = abs((y2-y1)*x0 - (x2-x1)*y0 + x2*y1 - y2*x1)
        denominator = np.sqrt((y2-y1)**2 + (x2-x1)**2)
        distances.append(numerator/denominator)
    return distances.index(max(distances)) + 2


def elbow_method_fast(data, k_max, k_min,  k_flag= -1):
    '''
    [fast version]
    Apply the elbow_method to calculate the optimal cluster number to be used in Kmeans
    As a cost measure it uses the sum of the square distances of the samples to the nearest cluster centre.

    :param data : data
    :type data: dataframe
    :param k_max : max number of clusters
    :type k_max: int
    :param k_min: min number of clusters
    :type K_min: int
    :retun: optimal model
    :rtype: KMeans
    '''    
    K = range(k_min, k_max)

    
    K_means, distortions = zip(*[model_train(data, x) for x in K])
    kn = KneeLocator(K, distortions, curve='convex', direction='decreasing')
    best_k = kn.knee  # obtains a value of k for which less distortion is obtained

    # obtains the classification that gives rise to the least distortion
    kmeanModel = K_means[best_k-k_min]
           
    return kmeanModel
    

def result_analysis_fast(view, clusters, time, data, conf_id, file_id):
    '''
    [fast version]
    Funcao, for analysis of results, saves results
    Produces and stores:
        -csv with entity vs cluster relationship assigned
        -save classifier with joblib
    :param plt: graph elbow method (math.plot)
    :param clusters: classifier
    :type clusters: KMeans
    :param file: file name
    :type file: str
    :param data: data
    :type data: dataframe
    '''

    data_cluster = pd.DataFrame(
        {'ip': data.index,  'cluster_number': clusters.predict(data),
         'c_timestamp': time, 'view': view, 'file_id': file_id, 'config_id': conf_id})
    data_cluster = data_cluster.sort_values('cluster_number')  


    #insert clusters data 
    insert_all_clusters(data_cluster, data)
   
        
   
def fast(time, time_window, file_id, ip_view, conf_k, conf_id):
    '''
  [fast version]
    Processes a file/time window in order to obtain the cluster and the classifier
    :param file: file name 
    :type file: str

    '''
    try:        
        data = get_data_features_to_df(time, conf_id)
       

    except:     
        print('Error on get features')

    int_df = data[data.index.str.match(ip_view)]  # selct internal IP
    ext_df = data[~data.index.str.match(ip_view)]  # select external ip
    int_df = int_df.loc[:, (int_df != 0).any(axis=0)]  # delete zero columns
    ext_df = ext_df.loc[:, (ext_df != 0).any(axis=0)]  # delete zero columns
    int_df.index.name = None
    ext_df.index.name = None
    
          
    if len(ext_df.index) >= 30:  # consider cases with more than 30 elements
        min_max_scaler = preprocessing.MinMaxScaler()  # normalization
        ext_df = pd.DataFrame(min_max_scaler.fit_transform(
            ext_df), index=ext_df.index, columns=ext_df.columns)
        clusters_ext = elbow_method_fast(ext_df, conf_k[0], conf_k[1])
        result_analysis_fast(
            view='Ext', clusters=clusters_ext, time=time, data=ext_df, conf_id=conf_id, file_id=file_id)

    else:
        pass
        
    if len(int_df.index) >= 30:  # consider cases with more than 30 elements
        min_max_scaler = preprocessing.MinMaxScaler()  # normalization
        int_df = pd.DataFrame(min_max_scaler.fit_transform(
            int_df), index=int_df.index, columns=int_df.columns)
        clusters_int = elbow_method_fast(int_df, conf_k[0], conf_k[1])
        result_analysis_fast(
            view='Int', clusters=clusters_int, time=time, data=int_df, conf_id=conf_id,file_id=file_id)
    else:
        pass




def exec_clustering_fast(time_start, time_end, time_window, file_id, ip_view, list_conf, config_id):
    '''
    fast mode, produces only the list of clusters and classifiers
    '''
    start = time.time() 
    file_name = get_file_name(file_id)   
    if file_id <= 0:
        return {'404': 'File not found'}

    dates = get_features_times(time_start, time_end,file_id, config_id) 
    
    # check if clusters for that parms already are done  
    if check_cluster_exists(file_id, config_id):
        pass
    else:
        Parallel(n_jobs=-1)(delayed(fast)(date, time_window, file_id, ip_view, list_conf, config_id)
                            for date in tqdm(iterable=dates, desc='loading clustering'))
        gc.collect()  
    end = time.time()
    
    return {'200': 'ok'}
