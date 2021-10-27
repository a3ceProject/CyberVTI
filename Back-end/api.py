from flask import Flask, request, render_template, Response
from flask_sslify import SSLify
import json
from Algorithms.features_extract import exec_features_extract, open_file, get_config_id

from Algorithms.load_file import *
from Algorithms.clustering_process import exec_clustering_fast
from Algorithms.metrics import metrics_request
from DataBase.cluster_db import *
from DataBase.files_db import *
from DataBase.features_db import *
from DataBase.user_db import *


app = Flask(__name__)

#sslify = SSLify(app)
#url = 'http://127.0.0.1:5000'


@app.route("/")
def index():
    return "<h1> Running flask App </h1>"

#    FILES REQUESTS
@app.route("/api/files", methods=['GET', 'POST'])
def get_post_files():

    if request.method == 'GET':
        j = request.get_json()    
        return {"Files": get_files_name(j['user'])}
    if request.method == 'POST':
        print('upload file request')
        user = request.form.get('user')
        freq = request.form.get('freq').split(", ")
        file_in = request.files['file']
        date_format = request.form.get('date_format')
        ip_to_match = request.form.get('ip_to_match')
        unidirectional_flow =  request.form.get('unidirectional_flow')
        
        default_Tws = request.form.get('default_tw').split(" ")
        methods = request.form.get('default_method')
        
        #breakpoint()
        ret = upload_file(user, file_in, freq, date_format)
        
        file_id = get_file_id(file_in.filename, user)
        
        if ret:
            # Extract default features
            date = get_day_start_end_date(file_in.filename, user)

            for m in methods:
                default_config = [date_format, m, 100, ip_to_match,  [], [
                    80, 194, 25, 22], [80, 194, 25, 22, 6667], unidirectional_flow]

                exec_features_extract(file_id,
                                      default_Tws, default_config)
                for tw in default_Tws:
                    config_id = get_config_id(file_id, tw, m, 100, ip_to_match,
                                              [[], [80, 194, 25, 22], [80, 194, 25, 22, 6667]], 1)

                    exec_clustering_fast(date[1].strftime('%Y-%m-%d %H:%M:%S'), date[2].strftime('%Y-%m-%d %H:%M:%S'), tw,
                                         file_id, ip_to_match, [23, 3, -1], config_id)

                    #cluster_and_models_put_file_id(file_id)
            return {'200': 'ok'}
        else:
            return {"400": "Bad request"}


@app.route("/api/files/<file_name>", methods=['GET'])
def returnsInfoFilieJSON(file_name):
    j = request.get_json()
    file_id = get_file_id(file_name,j['user'])
    return {"File Info": get_file_start_end_date(file_id)}


@app.route("/api/files/<file_name>", methods=['DELETE'])
def deletefile(file_name):
    j = request.get_json()
    file_id = get_file_id(file_name,j['user'])
    if delete_file(file_id):
        if remove_all_features_from_a_file(file_id, j['user'], file_name):
            if remove_all_clusters_from_a_file(file_id):    
                return {'200': 'ok'}
    else:            
        return {'400': 'Bad Request'}


@app.route("/api/files/<file_name>/file_activity", methods=['GET'])
def returnsActivity(file_name):
    j = request.get_json()   
    file_id = get_file_id(file_name,j['user'])
    return {"File_activity": get_view_activity(file_id, j['time'])}

@app.route("/api/files/<file_name>/conf_time_windows", methods=['GET'])
def returnsTimeWindows(file_name):
    j = request.get_json() 
    file_id = get_file_id(file_name,j['user'])
    return {"TimeWindows": get_dist_tw_on_config(file_id)}

@app.route("/api/file/<file_name>/results", methods=['GET'])
def returnsConfigs(file_name):
    r = request.get_json()
    file_id = get_file_id(file_name, r['user'])
    

    return {'Configs': get_config_features(file_id)}

# Results request
@app.route("/api/features/results",  methods=['GET'])
def return_config_id():
    r = request.get_json()
    file_id = get_file_id(r['file_name'], r['user'])
    return {"Config_id": get_config_id(file_id, r['tw'], r['method'], r['nr_ports'],
                                       r['ip_to_match'], r['ports_lst'], r['std_ports'])}

# FEATURES REQUEST
@app.route("/api/features/extract", methods=['POST'])
def extractFeatures():
    ret = ""
    j = request.get_json()
    file_id = get_file_id(j['file_name'], j['user'])
    conf = [j['DATE_FORMAT'], j['method'], j['nr_ports'], j['IP_TO_MATCH'],
            j['PORT_DAY'], j['OUTGENE_PORTS'], j['FLOWHACKER_PORTS'], j['unidirectional_flow']]
    
    ret = exec_features_extract(
        file_id, j['TimeWindows'], conf)

    if ret:
        return {'200': 'ok'}
    else:
        return {"400 Bad Request"}

@app.route("/api/clusters/heatmap", methods=['GET'])
def returnsHeatmapData():
    file_name = request.args.get('file_name')
    j = request.get_json()    
    file_id = get_file_id(file_name,j['user'])
    data_heatmap = get_cluster_to_heatmap(file_id, j['config_id'], j['timestamp'], j['view'])
    
    return data_heatmap


@app.route("/api/clusters/results", methods=['GET'])
def returnsClustersTimeWindows():
    file_name = request.args.get('file_name')
    config_id = request.args.get('result_id')
    j = request.get_json()    
    file_id = get_file_id(file_name,j['user'])
    return {"Results": json.loads(get_results_from_clusters(file_id,
                                                            config_id, j['view'], j['n_max']))}

@app.route("/api/clusters/results/all", methods=['GET'])
def returnsClusters():    
    file_name = request.args.get('file_name')
    config_id = request.args.get('result_id')
    timestamp = request.args.get('timestamp')
    
    r = request.get_json()    
    file_id = get_file_id(file_name, r['user'])

    ret = get_clusters_by_time(r['view'], timestamp, config_id,
                               file_id).to_json()
    
    return {"Results": json.loads(ret)}


@app.route("/api/clusters/timestamps/", methods=['GET'])
def returns_ts():
    file_name = request.args.get('file_name')
    config_id = request.args.get('result_id')
    view = request.args.get('view')
    r = request.get_json()   
    file_id = get_file_id(file_name, r['user'])

    return {'Timestamps': get_clusters_ts_with_view(file_id, config_id, view)}


@app.route("/api/clusters/fast", methods=['POST'])
def extract_clusters_fast():
    ret = ""
    r = request.get_json()
    conf = [r['k_max'], r['k_min'], r['k']]
    file_id = get_file_id(r['file_name'], r['user'])

    ret = exec_clustering_fast(
        r['time_start'], r['time_end'], r['time_window'], file_id, r['ip_view'], conf, r['conf_id'])
    #cluster_and_models_put_file_id(file_id)
    
    if ret:
        return {'200': 'ok'}
    else:
        return "409 Conflict"


@app.route("/api/history", methods=['POST'])
def extract_history():
    ret = ""
    j = request.get_json()
    ret = main_history(j['time_start'], j['time_end'],
                       j['time_window'], j['file_id'], j['config_id'], j['compar_mode'])
    #ret = exec_history(j['treshold'], j['compar_mode'], j['time_start'], j['time_end'], j['time_window'], j['file_id'])
    
    if ret:
        return {'200': 'ok'}
    else:
        return "409 Conflict"



# GET metrics
@app.route("/api/metrics", methods=['GET'])
def return_metrics():
    file_name = request.args.get('file_name')
    config_id = request.args.get('result_id')
    r = request.get_json()
   
    
    return metrics_request(r['white_list'], r['red_list_dict'], config_id,
                           file_name, r['user'], r['view'])


# Insert user
@app.route("/api/users", methods=['POST'])
def put_user():    
    r = request.get_json()   
    ret = insert_user(r['username'])
    
    if ret:
        return Response("{'a':'b'}", status=200, mimetype='application/json')
    else:
        return Response("{'Error':'User already exists'}", status=400, mimetype='application/json')

@app.route("/api/users", methods=['GET'])
def get_user_id():
    r = request.get_json()   
    ret = get_user_id_by_username(r['username'])
    
    if ret:
        
        return Response(json.dumps({'user_id': ret}), status=200, mimetype='application/json')
    else:
        return Response("{'Error':'User not exists'}", status=400, mimetype='application/json')


if __name__ == '__main__':
    # app.run()
    #app.run(host='127.0.0.1', port=5000, ssl_context='adhoc')
    app.run(host='127.0.0.1', port=5000, debug=True)

