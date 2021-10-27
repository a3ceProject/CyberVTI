import configparser
import os
import tkinter as tk


def update_cfg(section, key, value):
    #Update config using section key and the value to change
    #call this when you want to update a value in configuation file
    # with some changes you can save many values in many sections
    config.set(section, key, value )
    with open('conf.cfg', 'w') as output:
        config.write(output)

def init():
    'Create a configuration file if does not exist'
    config.add_section('Style')
    config.set('Style', 'width','1200')
    config.set('Style', 'height', '600')
    config.set('Style', 'color', )

    
    with open('conf.cfg', 'w') as output:
        config.write(output)

config = configparser.RawConfigParser()

# check if conf.cfg exist if not create it   
if not os.path.exists('conf.cfg'):
    init()
    root = tk.Tk()

    update_cfg('Style', 'screen_width', root.winfo_screenwidth())
    update_cfg('Style', 'screen_height', root.winfo_screenheight())

config.read('conf.cfg')


def get_int_cfg(section, key):
    return int(config[section][key])

def get_list_cfg(section, key):
    ret_list = []    
    conf = config.get(section, key)
    conf.replace('\'','')    
    #print(conf)
    if conf == '' or conf == '[]':
        return []
    elif len(conf) == 1 or len(conf)==0:
        return conf
    else:
        aux = conf.split(', ')
        for f in aux:
            ret_list.append(int(f))
        return ret_list

def get_conf_geo():
    w = int(config['Style']['width'])
    h = int(config['Style']['height'])
    ws = int(config['Style']['screen_width'])
    hs =  int(config['Style']['screen_height'])

    # calculate x and y coordinates for the Tk window
    ws = (ws/2) - (w/2)
    hs = (hs/2) - (h/2)
    
    return w,h,ws,hs

def get_time_window_graph():
     return str(config['GraphWindow']['time_window'])


def save_cfg_file(dict_settings):
    for section in dict_settings:
        for i, dict_key in enumerate(dict_settings[section]['key']):        
            config.set(section,dict_key, dict_settings[section]['value'][i])

    with open('conf.cfg', 'w') as f:
        config.write(f)
   
def get_conf_std():
    method = int(config['File_parms']['default_method'])
    n_ports = int(config['Extract_features']['n_ports'])
    ip_to_match = config['Extract_features']['ip_to_match']
    port_list = [ get_list_cfg('Extract_features', 'port_list'), 
        get_list_cfg('Extract_features', 'outgene_ports'),
        get_list_cfg('Extract_features', 'flowhacker_ports') ]

    return [method, n_ports, ip_to_match, port_list]

def get_ips_list_to_str(section, key):    
        cp = configparser.ConfigParser(converters={'list': lambda x: [i.strip() for i in x.split(',')]})
        cp.read('conf.cfg')
       

        return  cp.getlist(section, key)
 