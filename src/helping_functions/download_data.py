"""
    Contains fuctions related to the own cloud
"""

import owncloud
import json

def get_data(username, password, file_name, list_num, client="http://varuna.aero.iitb.ac.in/docs/"):
    """
        all parameters will be taken from json file will be taken from json file
        file_name and list_num together form the whole file 
    """
    oc = owncloud.Client(client)
    oc.login(username, password)

    for i in list_num:
        data_name = file_name + list_num + '.txt'
        oc.get_file('Mapreduce/%s'%(data_name), 'data/%s'%(data_name))
    
    return 0

def call_get_data(json_file='own_cloud.json'):
    """
        Load the json data and pass is to get_data
        num_list parameter will be transmited by the ssh
    """
    json_data = open(json_file).read()
    data = json.load(json_data)

    get_data(data['username'], data['password'], data['file_name'], data['num_list'])
    
    return 0
