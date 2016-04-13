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
        data_name = str(file_name) + str(i) + '.txt'
        print 'data_name -> ', data_name
        oc.get_file('Mapreduce/' + data_name, 'data/' + data_name)
    
    return 0

def call_get_data(json_file='src/helping_functions/own_cloud.json'):
    """
        Load the json data and pass is to get_data
        num_list parameter will be transmited by the ssh
    """
    json_data = open(json_file).read()
    print 'Data -> ', json_data
    data = json.loads(json_data)

    get_data(data['username'], data['password'], data['file_name'], data['num_list'])
    
    return (data['num_list'], data['file_name'])
