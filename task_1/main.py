import requests
import yaml
import json

from pathlib import Path

def load_config(config_path):
    with open(config_path, mode='rt') as yaml_file:
        config = yaml.safe_load(yaml_file)

    return config

def save_result(config, date_of_load, json_data):

    output_dir = config['output_dir']
    path_for_data_file = output_dir + '/' + date_of_load
    
    # check output dir
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    Path(path_for_data_file).mkdir(parents=True, exist_ok=True)

    with open(path_for_data_file + '/data.json', 'w') as outfile:
        json.dump(json_data, outfile)

def app(config, date_of_load):
    resource = config['url'] + '/auth'
    headers = { 'content-type': 'application/json' }
    data = {"username": config['auth']['username'], "password": config['auth']['password']}

    result = requests.post(resource, data=json.dumps(data), headers=headers)

    result_json = result.json()
    token = "JWT " + result_json['access_token']

    resource = config['url'] + '/out_of_stock'

    payload = {"date": date_of_load}
    headers = { 'content-type': 'application/json', 'Authorization': token}

    result = requests.get(resource, data=json.dumps(payload), headers=headers)

    save_result(config, date_of_load, result.json())


if __name__ == '__main__':

    date_of_load = "2021-04-01"
    
    config = load_config("config.yaml")
    app(config, date_of_load)

    
