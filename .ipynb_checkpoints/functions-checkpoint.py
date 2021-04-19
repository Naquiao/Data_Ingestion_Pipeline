import logging
import os
import subprocess
import yaml
import pandas as pd
import datetime 
import gc
import re

#This file contains all the functions needed to validate our data ingestion
def read_config_file(filepath):
    with open(filepath, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            logging.error(exc)


def load_config(config_name,config_path):
    '''
    Function to load YAML config file
    '''
    with open(os.path.join(config_path,config_name)) as file:
        config = yaml.safe_load(file)
    return config

def load_data(config_name):
    data = dd.read_csv(os.path.join(config_name['data_directory'], config_name['data_name']), low_memory=False)
    print('Data Uploaded from',config_name['data_directory'],' name = ',config_name['data_name'])

def headers_validation(data,config_name):
    '''
    This function performs replacements on whitespaces and
    standarized the column names 
    '''
    data.columns = data.columns.str.lower() #Converts column names into lowercase 
    data.columns = data.columns.str.replace('[^\w]','_',regex=True) #Replace Regex and whitesapces with '_'
    data.columns = list(map(lambda x: x.strip('_'), list(data.columns)))#Remove the last strip '_'
    data.columns = list(map(lambda x: replacer(x,'_'), list(data.columns)))#Remove doble under score'__'
    expected_col = list(map(lambda x: x.lower(), config_name['columns_names']))
    expected_col.sort()
    data.columns =list(map(lambda x: x.lower(), list(data.columns)))
    data = data.reindex(sorted(data.columns), axis=1)
    if len(data.columns) == len(expected_col) and list(expected_col)  == list(data.columns):
        print("Columns headers validation passed")
        return 1
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(data.columns).difference(expected_col))
        print("Following File columns are not in the YAML file",mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(data.columns))
        print("Following YAML columns are not in the file uploaded",missing_YAML_file)
        logging.info(f'DataFrame columns: {data.columns}')
        logging.info(f'expected columns: {expected_col}')
        return 0

def memory_usage_optimization(data,config_name):
    data['time'] = data['time'].astype(config_name['data_type']['time'])
    data['user'] = data['user'].astype(config_name['data_type']['user'])
    data['content'] = data['content'].astype(config_name['data_type']['content'])
    data['task'] = data['task'].astype(config_name['data_type']['task'])
    data['answer'] = data['answer'].astype(config_name['data_type']['answer'])
    data['answer_correctly'] = data['answer_correctly'].astype(config_name['data_type']['answer_correctly'])
    data['elapsedt'] = data['elapsedt'].astype(config_name['data_type']['elapsedt'])
    data['expl'] = data['expl'].astype(config_name['data_type']['expl'])

def drop_irrelevant(data, config_name):
    data = data.drop(config_name['drop_columns'],axis=1)

def drop_missing_values(data):
    data= data.dropna()

