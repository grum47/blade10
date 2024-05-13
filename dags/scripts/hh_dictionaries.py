import pandas as pd
import json


def get_list_dictionaries_name(context):
    list_dictionaries_name = []
    data = json.loads(context)
    for key in list(data.keys()):
        list_dictionaries_name.append(key)
    return list_dictionaries_name


def transform_data_dictionaries(data):
    data = json.loads(data)
    for key in list(data.keys()):
        df = pd.json_normalize(data[f'{key}'])
    return df