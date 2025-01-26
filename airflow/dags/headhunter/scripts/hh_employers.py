import pandas as pd
import numpy as np
import requests
import json
import time
import os
import random
# from tqdm import tqdm

from airflow.models import Variable


def split_list(**kwargs):
    lst = kwargs['return_value']
    lst = list(lst[:-1])
    split_list = [lst[i:i+500] for i in range(0, len(lst), 500)]
    for part in range(len(split_list)):
        Variable.set(key=f"HH_LISTS_EMPLOYERS_ID_{part}", value=split_list[part])
        last_part = part
    Variable.set(key=f"HH_LISTS_EMPLOYERS_ID_LAST_PART", value=last_part)
    return last_part


def transform_employer_data(data):
    id_list = []
    name_list = []
    type_list = []
    site_url_list = []
    vacancies_url_list = []
    area_id_list = []
    open_vacancies_list = []

    def extract_data(data):
        id_list.append(data.get('id', None))
        name_list.append(data.get('name', None))
        type_list.append(data.get('type', None))
        site_url_list.append(data.get('site_url', None))
        vacancies_url_list.append(data.get('vacancies_url', None))
        area_id_list.append(data.get('area').get('id') if data.get('area') else None)
        open_vacancies_list.append(data.get('open_vacancies', None))

    extract_data(data)

    df = pd.DataFrame({
            'id': id_list,
            'name': name_list,
            'type': type_list,
            'site_url': site_url_list,
            'vacancies_url': vacancies_url_list,
            'area_id': area_id_list,
            'open_vacancies': open_vacancies_list
        })

    employer_path = '/opt/airflow/tmp_stage/employers.csv'
    df.to_csv(employer_path, mode='a', header=not os.path.exists(employer_path), index=False, sep=';')
    # time.sleep(random.randint(45, 60) / 100)
    return True


def get_employers(*args, **kwargs):
    url = kwargs['url'],
    url = url[0]
    headers = json.loads(kwargs['headers'])
    employers_id = args[0]
    url_employer = url + "/" + str(int(employers_id))
    print(f" ::: processed url :{url_employer}")
    response = requests.get(url_employer, headers=headers)
    data = json.loads(response.text)
    transform_employer_data(data)
    return True



if __name__ == "__main__":
    pass