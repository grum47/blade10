import os
import json
import time
import random
import logging
import requests

from clickhouse_driver import Client

# from airflow.models import Variable
from airflow.hooks.base import BaseHook


# logging
log = logging.getLogger(__name__)


def get_conn_clickhouse(conn_name: str):
    # Get Clickhouse connection
    conn = BaseHook.get_connection(conn_name)
    conn_clickhouse = {}

    conn_clickhouse['host'] = conn.host
    conn_clickhouse['user'] = conn.login
    conn_clickhouse['password'] = conn.password
    
    return conn_clickhouse


# Функция для выполнения запросов в Clickhouse
def clickhouse_execute_query(host: str, user: str, password: str, database: str, query: str):
    client = Client(host=host, user=user, password=password, database=database)
    result = client.execute(query)
    log.info(f" ::: query: {query} ----->: success")
    
    return result

def get_data_hh_to_kafka_topic(headers: str, url:str, endpoint: str, params=None):
    headers_json = json.loads(headers)
    if params:
        response = requests.get(url + endpoint, headers=headers_json, params=params)
    else:
        response = requests.get(url + endpoint, headers=headers_json)
    data = response.text
    data = json.loads(response.text)

    yield ("message", str(data))


def check_folder_exists(path_folder):
    """Проверяем наличие папки, если нету, создаем

    Args:
        path_folder (string): Путь к папке
    """
    if not os.path.exists(path_folder):
        os.makedirs(path_folder)
    else:
        print(f"Folder {path_folder} is exists")


# vacancies
def get_url_from_data(
        data_str: str,
        host: str,
        user: str,
        password: str,
        database: str
        ):
    """Функция проходит по данных из АПИ и получает URL отдельных вакансий

    Args:
        data_str (str): Текст, полученный по АПИ
        host (str): Хост Clickhouse
        user (str): Логин пользователя
        password (str): Пароль
        database (str): Схема
    """
    
    data = json.loads(data_str)
    for vacancie in data['items']:
        vacancie_url = vacancie['url']

        query = f"Insert into blade_stage.tmp_vacancies_url (url) values ('{vacancie_url}');"
        clickhouse_execute_query(host, user, password, database, query)

# Функция выборки данных по вакансиям
def get_url_vacancies_to_clickhouse(
        base_url: str,
        endpoint: str,
        headers: str,
        area: str,
        search_text: str,
        search_date: str,
        host: str,
        user: str,
        password: str,
        database: str
        ):
    url = base_url + endpoint
    headers = json.loads(headers)
    area = area[0]

    # Создаем таблицу для урлов
    query = """CREATE TABLE IF NOT EXISTS blade_stage.tmp_vacancies_url
            (
                `url` String,
                `process_dttm` DateTime('UTC')
            )
            ENGINE = MergeTree
            ORDER BY process_dttm
            SETTINGS index_granularity = 8192;"""
    clickhouse_execute_query(host, user, password, database, query)

    params = {
        'area': int(area),
        'date_from': search_date,
        'date_to': search_date,
        'per_page': 100,  # не более 100 элементов на страницу
        'text': search_text,
        'search_field': 'name',
    }
    print(f" ::: params: {params}")

    response = requests.get(url, headers=headers, params=params) # type: ignore
    try:
        data = json.loads(response.text)
        print(f" ::: area: {area}, data.found: {data['found']}, date.pages: {data['pages']}, data.items: {len(data['items'])}")
        
        # TODO: Написать логику обработки когда более 20 страниц (разбивать по городам)
        if data['found'] != 0:
            if data['pages'] > 1:
                for page in range(1, data['pages'] + 1, 1):
                    print(f" ::: page process: {page}")
                    params['page'] = page
                    
                    response = requests.get(url, headers=headers, params=params) # type: ignore
                    get_url_from_data(response.text, host, user, password, database)
                    time.sleep(random.randint(33, 35) / 100)
            else:
                get_url_from_data(response.text, host, user, password, database)
    except Exception as e:
        log.error(e)
    return True


def get_data_vacancies_func(
        headers: str,
        conn_name: str
        ):
    
    headers_json = json.loads(headers)
    conn_clickhouse = get_conn_clickhouse(conn_name)
    host = conn_clickhouse['host']
    user = conn_clickhouse['user']
    password = conn_clickhouse['password']
    database = "blade_stage"

    # идем в базу смотрим сколько вакансий у нас - для цикла
    query = """SELECT  count(*) from blade_stage.tmp_vacancies_url;"""
    cnt_rows = clickhouse_execute_query(host, user, password, database, query)[0][0]
    log.info(f" ::: cnt_rows ----->: {cnt_rows}")

    for url in range(1, cnt_rows + 1):
        # идем в базу, берем урл
        query = """SELECT url from blade_stage.tmp_vacancies_url limit 1;"""
        url = clickhouse_execute_query(host, user, password, database, query)
        log.info(f" ::: url ----->: {url}")
        if url:
            url = url[0][0]
            vacancie_response = requests.get(url, headers=headers_json)
            vacancie_data = json.loads(vacancie_response.text)
            vacancie_response.close()
            time.sleep(random.randint(33, 35) / 100)

            query = f"""ALTER TABLE blade_stage.tmp_vacancies_url DELETE WHERE url = '{url}';"""
            clickhouse_execute_query(host, user, password, database, query)

            yield ("message", str(vacancie_data))
        else:
            break
