import requests
import json
import os
import datetime
import time
import random
import logging
import shutil

import pandas as pd

from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


log = logging.getLogger(__name__)


def execute_clickhouse(sql: str):
    from clickhouse_driver import Client
    client = Client(host='10.222.1.13', user='user', password='password', database='default')
    result = client.execute(sql)

    return result


def check_folder_exists(path_folder):
    """Проверяем наличие папки, если нету, создаем

    Args:
        path_folder (string): Путь к папке
    """
    if not os.path.exists(path_folder):
        os.makedirs(path_folder)
    else:
        print(f"Folder {path_folder} is exists")


def clear_stage_folder_func(stage_folder_path):
    try:
        shutil.rmtree(stage_folder_path)
    except Exception as e:
        log.info(e)
    return True

def get_list_of_lists_func(variable_list: str):
        simple_list = Variable.get(variable_list)
        list_of_list = [[x] for x in simple_list.split(', ')]
        return list_of_list


def set_variables_func(key_name: str, get_value: str):
    context = get_current_context()
    print(context)
    value = context[f'{get_value}']

    Variable.set(key=key_name, value=value)
    return True



def get_data_func(url, endpoint, headers, stage_folder_path):
    """Получаем данные по эндпоинту через API и сохраняем в json

    Args:
        url (str): _description_
        endpoint (str): _description_
        headers (str): _description_
        stage_folder_path (str): _description_

    Returns:
        file_path: Путь к файлу json
    """
    headers = json.loads(headers)
    response = requests.get(url + endpoint, headers=headers)
    data = json.loads(response.text)

    check_folder_exists(stage_folder_path)

    file_path = f"{stage_folder_path}{endpoint}.json"
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
        
    return file_path


# load df to PG
def load_df_to_pg(df, pg_conn_id, pg_schema, pg_table, mode='replace'):
    """Сохраняем датафрейм пандас в Постгрес

    Args:
        df (dataframe): _description_
        pg_conn_id (str): _description_
        pg_schema (str): _description_
        pg_table (str): _description_
    """

    postgres_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    postgres_engine = postgres_hook.get_sqlalchemy_engine()
    # TODO: Переписать на hook
    df.to_sql(
        name=pg_table,
        con=postgres_engine,
        schema=pg_schema,
        if_exists=mode,
        index=False
        )
    return True


# areas
def transform_data_areas_func(file_path, pg_conn_id, pg_schema, pg_table):
    """Парсим данные по странам / городам

    Args:
        file_path (_type_): _description_
        pg_conn_id (_type_): _description_
        pg_schema (_type_): _description_
        pg_table (_type_): _description_
    """
    import pandas as pd

    with open(f"{file_path}.json", 'r', encoding='utf-8') as f:
        data = json.load(f)
    id_list = []
    parent_id_list = []
    name_list = []
    areas_list = []

    def extract_data(data):
        for item in data:
            id_list.append(item.get('id', None))
            parent_id_list.append(item.get('parent_id', None))
            name_list.append(item.get('name', None))
            areas_list.append(len(item.get('areas', [])))
            if item.get('areas'):
                extract_data(item['areas'])
        return id_list, parent_id_list, name_list, areas_list
    
    id_list, parent_id_list, name_list, areas_list = extract_data(data)

    df = pd.DataFrame({
        'id': id_list,
        'parent_id': parent_id_list,
        'name': name_list,
        'areas': areas_list
    })
    df['process_dttm'] = datetime.datetime.now()
    df.drop_duplicates(inplace=True)

    load_df_to_pg(df, pg_conn_id, pg_schema, pg_table)

    return True


# metro
def transform_data_metro_func(file_path, pg_conn_id, pg_schema, pg_table):

    with open(f"{file_path}.json", 'r', encoding='utf-8') as f:
        data = json.load(f)

    id_list_lines = []
    name_list_lines =[]
    hex_color_list_lines = []
    id_list_stations = []
    name_list_stations = []
    lat_list_stations = []
    lon_list_stations = []
    order_list_stations = []

    for city in data:
        for line in city['lines']:
            for station in line['stations']:
                id_list_lines.append(line.get('id', None))
                name_list_lines.append(line.get('name', None))
                hex_color_list_lines.append(line.get('hex_color', None))
                id_list_stations.append(station.get('id', None).split('.')[1])
                name_list_stations.append(station.get('name', None))
                lat_list_stations.append(station.get('lat', None))
                lon_list_stations.append(station.get('lng', None))
                order_list_stations.append(station.get('order', None))
    
    df = pd.DataFrame({
        'line_id': id_list_lines,
        'line_name': name_list_lines,
        'line_hex_color': hex_color_list_lines,
        'station_id': id_list_stations,
        'station_name': name_list_stations,
        'station_order': order_list_stations,
        'lat': lat_list_stations,
        'lon': lon_list_stations,
        }).astype({'line_id': 'int', 'station_id': 'int'}).sort_values(['line_id', 'station_id'])
    
    df['process_dttm'] = datetime.datetime.now()
    df.drop_duplicates(inplace=True)

    load_df_to_pg(df, pg_conn_id, pg_schema, pg_table)
    return True


# professional_roles
def transform_data_professional_roles_func(file_path, pg_conn_id, pg_schema, pg_table):

    with open(f"{file_path}.json", 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    id_categories_list = []
    name_categories_list = []
    id_roles_list = []
    name_roles_list = []

    for category in data['categories']:
        for role in category['roles']:
            id_categories_list.append(category['id'])
            name_categories_list.append(category['name'])
            id_roles_list.append(role['id'])
            name_roles_list.append(role['name'])

    df = pd.DataFrame({
        'category_id': id_categories_list,
        'category_name': name_categories_list,
        'role_id': id_roles_list,
        'role_name': name_roles_list,
    })
    
    df['process_dttm'] = datetime.datetime.now()
    df.drop_duplicates(inplace=True)

    load_df_to_pg(df, pg_conn_id, pg_schema, pg_table)
    return True




# dictionaries
def transform_data_dictionaries_func(file_path, pg_conn_id, pg_schema, pg_table):

    with open(f"{file_path}.json", 'r', encoding='utf-8') as f:
        data = json.load(f)    

    print(pg_table)

    df = pd.json_normalize(data[f'{pg_table}'])
    df['process_dttm'] = datetime.datetime.now()
    df.drop_duplicates(inplace=True)

    load_df_to_pg(df, pg_conn_id, pg_schema, pg_table)
    return True


# vacancies
def get_url_from_data(data_str: str, file_path):
    data = json.loads(data_str)
    for vacancie in data['items']:
        vacancie_url = vacancie['url']
        
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(vacancie_url)
            f.write("\n")


def get_url_vacancies_func(base_url: str, endpoint: str, headers: str, stage_folder_path: str, area: str, search_text: str, search_date: str):
    url = base_url + endpoint
    headers = json.loads(headers)
    file_path = f"{stage_folder_path}{endpoint}_url"
    area = area[0]

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
            check_folder_exists(f"{file_path}/{search_text}")

            if data['pages'] > 1:
                for page in range(1, data['pages'] + 1, 1):
                    print(f" ::: page process: {page}")
                    params['page'] = page
                    
                    response = requests.get(url, headers=headers, params=params) # type: ignore
                    get_url_from_data(response.text, file_path=f"{file_path}/{search_text}/urls.txt")
            else:
                get_url_from_data(response.text, file_path=f"{file_path}/{search_text}/urls.txt")
    except Exception as e:
        log.error(e)
    return True


def get_data_vacancies_func(stage_folder_path, endpoint, headers, search_text):
    headers = json.loads(headers)
    
    file_with_urls = f"{stage_folder_path}vacancies_url/{search_text}/urls.txt"
    folder_vacancies_data = f"{stage_folder_path}json/{endpoint}"
    check_folder_exists(folder_vacancies_data)

    try:
        # Читаем построчно файл с url
        with open(file_with_urls) as file:
            lines = [line.rstrip() for line in file]

            for line in lines:
                vacancie_url = line
                print(vacancie_url)
                vacancie_id = line.split("?")[0].split("/")[-1]

                vacancie_response = requests.get(vacancie_url, headers=headers)
                vacancie_data = json.loads(vacancie_response.text)

                file_with_data = f"{folder_vacancies_data}/{vacancie_id}.json"

                with open(file_with_data, 'w', encoding='utf-8') as f:
                    json.dump(vacancie_data, f, ensure_ascii=False, indent=4)
                vacancie_response.close()
                time.sleep(random.randint(33, 35) / 100)

    except Exception as e:
        log.error(e)
    return True


def get_file_names_func(stage_folder_path, endpoint):
    folder_process_path = f"{stage_folder_path}json/{endpoint}"
    ids = []
    for path, subdirs, files in os.walk(folder_process_path):
        for name in files:
            id = name.split(".")[0]
            ids.append(id)

    lisl_of_list_ids = [[x] for x in ids]
    return lisl_of_list_ids


def transform_data_vacancie_func(stage_folder_path, pg_conn_id, pg_schema, pg_table):

    vacancies_ids_list_of_list = get_file_names_func(stage_folder_path, pg_table)

    df = pd.DataFrame()
    try:
        for vacancie_id in vacancies_ids_list_of_list:
            vacancie_id = vacancie_id[0]
            
            process_stage_folder_path = f"{stage_folder_path}json/vacancies/{vacancie_id}.json"

            with open(process_stage_folder_path, 'r', encoding='utf-8') as f:
                vacancie_data = json.load(f)

            id_list = [] 
            name_list = []
            area_list = []
            salary_from_list = []  # зп 
            salary_to_list = []  # зп 
            type_list = [] 
            metro_list =[]
            experience_list = [] 
            schedule_list = [] 
            employment_list = [] 
            key_skills_list = []
            archived_list = []
            specializations_list = [] 
            professional_roles_list = [] 
            employer_list = []  # достать только id
            published_at_list = [] 
            created_at_list = [] 
            initial_created_at_list = [] 
            working_days_list = [] 
            working_time_intervals_list = [] 
            working_time_modes_list = [] 
            languages_list = [] 
            languages_level_list = []
            approved_list = []

            id_list.append(vacancie_data.get('id', None) if vacancie_data.get('id') else None) 
            name_list.append(vacancie_data.get('name', None) if vacancie_data.get('name') else None)
            area_list.append(vacancie_data.get('area', None).get('id') if vacancie_data.get('area') else None)
            salary_from_list.append(vacancie_data.get('salary', {'from': 0}).get('from') if vacancie_data.get('salary') else None)
            salary_to_list.append(vacancie_data.get('salary', {'to': 0}).get('to') if vacancie_data.get('salary') else None) 
            type_list.append(vacancie_data.get('type', None).get('id', None) if vacancie_data.get('type') else None) 
            metro_list.append(vacancie_data.get('address', None).get('metro').get('station_id') if vacancie_data.get('address') and vacancie_data.get('address').get('metro') else None)
            experience_list.append(vacancie_data.get('experience', None).get('id', None) if vacancie_data.get('experience') else None) 
            schedule_list.append(vacancie_data.get('schedule', None).get('id', None) if vacancie_data.get('schedule') else None) 
            employment_list.append(vacancie_data.get('employment', None).get('id', None) if vacancie_data.get('employment') else None) 
            key_skills_list.append([d['name'] for d in vacancie_data.get('key_skills', None)] if vacancie_data.get('key_skills') else None)
            archived_list.append(vacancie_data.get('archived', None) if vacancie_data.get('archived') else None) 
            specializations_list.append(vacancie_data.get('specializations', None) if vacancie_data.get('specializations') else None) 
            professional_roles_list.append(vacancie_data.get('professional_roles', None)[0].get('id', None) if vacancie_data.get('professional_roles') else None) 
            employer_list.append(vacancie_data.get('employer', None).get('id', None) if vacancie_data.get('employer') else None)
            published_at_list.append(vacancie_data.get('published_at', None) if vacancie_data.get('published_at') else None)
            created_at_list.append(vacancie_data.get('created_at', None) if vacancie_data.get('created_at') else None)
            initial_created_at_list.append(vacancie_data.get('initial_created_at', None) if vacancie_data.get('initial_created_at') else None)
            working_days_list.append(vacancie_data.get('working_days', None)[0].get('id') if vacancie_data.get('working_days') and len(vacancie_data['working_days']) > 0 else None) 
            working_time_intervals_list.append(vacancie_data.get('working_time_intervals', None)[0].get('id') if vacancie_data.get('working_time_intervals') and len(vacancie_data['working_time_intervals']) > 0 else None) 
            working_time_modes_list.append(vacancie_data.get('working_time_modes', None)[0].get('id') if vacancie_data.get('working_time_modes') and len(vacancie_data['working_time_modes']) > 0 else None) 
            languages_list.append(vacancie_data.get('languages', None)[0].get('id') if vacancie_data.get('languages') and len(vacancie_data['languages']) > 0 else None)
            languages_level_list.append(vacancie_data.get('languages', None)[0].get('level').get('id') if vacancie_data.get('languages') and len(vacancie_data['languages']) > 0 else None)
            approved_list.append(vacancie_data.get('approved', None))

            tmp_df = pd.DataFrame({
                'id': id_list,  
                'name': name_list,  
                'area': area_list,  
                'salary_from': salary_from_list,
                'salary_to': salary_to_list,  
                'type': type_list,
                'metro': metro_list,
                'experience': experience_list,  
                'schedule': schedule_list,  
                'employment': employment_list,  
                'key_skills': key_skills_list,   
                'archived': archived_list,  
                'specializations': specializations_list,  
                'professional_roles': professional_roles_list,    
                'employer': employer_list,  
                'published_at': published_at_list,  
                'created_at': created_at_list,  
                'initial_created_at': initial_created_at_list,   
                'working_days': working_days_list,  
                'working_time_intervals': working_time_intervals_list,  
                'working_time_modes': working_time_modes_list,  
                'languages': languages_list, 
                'languages_level': languages_level_list, 
                'approved': approved_list
            })

            tmp_df['process_dttm'] = datetime.datetime.now()

            df = pd.concat([df, tmp_df])
    except Exception as e:
        log.error(e)
    finally:
        load_df_to_pg(df, pg_conn_id, pg_schema, pg_table, mode='append')
    
    return True


# employers
def get_data_employers_func(base_url, endpoint, headers, stage_folder_path, process_id_list):
    url = f"{base_url}{endpoint}"
    headers = json.loads(headers)
    file_path = f"{stage_folder_path}json/employers"
    check_folder_exists(file_path)
    
    unprocessed_id_list = process_id_list.copy()

    for employer_id in process_id_list:
        print(process_id_list)
        url_employer = f"{url}/{str(employer_id)}"
        print(f" ::: processed url :{url_employer}")

        response = requests.get(url_employer, headers=headers)
        data = json.loads(response.text)
    
        with open(f"{file_path}/{str(employer_id)}.json", 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        unprocessed_id_list.remove(employer_id)
        unprocessed_id_str = str(unprocessed_id_list).replace("[", "").replace("]", "")
        Variable.set(key="HH_EMPLOYERS_ID", value=unprocessed_id_str)
        print(f"set var: {unprocessed_id_str}")

        time.sleep(random.randint(33, 35) / 100)

    return True

def transform_data_employer_func(stage_folder_path, pg_conn_id, pg_schema, pg_table):

    employers_ids_list_of_list = get_file_names_func(stage_folder_path, pg_table)
    
    df = pd.DataFrame()
    try:
        for employer_id in employers_ids_list_of_list:
            employer_id = employer_id[0]
            
            process_stage_folder_path = f"{stage_folder_path}json/employers/{employer_id}.json"

            with open(process_stage_folder_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            id_list = []
            name_list = []
            type_list = []
            site_url_list = []
            vacancies_url_list = []
            area_id_list = []
            open_vacancies_list = []

            id_list.append(data.get('id', None))
            name_list.append(data.get('name', None))
            type_list.append(data.get('type', None))
            site_url_list.append(data.get('site_url', None))
            vacancies_url_list.append(data.get('vacancies_url', None))
            area_id_list.append(data.get('area').get('id') if data.get('area') else None)
            open_vacancies_list.append(data.get('open_vacancies', None))

            tmp_df = pd.DataFrame({
                    'id': id_list,
                    'name': name_list,
                    'type': type_list,
                    'site_url': site_url_list,
                    'vacancies_url': vacancies_url_list,
                    'area_id': area_id_list,
                    'open_vacancies': open_vacancies_list
                })

            tmp_df['process_dttm'] = datetime.datetime.now()

            df = pd.concat([df, tmp_df])
    except Exception as e:
        log.error(e)
    finally:
        load_df_to_pg(df, pg_conn_id, pg_schema, pg_table, mode='append')
    return True

