import pendulum
import logging
import json
import requests
import pandas as pd
import random
import time
import os

from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

# logging
log = logging.getLogger(__name__)



def func_create_variable_list_dict_tables(**kwargs):
    variable_key = kwargs['variable_key']
    variable_value = kwargs['variable_values']
    # context = get_current_context()
    Variable.set(key=variable_key, value=variable_value)
    return True


def save_to_csv(df, df_path):
    if os.path.isfile(df_path):
        os.remove(df_path)
        log.info(f" ::: File {df_path} removed")
    else:
        log.info(f" ::: File {df_path} is not exists")
    log.info(f" ::: df.shape = {df.shape}")
    df.to_csv(df_path, index=False, sep=';')
    log.info(f" ::: df save to {df_path}")
    return True


# load df to PG
def tmp_stage_to_pg(*args, **kwargs):
    PG_CONN_ID = kwargs['PG_CONN_ID']
    PG_RAW_SCHEMA = kwargs['PG_RAW_SCHEMA']
    PG_TABLE_NAME = args[0]
    tmp_stage_path_df = kwargs['tmp_stage_path_df']

    postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    postgres_engine = postgres_hook.get_sqlalchemy_engine()

    df = pd.read_csv(tmp_stage_path_df, sep=';')
    df['process_dttm'] = pendulum.now('Europe/Moscow')
    log.info(f" ::: df.shape = {df.shape}")
    df.drop_duplicates(inplace=True)

    df.to_sql(
        name=PG_TABLE_NAME,
        con=postgres_engine,
        schema=PG_RAW_SCHEMA,
        if_exists='replace',
        index=False
        )
    log.info(f" ::: df {tmp_stage_path_df} insert into table {PG_TABLE_NAME}")
    return True


# dictionaries
def get_list_names_dictionaries(**kwargs):
    process_dicts = kwargs['process_dicts']
    list_process_dicts = [x for x in process_dicts.split(', ')]
    context = get_current_context()
    data_str = context["ti"].xcom_pull(task_ids="get_data")
    data = json.loads(data_str)
    list_names_dictionaries = list(data.keys())
    list_names_dictionaries = [[item] for item in list_names_dictionaries if item in list_process_dicts]
    
    log.info(f" ::: process_dicts:           {process_dicts}")
    log.info(f" ::: list_names_dictionaries: {list_names_dictionaries}")
    log.info(f" ::: list_process_dicts:      {list_process_dicts}")
    return list_names_dictionaries


def transform_data_dictionaries(*args, **kwargs):
    name_dict = args[0]
    df_path = kwargs['tmp_stage_path_df']
    context = get_current_context()
    data_str = context["ti"].xcom_pull(task_ids="get_data")
    data = json.loads(data_str)
    df = pd.json_normalize(data[f'{name_dict}'])
    save_to_csv(df, df_path)
    return True

# areas
def transform_data_areas(data, df_path):
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
    save_to_csv(df, df_path)
    return True


def get_areas_data(**kwargs):
    # TODO: Сделать через файл передачу данных
    df_path = kwargs['tmp_stage_path_df']
    context = get_current_context()
    data_str = context["ti"].xcom_pull(task_ids="get_data")
    data = json.loads(data_str)
    transform_data_areas(data, df_path)
    return True


# professional_roles
def transform_data_professional_roles(data, df_path):
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

    df.drop_duplicates(inplace=True)

    save_to_csv(df, df_path)
    return True


def get_profesional_roles_data(**kwargs):
    print(f"""
          url: {kwargs['url']},
          headers: {json.loads(kwargs['headers'])},
          tmp_stage_path_df: {kwargs['tmp_stage_path_df']}
          """)
    url = kwargs['url']
    headers = json.loads(kwargs['headers'])
    df_path = kwargs['tmp_stage_path_df']
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)
    transform_data_professional_roles(data, df_path)
    return True


# metro
def transform_data_metro(**kwargs):
    df_path = kwargs['tmp_stage_path_df']
    context = get_current_context()
    data_str = context["ti"].xcom_pull(task_ids="get_data")
    data = json.loads(data_str)

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
    save_to_csv(df, df_path)
    return True


# vacancies
def transform_vacancie_data(data, headers):
    for vacancie in data['items']:
        vacancie_url = vacancie['url']
        vacancie_response = requests.get(vacancie_url, headers=headers)
        vacancie_data = json.loads(vacancie_response.text)
        # print(vacancie_data)

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

        df = pd.DataFrame({
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

        vacancie_path = '/opt/airflow/tmp_stage/vacancies.csv'
        # TODO: поменять на функцию сохранения
        df.to_csv(vacancie_path, mode='a', header=not os.path.exists(vacancie_path), index=False, sep=';')
        vacancie_response.close()
        time.sleep(random.randint(33, 45) / 100)
    # except Exception as e:
    #     print(f" ::: data: {data}")
    #     print({e})
    return True


def get_vacancies_data(*args, **kwargs):
    log.info(f" ::: ds ->> {kwargs['ds']}")
    url = kwargs['url']
    headers = json.loads(kwargs['headers'])
    search_date = kwargs['search_date']
    search_text = kwargs['search_text']
    area_id = int(args[0])
    params = {
        'area': area_id,
        'date_from': search_date,
        'date_to': search_date,
        'per_page': 100,  # не более 100 элементов на страницу
        'text': search_text,
        'search_field': 'name',
    }
    print(params)

    response = requests.get(url, headers=headers, params=params)
    data = json.loads(response.text)
    try:
        data_found = data['found']
        data_pages = data['pages']
        data_cnt_items = len(data['items'])
        print(f" ::: area: {area_id}, data.found: {data_found}, date.pages: {data_pages}, data.items: {data_cnt_items}")
        # Написать логику обработки когда более 20 страниц (разбивать по городам)
        if data['found'] != 0:
            if data['pages'] > 1:
                for page in range(1, data['pages'] + 1, 1):
                    print(f" ::: page process: {page}")
                    params['page'] = page
                    # print(params)
                    response = requests.get(url, headers=headers, params=params)
                    data = json.loads(response.text)
                    transform_vacancie_data(data, headers)
            else:
                transform_vacancie_data(data, headers)
    except Exception as e:
        log.info(f" ::::: ERROR: {e}")
    return True

# employers
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