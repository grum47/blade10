#!/home/blade10/blade10/blade10/blade10Venv/bin/python3

import os
import yaml
import json
import requests
import pandas as pd
import logging

import time
import random

from datetime import (
    datetime,
    timedelta
)
from typing import Dict, Any, List

log = logging.getLogger(__name__)


def get_data_simple_processing__api2json(
        base_url: str,
        headers: dict,
        endpoint: str,
        stage_folder_path: str
    ) -> str:

    """We get the endpoint data through the API,
    save it in json for further processing.

    Args:
        base_url (str): _description_
        endpoint (str): _description_
        headers (dict): _description_
        stage_folder_path (str): _description_
    Returns:
        file_path: The path to the json file
    """

    response = requests.get(base_url + endpoint, headers)
    data = json.loads(response.text)

    file_path = f"{stage_folder_path}/{endpoint}.json"
    check_folder_exists(stage_folder_path)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    
    return file_path


def add_metadata_and_save_to_parquet(
        input_df: pd.DataFrame,
        output_file_path: str
    ) -> bool:
    """add the necessary metadata to the data and save the parquet d file.

    Args:
        input_df (pd.DataFrame): The dataframe to add metadata to
        output_file_path (str): The path to the file with the result of processing
    """
    output_df = input_df
    output_df['process_dttm'] = datetime.now()
    output_df.drop_duplicates(inplace=True)

    output_file_path = output_file_path
    output_df.to_parquet(output_file_path, index=False)

    return True


def parse_data_areas__json2parquet(
        input_file_path: str
    ) -> tuple:

    """We process the json file with data on cities and save it to the parquet file.

    Args:
        input_file_path (str): The path of the data file in json format
    Returns:
        (output_file_path, output_file_path_parents_id) (tuple): The path to the file with the result of processing
    """

    with open(f"{input_file_path}/areas.json", 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    id_list = []
    parent_id_list = []
    name_list = []
    areas_list = []

    # due to the nested json structure, we recursively process the data in the file
    def parse_areas_data(data):
        for item in data:
            id_list.append(item.get('id', None))
            parent_id_list.append(item.get('parent_id', None))
            name_list.append(item.get('name', None))
            areas_list.append(len(item.get('areas', [])))
            if item.get('areas'):
                parse_areas_data(item['areas'])
        return id_list, parent_id_list, name_list, areas_list
    
    # run parsing areas data
    id_list, parent_id_list, name_list, areas_list = parse_areas_data(data)

    df = pd.DataFrame({
        'id': id_list,
        'parent_id': parent_id_list,
        'name': name_list,
        'areas': areas_list
    })
    
    output_file_path = f"{input_file_path}/areas.parquet"
    add_metadata_and_save_to_parquet(
        input_df=df,
        output_file_path=output_file_path
    )

    areas_parent_id_list = [parent_id for parent_id in df['parent_id'].unique()]
    output_file_path_parents_id = f"{input_file_path}/areas_parent_id.txt"
    with open(output_file_path_parents_id, "w") as output:
        for area_parent_id in areas_parent_id_list:
            if area_parent_id:
                output.write(f"{area_parent_id}\n")

    return (output_file_path, output_file_path_parents_id)


def get_list_from_file(file_path: str) -> List:
    with open(file_path, 'r') as file:
        outpup_list = [line.rstrip() for line in file]
    return outpup_list


def get_url_from_first_json_vacancies(
        data_str: str,
        file_path: str
        ) -> bool:
    """_summary_

    Args:
        data_str (str): _description_
        file_path (str): _description_

    Returns:
        bool: _description_
    """
    data = json.loads(data_str)
    for vacancie in data['items']:
        vacancie_url = vacancie['url']
        
        with open(file_path, 'a', encoding='utf-8') as f:
            f.write(vacancie_url)
            f.write("\n")
    return True


def get_vacancies_url_to_txt(
        base_url: str,
        endpoint: str,
        headers: dict,
        search_date: str,
        file_path: str,
        search_text_list: List,
        areas_parent_id_list: List
        ) -> bool:
    
    url = base_url + endpoint
    print(url)

    params = {
        'date_from': search_date,
        'date_to': search_date,
        'per_page': 100,  # not more 100 elements per page
        'search_field': 'name',
    }

    print(f" ::::: {params}")

    for search_text in search_text_list:
        
        params['text'] = search_text

        print(f"search text: {search_text}")
        file_path_from_urls = f"{file_path}/{search_text}"
        print(file_path_from_urls)

        check_folder_exists(file_path_from_urls)

        for areas_parent_id in areas_parent_id_list:
            params['area'] = int(areas_parent_id)

            response = requests.get(url, params=params, headers=headers)

            try:
                data = json.loads(response.text)
                print(f" ::::: area: {areas_parent_id}, data.found: {data['found']}, date.pages: {data['pages']}, data.items: {len(data['items'])}")

                if data['found'] != 0:
                    if data['pages'] > 1:
                        for page in range(1, data['pages'] + 1, 1):
                            print(f" ::::: page process: {page}")
                            params['page'] = page

                            response = requests.get(url, params=params, headers=headers)
                            get_url_from_first_json_vacancies(response.text, file_path=file_path_from_urls + "/urls.txt")
                            time.sleep(random.randint(33, 35) / 100)
                    else:
                        get_url_from_first_json_vacancies(response.text, file_path=file_path_from_urls + "/urls.txt")
                time.sleep(random.randint(33, 35) / 100)
            except Exception as error:
                log.error(error)

    return True

def get_data_vacancies_url_to_json(folder_path, headers):    
    file_with_urls = get_all_files_in_subfolders(folder_path)
    print(file_with_urls)

    for file in file_with_urls:
        if file.endswith("urls.txt"):
            print(file)

            # Читаем построчно файл с url
            with open(file) as file:
                lines = [line.rstrip() for line in file]

                for line in lines:
                    vacancie_url = line
                    print(vacancie_url)
                    vacancie_id = line.split("?")[0].split("/")[-1]

                    vacancie_response = requests.get(vacancie_url, headers=headers)
                    vacancie_data = json.loads(vacancie_response.text)

                    file_with_data = f"{folder_path}/{vacancie_id}.json"

                    with open(file_with_data, 'w', encoding='utf-8') as f:
                        json.dump(vacancie_data, f, ensure_ascii=False, indent=4)
                    vacancie_response.close()
                    time.sleep(random.randint(33, 35) / 100)

    return True


def parse_data_dictionaries__json2parquet(input_file_path: str, dictionary_name: str) -> str:
    """We process the json file with data on dictionaries and save it to the parquet file.

    Args:
        input_file_path (str): The path of the data file in json format
        dictionary_name (str): The name of the nested dictionary
    Returns:
        output_file_path (str): The path to the file with the result of processing
    """
    
    with open(f"{input_file_path}dictionaries.json", 'r', encoding='utf-8') as f:
        data = json.load(f)    

    df = pd.json_normalize(data[f'{dictionary_name}'])
    
    output_file_path = f"{input_file_path}/{dictionary_name}.parquet"
    add_metadata_and_save_to_parquet(
        input_df=df,
        output_file_path=output_file_path
    )

    return output_file_path


def parse_data_metro__json2parquet(input_file_path: str) -> str:
    """We process the json file with data on metro and save it to the parquet file.

    Args:
        input_file_path (str): The path of the data file in json format
    Returns:
        output_file_path (str): The path to the file with the result of processing
    """
    
    with open(f"{input_file_path}metro.json", 'r', encoding='utf-8') as f:
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
    
    output_file_path = f"{input_file_path}/metro.parquet"
    add_metadata_and_save_to_parquet(
        input_df=df,
        output_file_path=output_file_path
    )

    return output_file_path


def parse_data_professional_roles__json2parquet(input_file_path: str) -> str:
    """We process the json file with data on professional_roles and save it to the parquet file.

    Args:
        input_file_path (str): The path of the data file in json format
    Returns:
        output_file_path (str): The path to the file with the result of processing
    """
    
    with open(f"{input_file_path}professional_roles.json", 'r', encoding='utf-8') as f:
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
    
    output_file_path = f"{input_file_path}/professional_roles.parquet"
    add_metadata_and_save_to_parquet(
        input_df=df,
        output_file_path=output_file_path
    )

    return output_file_path


def get_data_employers_api2json(
        base_url: str, 
        endpoint: str, 
        headers: dict, 
        folder_path: str, 
        process_id_list: List
        ) -> bool:
    
    url = f"{base_url}{endpoint}"
    check_folder_exists(folder_path + endpoint)
    
    for employer_id in process_id_list:
        url_employer = f"{url}/{str(employer_id)}"
        print(f" ::: processed url :{url_employer}")

        response = requests.get(url_employer, headers=headers)
        data = json.loads(response.text)
    
        with open(f"{folder_path}{endpoint}/{str(employer_id)}.json", 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        time.sleep(random.randint(33, 35) / 100)

    return True


def check_folder_exists(path_folder):
    """Проверяем наличие папки, если нету, создаем

    Args:
        path_folder (string): Путь к папке
    """
    if not os.path.exists(path_folder):
        os.makedirs(path_folder)
    else:
        print(f"Folder {path_folder} is exists")


# def find_url_files(folder_path: str) -> List:
#     """_summary_

#     Args:
#         folder_path (str): _description_

#     Returns:
#         List: _description_
#     """
#     url_files = []
#     for root, dirs, files in os.walk(folder_path):
#         for file in files:
#             if 'urls' in file:
#                 url_files.append(os.path.join(root, file))
#     return url_files


def get_all_files_in_subfolders(folder_path):
    """_summary_

    Args:
        folder_path (_type_): _description_

    Returns:
        _type_: _description_
    """
    file_paths = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return tuple(file_paths)


def get_vacancies_data_json2parquete(folder_path: str) -> str:
    """_summary_

    Args:
        folder_path (str): _description_
    Returns:
        str: _description_
    """

    vacancies_files = get_all_files_in_subfolders(folder_path)

    df = pd.DataFrame()
    for vacancie_file_path in vacancies_files:
        if vacancie_file_path.endswith('.json'):
            with open(vacancie_file_path, 'r', encoding='utf-8') as f:
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
            key_skills_list.append(str([d['name'] for d in vacancie_data.get('key_skills', None)] if vacancie_data.get('key_skills') else None))
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

            df = pd.concat([df, tmp_df])

    output_file_path = f"{folder_path}/vacancie.parquet"
    add_metadata_and_save_to_parquet(
        input_df=df,
        output_file_path=output_file_path
    )

    return output_file_path


def transform_employer_data_json2parquete(folder_path: str) -> str:

    employers_files = get_all_files_in_subfolders(folder_path)

    df = pd.DataFrame()
    print(employers_files)

    for employers_file_path in employers_files:
        if employers_file_path.endswith('.json'):
            with open(employers_file_path, 'r', encoding='utf-8') as f:
                employer_data = json.load(f)

            id_list = []
            name_list = []
            type_list = []
            site_url_list = []
            vacancies_url_list = []
            area_id_list = []
            open_vacancies_list = []

            id_list.append(employer_data.get('id', None))
            name_list.append(employer_data.get('name', None))
            type_list.append(employer_data.get('type', None))
            site_url_list.append(employer_data.get('site_url', None))
            vacancies_url_list.append(employer_data.get('vacancies_url', None))
            area_id_list.append(employer_data.get('area').get('id') if employer_data.get('area') else None)
            open_vacancies_list.append(employer_data.get('open_vacancies', None))

            tmp_df = pd.DataFrame({
                    'id': id_list,
                    'name': name_list,
                    'type': type_list,
                    'site_url': site_url_list,
                    'vacancies_url': vacancies_url_list,
                    'area_id': area_id_list,
                    'open_vacancies': open_vacancies_list
                })

            tmp_df['process_dttm'] = datetime.now()

            df = pd.concat([df, tmp_df])

    output_file_path = f"{folder_path}/employers.parquet"
    add_metadata_and_save_to_parquet(
        input_df=df,
        output_file_path=output_file_path
    )

    return output_file_path
