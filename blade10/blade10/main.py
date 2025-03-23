#!/home/blade10/blade10/blade10/blade10Venv/bin/python3

from datetime import (
    datetime,
    date,
    timedelta
)

import pandas as pd
import yaml
import time


from data_fetcher import (
    blade10_log,
    get_data_simple_processing__api2json,
    parse_data_areas__json2parquet,
    parse_data_dictionaries__json2parquet,
    parse_data_metro__json2parquet,
    parse_data_professional_roles__json2parquet,
    get_list_from_file,
    get_vacancies_url_to_txt,
    get_data_vacancies_url_to_json,
    get_vacancies_data_json2parquete,
    get_data_employers_api2json,
    transform_employer_data_json2parquete
)

from data_transfer import (
    find_parquete_files,
    transfer_data_parquete_to_clickhouse,
    search_ip
)


def main():
    # read config file
    with open('blade10/blade10/blade10/config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    base_url = config['hh_api']['base_url']
    headers = config['hh_api']['headers']
    data_folder_path = f'blade10/blade10/blade10/data/'

    process_date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    blade10_log(process_date)

    endpoints = [] 
    for i in config['hh_api']['endpoints']['simple_processing']:
        try:
            for key in i.keys():
                endpoints.append(key)
        except:
            endpoints.append(i)

    blade10_log(f"Endpoints: {endpoints}")
 
    for endpoint in endpoints:
        get_data_simple_processing__api2json(
            base_url=base_url,
            headers=headers,
            endpoint=endpoint,
            stage_folder_path=f"{data_folder_path}{endpoint}"
        )
        blade10_log(f"Process: {endpoint} api to json success")

    path_output_data_areas, path_id_parents = parse_data_areas__json2parquet(
        input_file_path=f"{data_folder_path}areas"
    )
    
    for dictionary_name in config['hh_api']['endpoints']['simple_processing'][1]['dictionaries']:
        parse_data_dictionaries__json2parquet(input_file_path=f'{data_folder_path}dictionaries/', dictionary_name=dictionary_name)
        blade10_log(f"Process: {dictionary_name} json to parquet success")

    parse_data_metro__json2parquet(input_file_path=f'{data_folder_path}metro/')
    blade10_log(f"Process: metro json to parquet success")
    parse_data_professional_roles__json2parquet(input_file_path=f'{data_folder_path}professional_roles/')
    blade10_log(f"Process: professional_roles json to parquet success")
    
    search_text_list = config['hh_api']['search']
    blade10_log(f"Search text list: {search_text_list}")
    areas_parent_id_list = get_list_from_file(path_id_parents)

    blade10_log("PROCESS VACANCIES")
    endpoint = 'vacancies'

    get_vacancies_url_to_txt(
        base_url=base_url,
        endpoint=endpoint,
        headers=headers,
        search_date=process_date,
        file_path=f"{data_folder_path}{endpoint}",
        search_text_list=search_text_list,
        areas_parent_id_list=areas_parent_id_list
    )

    get_data_vacancies_url_to_json(folder_path=f"{data_folder_path}{endpoint}",headers=headers)
    blade10_log(f"Vacancies url to json success")

    path_vacancy_parquete = get_vacancies_data_json2parquete(folder_path=f"{data_folder_path}{endpoint}")
    blade10_log(f"Vacancies json to parquet success")
    df_vac = pd.read_parquet(path_vacancy_parquete)
    list_employers_id = df_vac['employer'].tolist()
    
    endpoint = 'employers'
    get_data_employers_api2json(
        base_url=base_url,
        endpoint=endpoint,
        headers=headers,
        folder_path=data_folder_path,
        process_id_list=list_employers_id
    )
    blade10_log(f"Employers url to json success")

    transform_employer_data_json2parquete(data_folder_path + endpoint)
    blade10_log(f"Employers json to parquet success")

    # stage raw data to ClickHouse
    transfer_files_list = find_parquete_files('blade10/blade10/blade10/data')
    blade10_log(f"Paruet files: {transfer_files_list}")

    ip_list = search_ip()
    blade10_log(f"CliclHouse DB ip's: {ip_list}")

    for ip in ip_list:
        blade10_log(f"Process transfet to CliclHouse ip: {ip}")
        try:
            transfer_data_parquete_to_clickhouse(
                file_paths=transfer_files_list,
                db_name='blade10_raw',
                host=ip,
                user='blade10',
                password='blade10'
            )
        except Exception as e:
            blade10_log(f"Error: {e}")

if __name__ == "__main__":
    start = time.monotonic_ns()
    main()
    end = time.monotonic_ns()
    blade10_log('Время работы в минутах: ' + str((end - start) / 1000000000 / 60))
    