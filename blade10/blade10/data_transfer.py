import subprocess
import logging
from typing import List

import pandas as pd
from clickhouse_driver import Client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_parquete_files(path_to_folder: str) -> List:
    from data_fetcher import get_all_files_in_subfolders
    tuple_files = get_all_files_in_subfolders(path_to_folder)
    
    list_parquet_files = [file for file in tuple_files if file.endswith('.parquet')]
    return list_parquet_files


def transfer_data_parquete_to_clickhouse(
    file_paths: List,
    db_name: str,
    host: str,
    user: str,
    password: str) -> bool:

    client = Client(host=host, user=user, password=password, database=db_name)
    
    for path in file_paths:
        try:
            table_name = path.split('.')[-2].split('/')[-1]
            df = pd.read_parquet(path)
            columns = df.columns
            types = df.dtypes
            
            # Создание SQL запроса на создание таблицы
            create_table_query = f"CREATE TABLE IF NOT EXISTS {db_name}.{table_name} ("
            for col, col_type in zip(columns, types):
                if col_type == 'int64':
                    create_table_query += f"{col} UInt64,"
                elif col_type == 'float64':
                    create_table_query += f"{col} Float64,"
                elif col_type == 'object':
                    create_table_query += f"{col} String,"
                elif col_type == 'bool':
                    create_table_query += f"{col} UInt8,"
                elif col_type == 'datetime64[us]':
                    df[col] = df[col].astype('str')
                    create_table_query += f"{col} String,"
                else:
                    create_table_query += f"{col} String,"  # По умолчанию для неизвестных типов
            create_table_query = create_table_query.rstrip(',') + ") ENGINE = MergeTree() ORDER BY tuple();"

            client.execute(create_table_query)
            
            client.insert_dataframe(
                f'INSERT INTO {db_name}.{table_name} VALUES',
                df,
                settings=dict(use_numpy=True)
            )
            
            print(f"Файл {path} успешно обработан и данные записаны в таблицу {table_name}.")
        
        except Exception as e:
            print(f"Ошибка при обработке файла {path}: {e}")
    
    return True


def search_ip():

    import subprocess
    import re

    ip_list = []

    # Подсеть, в которой нужно искать устройства
    subnet = "10.8.0.0/24"

    # Выполняем сканирование
    try:
        result = subprocess.run(['nmap', '-sn', subnet], capture_output=True, text=True, check=True)
        # Ищем IP-адреса с помощью регулярного выражения
        ip_addresses = re.findall(r'Nmap scan report for ([\d\.]+)', result.stdout)

        # Выводим найденные IP-адреса
        for ip in ip_addresses:
            ip_list.append(ip)

    except subprocess.CalledProcessError as e:
        print(f"Ошибка при выполнении nmap: {e}")

    return ip_list