import os
import yaml
import json
import logging
import requests
import pandas as pd

from datetime import (
    datetime,
    timedelta
)
from typing import Dict, Any

log = logging.getLogger(__name__)

class DataFetcher:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def get_data_simple_processing__api2json(
            self,
            base_url: str,
            headers: str,
            endpoint: str,
            stage_folder_path: str
        ) -> str:

        """We get the endpoint data through the API,
        save it in json for further processing.

        Args:
            base_url (str): _description_
            endpoint (str): _description_
            headers (str): _description_
            stage_folder_path (str): _description_
        Returns:
            file_path: The path to the json file
        """

        header = json.loads(headers)
        response = requests.get(base_url + endpoint, headers=header)
        data = json.loads(response.text)

        if not os.path.exists(stage_folder_path):
            os.makedirs(stage_folder_path)
        else:
            print(f"Folder {stage_folder_path} is exists")

        file_path = f"{stage_folder_path}{endpoint}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        
        return file_path
    

    def add_metadata_and_save_to_parquet(
            self,
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
            self,
            input_file_path: str
        ) -> str:

        """We process the json file with data on cities and save it to the parquet file.

        Args:
            input_file_path (str): The path of the data file in json format
        Returns:
            output_file_path (str): The path to the file with the result of processing
        """

        with open(f"{input_file_path}.json", 'r', encoding='utf-8') as f:
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
        self.add_metadata_and_save_to_parquet(
            input_df=df,
            output_file_path=output_file_path
        )

        return output_file_path


    def parse_data_dictionaries__json2parquet(
            self,
            input_file_path: str,
            dictionary_name: str
        ) -> str:

        """We process the json file with data on dictionaries and save it to the parquet file.

        Args:
            input_file_path (str): The path of the data file in json format
            dictionary_name (str): The name of the nested dictionary
        Returns:
            output_file_path (str): The path to the file with the result of processing
        """
        
        with open(f"{input_file_path}.json", 'r', encoding='utf-8') as f:
            data = json.load(f)    

        df = pd.json_normalize(data[f'{dictionary_name}'])
        
        output_file_path = f"{input_file_path}/{dictionary_name}.parquet"
        self.add_metadata_and_save_to_parquet(
            input_df=df,
            output_file_path=output_file_path
        )

        return output_file_path


    def parse_data_metro__json2parquet(
            self,
            input_file_path: str
        ) -> str:

        """We process the json file with data on metro and save it to the parquet file.

        Args:
            input_file_path (str): The path of the data file in json format
        Returns:
            output_file_path (str): The path to the file with the result of processing
        """
        
        with open(f"{input_file_path}.json", 'r', encoding='utf-8') as f:
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
        self.add_metadata_and_save_to_parquet(
            input_df=df,
            output_file_path=output_file_path
        )

        return output_file_path



    def parse_data_professional_roles__json2parquet(
            self,
            input_file_path: str
        ) -> str:
        
        """We process the json file with data on professional_roles and save it to the parquet file.

        Args:
            input_file_path (str): The path of the data file in json format
        Returns:
            output_file_path (str): The path to the file with the result of processing
        """
        
        with open(f"{input_file_path}.json", 'r', encoding='utf-8') as f:
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
        self.add_metadata_and_save_to_parquet(
            input_df=df,
            output_file_path=output_file_path
        )

        return output_file_path


    def get_area_parents_id(self, ):
        pass

    def get_vacancies_url(self, ):
        pass

    def get_vacancies_data(self, ):
        pass

    def get_employers_id(self, ):
        pass

    def get_employers_data(self, ):
        pass