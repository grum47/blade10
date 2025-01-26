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
        df['process_dttm'] = datetime.now()
        df.drop_duplicates(inplace=True)

        output_file_path = f"{input_file_path}/areas.parquet"
        df.to_parquet(output_file_path, index=False)

        return output_file_path

    def parse_data_dictionaries__json2parquet(self,):
        pass

    def parse_data_metro__json2parquet(self,):
        pass

    def parse_data_professional_roles__json2parquet(self,):
        pass

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