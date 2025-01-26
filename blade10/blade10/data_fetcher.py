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

    def get_data_dictionaries(self, base_url, endpoint, headers, stage_folder_path):
        """We get the endpoint data via the API and save it to parquet
        Args:
            base_url (str): _description_
            endpoint (str): _description_
            headers (str): _description_
            stage_folder_path (str): _description_
        Returns:
            file_path: The path to the parquet file
        """
        headers = json.loads(headers)
        response = requests.get(base_url + endpoint, headers=headers)
        data = json.loads(response.text)

        id_list = []
        parent_id_list = []
        name_list = []
        areas_list = []

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

        if not os.path.exists(stage_folder_path):
            os.makedirs(stage_folder_path)
        else:
            log.info(f"Folder {stage_folder_path} is exists")
        
        file_path = f"{stage_folder_path}{endpoint}.parquet"
        df.to_parquet(file_path, index=False)
            
        return file_path