import json
import pandas as pd

from omegaconf import OmegaConf


conf_hh = OmegaConf.load('/opt/airflow/config/hh.yml')

def transform_data_areas(data):
    data = json.loads(data)
    area_id=[]
    parent_id=[]
    name=[]
    
    def foo(data):
        for item in data:
            area_id.append(item.get('id', None))
            parent_id.append(item.get('parent_id', None))
            name.append(item.get('name', None))
            if item.get('areas'):
                foo(item['areas'])
        return area_id, parent_id, name

    area_id, parent_id, name = foo(data)
    
    df = pd.DataFrame({
        'area_id': area_id,
        'parent_id': parent_id,
        'name': name
        })
    return df