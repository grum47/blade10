import pandas as pd
import numpy as np
import requests
import json
import time
import os
import random
# from tqdm import tqdm


# def get_list_area_id(**kwargs):
#     """
#     Функция возвращает список id стран, в которых более 1999 вакансий
#     и список id стран, в которых менее 1999 вакансий
#     """
#     context = get_current_context()
#     areas_id = context["ti"].xcom_pull(task_ids="get_areas_id")
#     areas_id = [int(x[0]) for x in areas_id] 

#     data_found_dict = {}
#     for id in areas_id:
#         print(f"""
#               url: {kwargs['url']},
#               headers: {json.loads(kwargs['headers'])},
#               search_date: {kwargs['search_date']},
#               search_text: {kwargs['search_text']},
#               area_id: {id}
#               """)
#         url = kwargs['url']
#         headers = json.loads(kwargs['headers'])
#         search_date = kwargs['search_date']
#         search_text = kwargs['search_text']
#         params = {
#             'area': id,
#             'date_from': search_date.replace('05', '04'),
#             'date_to': search_date,
#             'per_page': 100,  # не более 100 элементов на страницу
#             'text': search_text,
#             'search_field': 'name',
#         }
#         print(params)
#         response = requests.get(url, headers=headers, params=params)
        
#         data = json.loads(response.text)
#         data_found = data['found']
#         data_pages = data['pages']
#         data_cnt_items = len(data['items'])
        
#         print(f" ::: area: {id}, data.found: {data_found}, date.pages: {data_pages}, data.items: {data_cnt_items}")
        
#         data_found_dict[f"{id}"] = data_found
#     greater_than_1999 = []
#     less_than_or_equal_1999 = []
#     for key, value in data_found_dict.items():
#         if value > 1999:
#             greater_than_1999.append(key)
#         else:
#             less_than_or_equal_1999.append(key)
    
#     return greater_than_1999, less_than_or_equal_1999


# def get_branch():
#     context = get_current_context()
#     grp_name = str(context['ti']).split('.')[1]
#     print(f" ::: {grp_name}")
#     areas_id = context["ti"].xcom_pull(task_ids=f"{grp_name}.get_list_areas")
#     if len(areas_id[0]) != 0:
#         return [f'{grp_name}.get_list_city_id']
#     else:
#         return [f'{grp_name}_data']


def transform_vacancie_data(data, headers):
    try:
        for vacancie in data['items']:
            vacancie_url = vacancie['url']
            vacancie_response = requests.get(vacancie_url, headers=headers)
            vacancie_data = json.loads(vacancie_response.text)

            id_list = [] 
            relations_list = [] 
            name_list = []
            area_list = []
            salary_from_list = []  # зп 
            salary_to_list = []  # зп 
            type_list = [] 
            experience_list = [] 
            schedule_list = [] 
            employment_list = [] 
            description_list = [] 
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

            id_list.append(vacancie_data.get('id', None)) 
            relations_list.append(vacancie_data.get('relations', None)) 
            name_list.append(vacancie_data.get('name', None)) 
            area_list.append(vacancie_data.get('area', None).get('id')) 
            salary_from_list.append(vacancie_data.get('salary', {'from': 0}).get('from') if vacancie_data.get('salary') else None)
            salary_to_list.append(vacancie_data.get('salary', {'to': 0}).get('to') if vacancie_data.get('salary') else None) 
            type_list.append(vacancie_data.get('type', None).get('id', None) if vacancie_data.get('type') else None) 
            experience_list.append(vacancie_data.get('experience', None).get('id', None)) 
            schedule_list.append(vacancie_data.get('schedule', None).get('id', None)) 
            employment_list.append(vacancie_data.get('employment', None).get('id', None)) 
            description_list.append(vacancie_data.get('description', None)) 
            key_skills_list.append([d['name'] for d in vacancie_data.get('key_skills', None)])
            archived_list.append(vacancie_data.get('archived', None)) 
            specializations_list.append(vacancie_data.get('specializations', None)) 
            professional_roles_list.append(vacancie_data.get('professional_roles', None)[0].get('id', None)) 
            employer_list.append(vacancie_data.get('employer', None).get('id', None)) 
            published_at_list.append(vacancie_data.get('published_at', None)) 
            created_at_list.append(vacancie_data.get('created_at', None)) 
            initial_created_at_list.append(vacancie_data.get('initial_created_at', None)) 
            working_days_list.append(vacancie_data.get('working_days', None)[0].get('id') if vacancie_data.get('working_days') and len(vacancie_data['working_days']) > 0 else None) 
            working_time_intervals_list.append(vacancie_data.get('working_time_intervals', None)[0].get('id') if vacancie_data.get('working_time_intervals') and len(vacancie_data['working_time_intervals']) > 0 else None) 
            working_time_modes_list.append(vacancie_data.get('working_time_modes', None)[0].get('id') if vacancie_data.get('working_time_modes') and len(vacancie_data['working_time_modes']) > 0 else None) 
            languages_list.append(vacancie_data.get('languages', None)[0].get('id') if vacancie_data.get('languages') and len(vacancie_data['languages']) > 0 else None)
            languages_level_list.append(vacancie_data.get('languages', None)[0].get('level').get('id') if vacancie_data.get('languages') and len(vacancie_data['languages']) > 0 else None)
            approved_list.append(vacancie_data.get('approved', None))

            df = pd.DataFrame({
                'id': id_list,  
                'relations': relations_list,  
                'name': name_list,  
                'area': area_list,  
                'salary_from': salary_from_list,
                'salary_to': salary_to_list,  
                'type': type_list,   
                'experience': experience_list,  
                'schedule': schedule_list,  
                'employment': employment_list,  
                'description': description_list,   
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
            df.to_csv(vacancie_path, mode='a', header=not os.path.exists(vacancie_path), index=False, sep=';')
            vacancie_response.close()
            time.sleep(random.randint(33, 45) / 100)
    except:
        print(f" ::: data: {data}")
    return True


def get_vacancies_data(*args, **kwargs):
    print(f"""
          url: {kwargs['url']},
          headers: {json.loads(kwargs['headers'])},
          search_date: {kwargs['search_date']},
          search_text: {kwargs['search_text']},
          area_id: {int(args[0])}
          """)
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
                print(params)
                response = requests.get(url, headers=headers, params=params)
                data = json.loads(response.text)
                transform_vacancie_data(data, headers)
        else:
            transform_vacancie_data(data, headers)
    return True



if __name__ == "__main__":
    pass