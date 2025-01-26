import os
import logging

from datetime import datetime, timedelta

from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# logging
logger = logging.getLogger(__name__)

DAG_ID = os.path.realpath(__file__).split("/")[-1].split(".")[0]
NEXT_DAG_ID = "02_02_etl_ods"

default_args = {
    "owner": "Pavel Evtuhovich",
}

@dag(
    dag_id=DAG_ID,
    tags=["02"],
    start_date=datetime(2024, 7, 27),
    end_date=None,
    default_args=default_args,
    schedule='30 5 * * *',
    max_active_tasks=5,
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=["/opt/airflow/include/sql/hh/"],
)
def process_dag():

    from headhunter.scripts.draft_new import get_list_of_lists_func

    PG_CONN_ID = "postgres_conn"
    PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")

    HH_BASE_URL = Variable.get("HH_URL")
    HH_HEADERS = Variable.get("HH_APPNAME")
    HH_VACANCIES_ENDPOINT = Variable.get("HH_ENDPOINT_VACANCIES")
    HH_EMPLOYERS_ENDPOINT = Variable.get("HH_ENDPOINT_EMPLOYERS")
    HH_LIST_TEXT_SEARCH = Variable.get("HH_LIST_TEXT_SEARCH").split(', ')

    STAGE_FOLDER_PATH = Variable.get("HH_PATH_DF_STAGE")
    STAGE_FOLDER_PATH_JSON = STAGE_FOLDER_PATH + "json/"

    TG_CONN_ID = "telegram_connection"
    TG_CHAT_ID = Variable.get("TG_CHAT_ID")

    # Записываем дату выполнения для передачи в следующие даги
    @task()
    def start():
        from headhunter.scripts.draft_new import set_variables_func
        set_variables_func("HH_PROCESS_DS", 'ds')

    # send_message_telegram_task = TelegramOperator(
    #     task_id="send_message_telegram",
    #     telegram_conn_id=TG_CONN_ID,
    #     chat_id=TG_CHAT_ID,
    #     text=f"Start in {datetime.now()}",
    #     dag=dag,
    # )


    # Проверка доступности БД
    check_db = SQLExecuteQueryOperator(
        task_id="check_db",
        conn_id=PG_CONN_ID,
        sql="check_db.sql",
        params={'pg_raw_schema': PG_RAW_SCHEMA}
    )

    # Создание необходимых схем
    create_schemas = SQLExecuteQueryOperator(
        task_id="create_schemas",
        conn_id=PG_CONN_ID,
        sql="create_schemas.sql",
    )

    # Очистка stage папки
    @task()
    def clear_stage_folder():
        from headhunter.scripts.draft_new import clear_stage_folder_func
        clear_stage_folder_func(STAGE_FOLDER_PATH)

    # Получение списка таблиц stage слоя
    get_tables_name = SQLExecuteQueryOperator(
        task_id="get_tables_name",
        conn_id=PG_CONN_ID,
        sql="get_tables_stage.sql",
        params={'pg_raw_schema': PG_RAW_SCHEMA,}
        )

    # Очистка stage таблиц
    clear_stage_tables = SQLExecuteQueryOperator.partial(
        task_id="clear_stage_tables",
        conn_id=PG_CONN_ID,
        map_index_template="table processed: {{ task.parameters[0] }}",
        sql="truncate_stage_tables.sql",
        params={
            'pg_raw_schema': PG_RAW_SCHEMA,
            }
    ).expand(parameters=get_tables_name.output)

    # Получение данных по справочникам
    @task_group(group_id="stage_small_sources")
    def tg_stage_small_sources():
        
        # По списку необходимых эндпоинтов получаем список списков
        @task()
        def get_name_simple_endpoints():
            endpoints = get_list_of_lists_func("HH_SIMPLE_ENDPOINTS")
            return endpoints
        
        # Получаем данные по эндпоинту. Динамическая генерация тасок
        @task(map_index_template="{{ my_custom_map_index }}")
        def get_data_simple_endpoints(endpoint):
            from headhunter.scripts.draft_new import get_data_func

            context = get_current_context()
            context["my_custom_map_index"] = "Process: " + str(endpoint[0])
            
            # По эндпоинту забираем данные через API
            get_data_func(HH_BASE_URL, endpoint[0], HH_HEADERS, STAGE_FOLDER_PATH_JSON)
        

        # Порядок выполнения тасок внутри группы
        name_simple_endpoints_list = get_name_simple_endpoints()
        data_simple_endpoints = get_data_simple_endpoints.partial().expand(endpoint=name_simple_endpoints_list)


        # По списку необходимых словарей, получаем список списков
        @task()
        def get_name_dictionaries():
            dictionaties = get_list_of_lists_func("HH_LIST_PROCESS_DICTS")
            return dictionaties
        
        # Получаем данные по необходимым словарям
        @task(map_index_template="{{ my_custom_map_index }}")
        def transform_load_data_dictionaries(name_dict):
            from headhunter.scripts.draft_new import transform_data_dictionaries_func

            context = get_current_context()
            context["my_custom_map_index"] = "Process: " + str(name_dict[0])

            PG_TABLE = name_dict[0]
            parse_file = STAGE_FOLDER_PATH_JSON + "dictionaries"
            transform_data_dictionaries_func(
                parse_file,
                PG_CONN_ID,
                PG_RAW_SCHEMA,
                PG_TABLE
            )

        # Получаем данные по странам / городам
        @task(trigger_rule='all_done')
        def transform_load_data_areas():
            from headhunter.scripts.draft_new import transform_data_areas_func
            PG_TABLE = "areas"
            parse_file = STAGE_FOLDER_PATH_JSON + PG_TABLE
            transform_data_areas_func(
                parse_file,
                PG_CONN_ID,
                PG_RAW_SCHEMA,
                PG_TABLE
            )
        
        # Получаем данные по линиям / станциям метро
        @task(trigger_rule='all_done')
        def transform_load_data_metro():
            from headhunter.scripts.draft_new import transform_data_metro_func
            PG_TABLE = "metro"
            parse_file = STAGE_FOLDER_PATH_JSON + PG_TABLE
            transform_data_metro_func(
                parse_file,
                PG_CONN_ID,
                PG_RAW_SCHEMA,
                PG_TABLE
            )

        # Получаем данные по проф ролям
        @task(trigger_rule='all_done')
        def transform_load_data_professional_roles():
            from headhunter.scripts.draft_new import transform_data_professional_roles_func
            PG_TABLE = "professional_roles"
            parse_file = STAGE_FOLDER_PATH_JSON + PG_TABLE
            transform_data_professional_roles_func(
                parse_file,
                PG_CONN_ID,
                PG_RAW_SCHEMA,
                PG_TABLE
            )


        # Порядок выполнения тасок внутри группы
        name_dictionaries = get_name_dictionaries()
        data_dictionaries = transform_load_data_dictionaries.partial().expand(name_dict=name_dictionaries)
        
        (
            name_simple_endpoints_list 
            >> data_simple_endpoints 
            >> name_dictionaries 
            >> [
                data_dictionaries,
                transform_load_data_areas(),
                transform_load_data_metro(),
                transform_load_data_professional_roles()
            ]
        )

    # Получение данных по вакансиям
    @task_group(group_id="stage_vacancies")
    def tg_stage_vacancies():
        get_area_parents_id = SQLExecuteQueryOperator(
            task_id="get_area_parents_id",
            conn_id=PG_CONN_ID,
            sql="get_area_parents_id.sql",
            params={'pg_raw_schema': PG_RAW_SCHEMA}
        )

        # Разбиваем на подгруппы по наименованию вакансии, чтобы обойти лимит по выгрузке
        subgroup_list = []
        for search_text in HH_LIST_TEXT_SEARCH:
            search_text_list = []
            search_text_list.append(search_text)
            @task_group(group_id=f"group_{search_text.replace(' ', '_')}")
            def tg1():
                
                # Получаем URL вакансии и пишем в файл txt
                @task(task_id="get_url", map_index_template="{{ my_custom_map_index }}")
                def get_url_vacancies(area_id, search_text):
                    from headhunter.scripts.draft_new import get_url_vacancies_func

                    context = get_current_context()
                    context["my_custom_map_index"] = "Process: " + str(area_id[0])

                    get_url_vacancies_func(
                        base_url=HH_BASE_URL,
                        endpoint=HH_VACANCIES_ENDPOINT,
                        headers=HH_HEADERS,
                        stage_folder_path=STAGE_FOLDER_PATH,
                        area=area_id,
                        search_text=search_text,
                        search_date=Variable.get("HH_PROCESS_DS")
                    )

                # Проходим по URL в файле и забираем данные через API
                @task(task_id="get_data")
                def get_data_vacancies(search_text):
                    from headhunter.scripts.draft_new import get_data_vacancies_func

                    get_data_vacancies_func(
                        stage_folder_path=STAGE_FOLDER_PATH,
                        endpoint=HH_VACANCIES_ENDPOINT,
                        headers=HH_HEADERS,
                        search_text=search_text
                    )

                # Порядок выполнения тасок внутри группы
                url_vacancies = get_url_vacancies.partial().expand(area_id=get_area_parents_id.output, search_text=search_text_list)
                data_vacancies = get_data_vacancies(search_text=search_text)
                url_vacancies >> data_vacancies
            
            subgroup_list.append(tg1())
            chain(*subgroup_list)
        
        # Преобразуем и сохраняем данные в БД
        @task()
        def transform_load_data_vacancies():
            from headhunter.scripts.draft_new import transform_data_vacancie_func

            transform_data_vacancie_func(
                stage_folder_path=STAGE_FOLDER_PATH,
                pg_conn_id=PG_CONN_ID,
                pg_schema=PG_RAW_SCHEMA,
                pg_table="vacancies"
            )

        
        subgroup_list >> transform_load_data_vacancies()

    # Получение данных по работадателям
    # Так как количество работадателей большое, не использовал динамические таски
    @task_group(group_id="stage_employers")
    def tg_stage_employers():
        
        # Получаем идентификаторы работадателей по текущему батчу вакансий
        get_employers_id = SQLExecuteQueryOperator(
            task_id="get_employers_id",
            conn_id=PG_CONN_ID,
            sql="get_employers_id.sql",
            params={'pg_raw_schema': PG_RAW_SCHEMA},
            trigger_rule='all_success'
        )

        # Преобразуем в список списков
        @task()
        def set_variables_employers_id(employers_id):
            employers_id_str = str(employers_id).replace("[", "").replace("]", "")
            Variable.set(key="HH_EMPLOYERS_ID", value=employers_id_str)

        # Получаем данные по работадателям
        @task()
        def get_data_emplyers():
            from headhunter.scripts.draft_new import get_data_employers_func

            employers_id_str = Variable.get("HH_EMPLOYERS_ID")
            process_id_list = [int(x) for x in employers_id_str.split(", ")]

            get_data_employers_func(
                base_url=HH_BASE_URL,
                endpoint=HH_EMPLOYERS_ENDPOINT,
                headers=HH_HEADERS,
                stage_folder_path=STAGE_FOLDER_PATH,
                process_id_list=process_id_list
            )
            
        # Преобразуем и сохраняем данные в БД
        @task()
        def transform_load_data_employers():
            from headhunter.scripts.draft_new import transform_data_employer_func

            transform_data_employer_func(
                stage_folder_path=STAGE_FOLDER_PATH,
                pg_conn_id=PG_CONN_ID,
                pg_schema=PG_RAW_SCHEMA,
                pg_table="employers"
            )

        (
            get_employers_id
            >> set_variables_employers_id(get_employers_id.output)
            >> get_data_emplyers()
            >> transform_load_data_employers()
        )

    trigger_next_dag = TriggerDagRunOperator(
        task_id=f'trigger_{NEXT_DAG_ID}',
        trigger_dag_id=NEXT_DAG_ID,
        wait_for_completion=True,
        poke_interval=30
    )


    @task()
    def end():
        pass

    (
        start()
        >> check_db
        >> create_schemas
        >> clear_stage_folder()
        >> get_tables_name
        >> clear_stage_tables
        >> tg_stage_small_sources()
        >> tg_stage_vacancies()
        >> tg_stage_employers()
        >> trigger_next_dag
        >> end()
        )

process_dag()
