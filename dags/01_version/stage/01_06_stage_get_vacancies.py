from __future__ import annotations

import datetime
import pendulum
import logging

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.models.baseoperator import chain

from headhunter.scripts.func import get_vacancies_data
from headhunter.scripts.func import tmp_stage_to_pg


# logging
log = logging.getLogger(__name__)

DAG_ID = "01_06_stage_get_vacancies"
NEXT_DAG_ID = "01_07_stage_get_employers"

TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")

HH_APPNAME = Variable.get("HH_APPNAME")
HH_LIST_TEXT_SEARCH = Variable.get("HH_LIST_TEXT_SEARCH").split(', ')
HH_URL_VACANCIES = Variable.get("HH_URL_VACANCIES")
HH_PATH_DF_STAGE = Variable.get("HH_PATH_DF_STAGE")

PG_CONN_ID = "docker_blade10_db"
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
PG_TABLE_NAME = "vacancies"


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    end_date=None,
    max_active_tasks=5,
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["stage", "star"],
    dag_display_name=DAG_ID,
    template_searchpath=["/opt/airflow/include/sql/hh/"],
) as dag:
    
    start = EmptyOperator(
        task_id="start",
    )

    get_area_parents_id = PostgresOperator(
        task_id="get_area_parents_id",
        postgres_conn_id=PG_CONN_ID,
        sql="get_area_parents_id.sql",
        params={'pg_raw_schema': PG_RAW_SCHEMA}
    )

    # <--- general group --->
    with TaskGroup(group_id="group_transform_data") as group_transform_data:
        group_list = []
        for text_search in HH_LIST_TEXT_SEARCH:
            group_id = f"group_{text_search.replace(' ', '_')}"
            
            # <--- vacancy groups --->
            with TaskGroup(group_id=group_id) as tg2:
                transform_data_vacancies = PythonOperator.partial(
                    task_id=f"{text_search.replace(' ', '_')}_data",
                    python_callable=get_vacancies_data,
                    map_index_template="area id: {{ task.op_args[0] }}",
                    trigger_rule="all_success",
                    op_kwargs={
                        "url": HH_URL_VACANCIES,
                        "headers": HH_APPNAME,
                        "search_date": '{{ macros.ds_add(ds, -1) }}',
                        "search_text": text_search,
                        }
                ).expand(op_args=get_area_parents_id.output)
            
            group_list.append(tg2)
    
        chain(*group_list)

    load_data = PythonOperator(
        task_id=f"load_data",
        python_callable=tmp_stage_to_pg,
        op_args=[PG_TABLE_NAME,],
        op_kwargs={
            "PG_CONN_ID": PG_CONN_ID,
            "PG_RAW_SCHEMA": PG_RAW_SCHEMA,
            "tmp_stage_path_df": HH_PATH_DF_STAGE + PG_TABLE_NAME + ".csv"
        }
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id=f'trigger_{NEXT_DAG_ID}',
        trigger_dag_id=NEXT_DAG_ID,
        wait_for_completion=True,
    )

    stop = EmptyOperator(
        task_id="stop"
    )

# pipline
(
    start
    >> get_area_parents_id
    >> group_transform_data
    >> load_data
    >> trigger_next_dag
    >> stop
)