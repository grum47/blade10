from __future__ import annotations

import logging
import datetime
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from headhunter.scripts.func import get_list_names_dictionaries
from headhunter.scripts.func import transform_data_dictionaries
from headhunter.scripts.func import tmp_stage_to_pg

# logging
log = logging.getLogger(__name__)

DAG_ID = "01_02_stage_get_dictionaries"
NEXT_DAG_ID = "01_03_stage_get_professional_roles"
DAG_DISPLAY_NAME = DAG_ID

TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")

PG_CONN_ID = "docker_blade10_db"
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")

HHTP_CONN_ID = "http_connection_hh"
ENDPOINT="dictionaries"
HH_PATH_DF_STAGE = Variable.get("HH_PATH_DF_STAGE")


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    end_date=None,
    max_active_tasks=5,
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["raw", "star"],
    dag_display_name=DAG_DISPLAY_NAME,
) as dag:

    start = EmptyOperator(
            task_id="start",
        )

    # TODO: Переделать
    get_data = HttpOperator(
        task_id="get_data",
        http_conn_id=HHTP_CONN_ID,
        method="GET",
        endpoint=ENDPOINT,
        headers={'Content-Type': 'hh-recommender'},
    )

    get_list_names = PythonOperator(
        task_id="get_list_names",
        python_callable=get_list_names_dictionaries,
        op_kwargs={
            'process_dicts': Variable.get('HH_LIST_PROCESS_DICTS')
            }
    )

    transform_data = PythonOperator.partial(
        task_id="transform_data",
        python_callable=transform_data_dictionaries,
        map_index_template="transform: {{ task.op_args[0] }}",
        op_kwargs={
            "tmp_stage_path_df": HH_PATH_DF_STAGE + "{{ task.op_args[0] }}" + ".csv"
            }
    ).expand(op_args=get_list_names.output)

    load_data = PythonOperator.partial(
        task_id=f"load_data",
        python_callable=tmp_stage_to_pg,
        map_index_template="load: {{ task.op_args[0] }}",
        op_kwargs={
            "PG_CONN_ID": PG_CONN_ID,
            "PG_RAW_SCHEMA": PG_RAW_SCHEMA,
            "PG_TABLE_NAME": '{{ task.op_args[0] }}',
            "tmp_stage_path_df": HH_PATH_DF_STAGE + '{{ task.op_args[0] }}' + ".csv"
            }
    ).expand(op_args=get_list_names.output)

    trigger_next_dag = TriggerDagRunOperator(
        task_id=f'trigger_{NEXT_DAG_ID}',
        trigger_dag_id=NEXT_DAG_ID,
        wait_for_completion=True,
    )

    stop = EmptyOperator(
        task_id="stop",
    )

# pipline
(
    start
    >> get_data
    >> get_list_names
    >> transform_data
    >> load_data
    >> trigger_next_dag
    >> stop
) 