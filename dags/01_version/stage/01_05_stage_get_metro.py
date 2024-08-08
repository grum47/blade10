from __future__ import annotations

import json
import logging
import datetime
import pendulum
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.providers.telegram.operators.telegram import TelegramOperator

# from airflow.operators.python import get_current_context
# from airflow.providers.postgres.hooks.postgres import PostgresHook

from headhunter.scripts.func import transform_data_metro
from headhunter.scripts.func import tmp_stage_to_pg

# logging
log = logging.getLogger(__name__)

DAG_ID = "01_05_stage_get_metro"
NEXT_DAG_ID = "01_06_stage_get_vacancies"
DAG_DISPLAY_NAME = DAG_ID

TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")

PG_CONN_ID = "docker_blade10_db"
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
PG_TABLE_NAME = 'metro'

HHTP_CONN_ID = "http_connection_hh"
ENDPOINT="metro"
HH_PATH_DF_STAGE = Variable.get("HH_PATH_DF_STAGE")
HH_PATH_DF = HH_PATH_DF_STAGE + PG_TABLE_NAME + ".csv"


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    end_date=None,
    max_active_tasks=1,
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["stage", "star"],
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

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data_metro,
        op_kwargs={
            "tmp_stage_path_df": HH_PATH_DF
        }
    )

    load_data = PythonOperator(
        task_id=f"load_data",
        python_callable=tmp_stage_to_pg,
        op_args=[PG_TABLE_NAME,],
        op_kwargs={
            "PG_CONN_ID": PG_CONN_ID,
            "PG_RAW_SCHEMA": PG_RAW_SCHEMA,
            "tmp_stage_path_df": HH_PATH_DF
        }
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id=f'trigger_{NEXT_DAG_ID}',
        trigger_dag_id=NEXT_DAG_ID,
        wait_for_completion=True,
    )

    stop = EmptyOperator(
        task_id="stop",
        trigger_rule="all_success"
    )

# pipeline
(
    start
    >> get_data
    >> transform_data
    >> load_data
    >> trigger_next_dag
    >> stop
)