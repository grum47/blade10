from __future__ import annotations

import logging
import datetime
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from headhunter.scripts.func import tmp_stage_to_pg
from headhunter.scripts.func import get_profesional_roles_data

# logging
log = logging.getLogger(__name__)

DAG_ID = "01_03_stage_get_professional_roles"
NEXT_DAG_ID = "01_04_stage_get_areas"
DAG_DISPLAY_NAME = DAG_ID
TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")

HH_APPNAME = Variable.get("HH_APPNAME")
HH_PATH_DF_STAGE = Variable.get("HH_PATH_DF_STAGE")
HH_URL_PROFESSIONAL_ROLES = Variable.get('HH_URL_PROFESSIONAL_ROLES')

PG_CONN_ID = "docker_blade10_db"
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
PG_TABLE_NAME = 'professional_roles'
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
        task_id="start"
    )

    get_data = EmptyOperator(
        task_id="get_data"
    )

    get_transform_data = PythonOperator(
        task_id="get_transform_data",
        python_callable=get_profesional_roles_data,
        op_kwargs={
            "url": HH_URL_PROFESSIONAL_ROLES,
            "headers": HH_APPNAME,
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
        task_id="stop"
    )

# pipline
(
    start
    >> get_data
    >> get_transform_data
    >> load_data
    >> trigger_next_dag
    >> stop
)