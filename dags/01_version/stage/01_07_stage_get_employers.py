from __future__ import annotations

import time
import datetime
import pendulum
import logging

from airflow.models.dag import DAG
from airflow.models import Variable
# from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.telegram.operators.telegram import TelegramOperator

from headhunter.scripts.hh_employers import get_employers, split_list
from headhunter.scripts.func import tmp_stage_to_pg

# logging
log = logging.getLogger(__name__)

DAG_ID = "01_07_stage_get_employers"
NEXT_DAG_ID = "01_08_ods"

TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")

HH_APPNAME = Variable.get("HH_APPNAME")
HH_URL_EMPLOYERS = Variable.get("HH_URL_EMPLOYERS")
HH_PATH_DF_STAGE = Variable.get("HH_PATH_DF_STAGE")

PG_AIRFLOW_ID = "airflow-postgres-1"
PG_CONN_ID = "docker_blade10_db"
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
PG_TABLE_NAME = "employers"


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    end_date=None,
    max_active_tasks=30,
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["raw", "star"],
    dag_display_name=DAG_ID,
    template_searchpath=["/opt/airflow/include/sql/hh/"],
) as dag:
    
    start = EmptyOperator(
        task_id="start",
    )

    get_employers_id = PostgresOperator(
        task_id="get_employers_id",
        postgres_conn_id=PG_CONN_ID,
        sql="get_employers_id.sql",
        params={'pg_raw_schema': PG_RAW_SCHEMA}
    )

    get_lists_employers_id = PythonOperator(
        task_id="get_lists_employers_id",
        python_callable=split_list,
        op_kwargs={
            "return_value": get_employers_id.output
        }
    )

    # <--- general group --->
    lists_employers_id_last_part = Variable.get('HH_LISTS_EMPLOYERS_ID_LAST_PART')
    with TaskGroup(group_id="group_transform_data_employers") as group_transform_data_employers:
        group_lists = []        
        # <--- employers groups are dynamic in parts --->
        for part in range(int(lists_employers_id_last_part) + 1):
            lists_employers_id = Variable.get(f'HH_LISTS_EMPLOYERS_ID_{part}')
            lists_employers_id = lists_employers_id.replace('[', '').replace(']', '').split(', ')
            lists_employers_id = [[int(x)] for x in lists_employers_id]

            with TaskGroup(group_id=f"processed_employers_part_{part}") as tg2:
                transform_data_employers = PythonOperator.partial(
                    task_id=f"transform_data_employers_{part}",
                    map_index_template="employer id: {{ task.op_args[0] }}",    
                    python_callable=get_employers,
                    op_kwargs={
                        "url": HH_URL_EMPLOYERS,
                        "headers": HH_APPNAME,
                    }
                ).expand(op_args=lists_employers_id)
            group_lists.append(tg2)
    # chain(*group_lists)

    load_data = PythonOperator(
        task_id=f"load_data",
        trigger_rule='all_done',
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
        task_id="stop",
    )

# pipline
(
    start 
    >> get_employers_id
    >> get_lists_employers_id
    >> group_transform_data_employers
    >> load_data
    >> trigger_next_dag
    >> stop
)
