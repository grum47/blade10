from __future__ import annotations

import time
import datetime
import pendulum
import logging

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

from headhunter.scripts.hh_employers import get_employers, split_list

# logging
log = logging.getLogger(__name__)

DAG_ID = "01_06_raw_get_employers"
TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")
HH_APPNAME = Variable.get("HH_APPNAME")
HH_URL_EMPLOYERS = Variable.get("HH_URL_EMPLOYERS")
PG_CONN_ID = "docker_blade10_db"
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
PG_TABLE_NAME = "employers"

def copy_to_pg_tmp():
    import pandas as pd
    postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    postgres_engine = postgres_hook.get_sqlalchemy_engine()

    df = pd.read_csv('/opt/airflow/tmp_stage/employers.csv', sep=';')
    df['process_dttm'] = pendulum.now('Europe/Moscow')
    log.info(f" ::: df.shape = {df.shape}")

    df.to_sql(
        name=PG_TABLE_NAME,
        con=postgres_engine,
        schema=PG_RAW_SCHEMA,
        if_exists='replace',
        index=False
        )
    log.info(f" ::: df insert into table {PG_TABLE_NAME}")
    return True


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2024, 4, 22, tz="UTC"),
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
    with TaskGroup(group_id="group_transform_data_employers") as group_transform_data_employers:
        group_lists = []
        lists_employers_id_last_part = Variable.get('HH_LISTS_EMPLOYERS_ID_LAST_PART')
        
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

        chain(*group_lists)

    load_data_to_db = PythonOperator(
        task_id="load_data_to_db",
        python_callable=copy_to_pg_tmp
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_01_07_clean_process_tables',
        trigger_dag_id='01_07_clean_process_tables',
        wait_for_completion=True,
    )

    stop = EmptyOperator(
        task_id="stop",
    )

    send_tg = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id=TG_CONN_ID,
        chat_id=TG_CHAT_ID,
        text=f"""Данные по работадателям за {{{{ macros.ds_add(ds, -1) }}}} загружены""",
    )

# pipline
(
    start 
    >> get_employers_id
    >> get_lists_employers_id
    >> group_transform_data_employers
    >> load_data_to_db
    >> trigger_next_dag
    >> stop
    >> send_tg
)
