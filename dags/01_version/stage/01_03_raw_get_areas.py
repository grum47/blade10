from __future__ import annotations

import json
import logging
import datetime
import pendulum
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator

from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


# logging
log = logging.getLogger(__name__)

DAG_ID = "01_03_raw_get_areas"
DAG_DISPLAY_NAME = DAG_ID
TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")
PG_CONN_ID = "docker_blade10_db"
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
HHTP_CONN_ID = "http_connection_hh"
ENDPOINT="areas"


def func_transform_load_data_areas():
    postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    postgres_engine = postgres_hook.get_sqlalchemy_engine()

    context = get_current_context()
    data_str = context["ti"].xcom_pull(task_ids="get_data_areas")
    data = json.loads(data_str)

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
    
    df['process_dttm'] = pendulum.now('Europe/Moscow')    
    
    log.info(f" ::: df.shape = {df.shape}")
    
    df.to_sql(
        name=ENDPOINT,
        con=postgres_engine,
        schema=PG_RAW_SCHEMA,
        if_exists='replace',
        index=False
        )
    log.info(f" ::: df insert into table {ENDPOINT}")
    return True


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2024, 4, 22, tz="UTC"),
    end_date=None,
    max_active_tasks=1,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["raw", "star"],
    dag_display_name=DAG_DISPLAY_NAME,
) as dag:

    start = EmptyOperator(
            task_id="start",
        )

    get_data_areas = HttpOperator(
        task_id="get_data_areas",
        http_conn_id=HHTP_CONN_ID,
        method="GET",
        endpoint=ENDPOINT,
        headers={'Content-Type': 'hh-recommender'},
        )

    transform_load_data_areas = PythonOperator(
        task_id="transform_load_data_areas",
        python_callable=func_transform_load_data_areas,
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_01_04_raw_get_metro',
        trigger_dag_id='01_04_raw_get_metro',
        wait_for_completion=True,
    )

    stop = EmptyOperator(
        task_id="stop",
    )

    send_tg = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id=TG_CONN_ID,
        chat_id=TG_CHAT_ID,
        text=f"Данные по городам за {{{{ macros.ds_add(ds, -1) }}}} загружены.",
    )

    start >> get_data_areas >> transform_load_data_areas >> trigger_next_dag >> stop >> send_tg