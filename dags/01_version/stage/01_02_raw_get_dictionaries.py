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

DAG_ID = "01_02_raw_get_dictionaries"
DAG_DISPLAY_NAME = DAG_ID
TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")
PG_CONN_ID = "docker_blade10_db"
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
HHTP_CONN_ID = "http_connection_hh"
ENDPOINT="dictionaries"


def func_get_list_names_dictionaries():
    """Получаем cправочников полей и сущностей, используемых в API

    Returns:
        list: список имен справочников
    """
    context = get_current_context()
    data_str = context["ti"].xcom_pull(task_ids="get_data_dictionaries")
    data = json.loads(data_str)
    list_names_dictionaries = list(data.keys())
    list_names_dictionaries = [[item] for item in list_names_dictionaries]
    return list_names_dictionaries


def func_transform_load_data_dictionaries(name_dict):
    postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    postgres_engine = postgres_hook.get_sqlalchemy_engine()

    context = get_current_context()
    data_str = context["ti"].xcom_pull(task_ids="get_data_dictionaries")
    data = json.loads(data_str)
    df = pd.json_normalize(data[f'{name_dict}'])

    # df['md_ins_date'] = context['ds']
    # df['md_upd_date'] = context['ds']
    # df['md_ins_date'] = pd.to_datetime(df['md_ins_date'], format='%Y-%m-%d')
    # df['md_upd_date'] = pd.to_datetime(df['md_upd_date'], format='%Y-%m-%d')
    # df['md_is_activ'] = 1
    # df['md_is_delete'] = 0
    df['process_dttm'] = pendulum.now('Europe/Moscow')
    
    log.info(f" ::: {name_dict}: df.shape = {df.shape}")
    df.to_sql(
        name=name_dict,
        con=postgres_engine,
        schema=PG_RAW_SCHEMA,
        if_exists='replace',
        index=False
        )
    log.info(f" ::: df insert into table {name_dict}")
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

    get_data_dictionaries = HttpOperator(
        task_id="get_data_dictionaries",
        http_conn_id=HHTP_CONN_ID,
        method="GET",
        endpoint=ENDPOINT,
        headers={'Content-Type': 'hh-recommender'},
    )

    get_list_names_dictionaries = PythonOperator(
        task_id="get_list_names_dictionaries",
        python_callable=func_get_list_names_dictionaries
    )

    transform_load_data_dictionaries = PythonOperator.partial(
        task_id="transform_data_dictionaries",
        python_callable=func_transform_load_data_dictionaries
    ).expand(op_args=get_list_names_dictionaries.output)

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_01_03_raw_get_areas',
        trigger_dag_id='01_03_raw_get_areas',
        wait_for_completion=True,
    )

    stop = EmptyOperator(
        task_id="stop",
    )

    send_tg = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id=TG_CONN_ID,
        chat_id=TG_CHAT_ID,
        text=f"Данные по справочникам за {{{{ macros.ds_add(ds, -1) }}}} загружены.",
    )
    # pipline
    start >> get_data_dictionaries >> get_list_names_dictionaries >> transform_load_data_dictionaries >> trigger_next_dag
    trigger_next_dag >> stop >> send_tg