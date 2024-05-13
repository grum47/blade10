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

DAG_ID = "01_04_raw_get_metro"
DAG_DISPLAY_NAME = DAG_ID
TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")
PG_CONN_ID = "docker_blade10_db"
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
HHTP_CONN_ID = "http_connection_hh"
ENDPOINT="metro"


def func_transform_load_data_metro_lines():
    postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    postgres_engine = postgres_hook.get_sqlalchemy_engine()

    context = get_current_context()
    data_str = context["ti"].xcom_pull(task_ids="get_data_metro")
    data = json.loads(data_str)

    id_list_city = []
    id_list_lines = []
    name_list_lines = []
    hex_color_list_lines = []

    for city in data:
        for line in city['lines']:
            id_list_city.append(city.get('id', None))
            id_list_lines.append(line.get('id', None))
            name_list_lines.append(line.get('name', None))
            hex_color_list_lines.append(line.get('hex_color', None))
    
    df = pd.DataFrame({
                'metro_line_id': id_list_lines,
                'city_id': id_list_city,
                'name': name_list_lines,
                'hex_color': hex_color_list_lines
            }).drop_duplicates().reset_index(drop=True).astype({'city_id': 'int', 'metro_line_id': 'int'}).sort_values(['city_id','metro_line_id'])
    
    df['process_dttm'] = pendulum.now('Europe/Moscow')

    log.info(f" ::: df.shape = {df.shape}")

    df.to_sql(
        name=ENDPOINT + "_lines",
        con=postgres_engine,
        schema=PG_RAW_SCHEMA,
        if_exists='replace',
        index=False
        )
    log.info(f" ::: df insert into table {ENDPOINT + "_lines"}")
    return True

def func_transform_load_data_metro_stations():
    postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    postgres_engine = postgres_hook.get_sqlalchemy_engine()

    context = get_current_context()
    data_str = context["ti"].xcom_pull(task_ids="get_data_metro")
    data = json.loads(data_str)

    id_list_lines = []
    id_list_stations = []
    name_list_stations = []
    lat_list_stations = []
    lon_list_stations = []
    order_list_stations = []

    for city in data:
        for line in city['lines']:
            for station in line['stations']:
                id_list_lines.append(line.get('id', None))
                id_list_stations.append(station.get('id', None).split('.')[1])
                name_list_stations.append(station.get('name', None))
                lat_list_stations.append(station.get('lat', None))
                lon_list_stations.append(station.get('lng', None))
                order_list_stations.append(station.get('order', None))
    
    df = pd.DataFrame({
        'metro_station_id': id_list_stations,
        'metro_line_id': id_list_lines,
        'order_station': order_list_stations,
        'lat': lat_list_stations,
        'lon': lon_list_stations,
        'name': name_list_stations
        }).astype({'metro_line_id': 'int', 'metro_station_id': 'int'}).sort_values(['metro_line_id', 'metro_station_id'])
    
    df['raw_timestamp'] = pendulum.now()

    log.info(f" ::: df.shape = {df.shape}")

    df.to_sql(
        name=ENDPOINT + "_stations",
        con=postgres_engine,
        schema=PG_RAW_SCHEMA,
        if_exists='replace',
        index=False
        )
    log.info(f" ::: df insert into table {ENDPOINT + "_stations"}")
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

    get_data_metro = HttpOperator(
        task_id="get_data_metro",
        http_conn_id=HHTP_CONN_ID,
        method="GET",
        endpoint=ENDPOINT,
        headers={'Content-Type': 'hh-recommender'},
        )

    transform_load_data_metro_lines = PythonOperator(
        task_id="transform_load_data_metro_lines",
        python_callable=func_transform_load_data_metro_lines,
    )

    transform_load_data_metro_stations = PythonOperator(
        task_id="transform_load_data_metro_stations",
        python_callable=func_transform_load_data_metro_stations,
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_01_05_raw_get_vacancies',
        trigger_dag_id='01_05_raw_get_vacancies',
        wait_for_completion=True,
    )

    stop = EmptyOperator(
        task_id="stop",
        trigger_rule="all_success"
    )

    send_tg = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id=TG_CONN_ID,
        chat_id=TG_CHAT_ID,
        text=f"Данные по линиям и станциям метро за {{{{ macros.ds_add(ds, -1) }}}} загружены.",
    )

    start >> get_data_metro >> [
        transform_load_data_metro_lines,
        transform_load_data_metro_stations
        ] >> trigger_next_dag >> stop >> send_tg