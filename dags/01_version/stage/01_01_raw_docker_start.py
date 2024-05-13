from __future__ import annotations

import datetime
import pendulum
import logging

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator


# logging
logger = logging.getLogger(__name__)

DAG_ID = "01_01_raw_start"
CONN_ID = "telegram_connection"
CHAT_ID = "-1002098826303"

with DAG(
    dag_id=DAG_ID,
    schedule="5 5 * * *",  # “At 8:05 MSK”
    start_date=pendulum.datetime(2024, 4, 22, tz="UTC"),
    end_date=None,
    max_active_tasks=1,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["raw", "star"],
    dag_display_name=DAG_ID
) as dag:
    
    start = EmptyOperator(
        task_id="start",
    )

    chek_db = PostgresOperator(
        task_id="check_db",
        postgres_conn_id="docker_blade10_db",
        sql="select schema_name from information_schema.schemata where schema_name = 'blade';",
    )

    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_01_02_raw_get_dictionaries',
        trigger_dag_id='01_02_raw_get_dictionaries',
        wait_for_completion=True,
    )

    send_tg = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id=CONN_ID,
        chat_id=CHAT_ID,
        text=f"Процесс загрузки вакансий старовал {pendulum.now()}",
    )

    stop = EmptyOperator(
        task_id="stop",
    )

    # pipline
    start >> chek_db >> trigger_next_dag >> stop >> send_tg
