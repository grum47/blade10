from __future__ import annotations

import datetime
import pendulum
import logging

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


# logging
logger = logging.getLogger(__name__)

DAG_ID = "01_01_stage_start"
NEXT_DAG_ID = "01_02_stage_get_dictionaries"
TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")

with DAG(
    dag_id=DAG_ID,
    schedule="5 5 * * *",  # “At 8:05 MSK”
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    end_date=None,
    max_active_tasks=1,
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["stage", "star"],
    dag_display_name=DAG_ID,
    template_searchpath=["/opt/airflow/include/sql/hh/"],
) as dag:
    
    start = EmptyOperator(
        task_id="start",
    )

    chek_db = PostgresOperator(
        task_id="check_db",
        postgres_conn_id="docker_blade10_db",
        sql="check_db.sql",
        params={'pg_raw_schema': PG_RAW_SCHEMA}
    )

    create_schemas = PostgresOperator(
        task_id="create_schemas",
        postgres_conn_id="docker_blade10_db",
        sql="create_schemas.sql",
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
        >> chek_db
        >> create_schemas
        >> trigger_next_dag
        >> stop
    )