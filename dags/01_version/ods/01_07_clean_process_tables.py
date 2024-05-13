from __future__ import annotations

import datetime
import pendulum
import logging

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.models.baseoperator import chain


# logging
log = logging.getLogger(__name__)

DAG_ID = "01_07_clean_process_tables"
TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")
PG_CONN_ID = "docker_blade10_db"
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
PG_CLEAN_SCHEMA = Variable.get("PG_CLEAN_SCHEMA")
HH_LIST_CLEAN_OTHER_TABLES = [[x] for x in Variable.get("HH_LIST_CLEAN_OTHER_TABLES").split(', ')]


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2024, 4, 22, tz="UTC"),
    end_date=None,
    max_active_tasks=50,
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["clean", "star"],
    dag_display_name=DAG_ID,
    template_searchpath=["/opt/airflow/include/sql/hh/"],
) as dag:
    
    start = EmptyOperator(
        task_id="start",
    )

    get_list_all_tables = PostgresOperator(
        task_id="get_list_all_tables",
        postgres_conn_id=PG_CONN_ID,
        sql="get_list_all_tables.sql",
        params={'raw_schema': PG_RAW_SCHEMA}
    )

    # <--- global group --->
    with TaskGroup(group_id="processed_tables") as processed_tables:

        # <--- all_tables --->
        with TaskGroup(group_id="all_tables") as all_tables:

            process_all_tables = PostgresOperator.partial(
                task_id=f"process_all_tables",
                postgres_conn_id=PG_CONN_ID,
                map_index_template="table processed: {{ task.parameters[0] }}",
                sql=f"process_all_tables.sql",
                params={
                    'pg_clean_schema': PG_CLEAN_SCHEMA,
                    'pg_raw_schema': PG_RAW_SCHEMA
                    }
            ).expand(parameters=get_list_all_tables.output)
        
        # <--- areas_table --->
        with TaskGroup(group_id="areas_table") as areas_table:

            process_areas_table = PostgresOperator.partial(
                task_id="process_areas_table",
                postgres_conn_id=PG_CONN_ID,
                map_index_template="table processed: {{ task.sql[0].split('.')[1].split(';')[0] }}",
                params={
                    'pg_clean_schema': PG_CLEAN_SCHEMA,
                    'pg_raw_schema': PG_RAW_SCHEMA
                    }
            ).expand(
                sql=HH_LIST_CLEAN_OTHER_TABLES
                )


    stop = EmptyOperator(
        task_id="stop",
    )

    send_tg = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id=TG_CONN_ID,
        chat_id=TG_CHAT_ID,
        text=f"""Таблицы сырого слоя обоработаны {{{{ macros.ds_add(ds, -1) }}}}""",
    )

# pipline
(
    start
    >> get_list_all_tables
    >> processed_tables
    >> stop
    >> send_tg
)