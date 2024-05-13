from __future__ import annotations

import datetime
import pendulum
import logging

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow.models.baseoperator import chain

from headhunter.scripts.hh_vacancies import get_vacancies_data

# logging
log = logging.getLogger(__name__)

DAG_ID = "01_05_raw_get_vacancies"
TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")
HH_APPNAME = Variable.get("HH_APPNAME")
HH_LIST_TEXT_SEARCH = Variable.get("HH_LIST_TEXT_SEARCH").split(', ')
HH_URL_VACANCIES = Variable.get("HH_URL_VACANCIES")
PG_CONN_ID = "docker_blade10_db"
PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
PG_TABLE_NAME = "vacancies"


def copy_to_pg_tmp():
    import pandas as pd

    postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    postgres_engine = postgres_hook.get_sqlalchemy_engine()

    df = pd.read_csv(f'/opt/airflow/tmp_stage/{PG_TABLE_NAME}.csv', sep=';')

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
    max_active_tasks=5,
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["raw", "star"],
    dag_display_name=DAG_ID,
    template_searchpath=["/opt/airflow/include/sql/hh/"],
) as dag:
    
    start = EmptyOperator(
        task_id="start",
    )

    get_area_parents_id = PostgresOperator(
        task_id="get_area_parents_id",
        postgres_conn_id=PG_CONN_ID,
        sql="get_area_parents_id.sql",
        params={'pg_raw_schema': PG_RAW_SCHEMA}
    )

    # <--- general group --->
    with TaskGroup(group_id="group_transform_data_vacancies") as group_transform_data_vacancies:
        group_list = []
        for text_search in HH_LIST_TEXT_SEARCH:
            group_id = f"group_{text_search.replace(' ', '_')}"
            
            # <--- vacancy groups --->
            with TaskGroup(group_id=group_id) as tg2:
                transform_data_vacancies = PythonOperator.partial(
                    task_id=f"{text_search.replace(' ', '_')}_data",
                    python_callable=get_vacancies_data,
                    map_index_template="area id: {{ task.op_args[0] }}",
                    trigger_rule="all_success",
                    op_kwargs={
                        "url": HH_URL_VACANCIES,
                        "headers": HH_APPNAME,
                        "search_date": '{{ macros.ds_add(ds, -1) }}',
                        "search_text": text_search,
                        }
                ).expand(op_args=get_area_parents_id.output)
            
            group_list.append(tg2)
    
        chain(*group_list)

    load_data_to_db = PythonOperator(
        task_id="load_data_to_db",
        trigger_rule="all_success",
        python_callable=copy_to_pg_tmp,
    )
    
    trigger_next_dag = TriggerDagRunOperator(
        task_id='trigger_01_06_raw_get_employers',
        trigger_dag_id='01_06_raw_get_employers',
        wait_for_completion=True,
    )

    stop = EmptyOperator(
        task_id="stop"
    )

    send_tg = TelegramOperator(
        task_id="send_message_telegram",
        telegram_conn_id=TG_CONN_ID,
        chat_id=TG_CHAT_ID,
        text=f"""Данные по по вакансиям за {{{{ macros.ds_add(ds, -1) }}}} загружены""",
    )

# pipline
(
    start
    >> get_area_parents_id
    >> group_transform_data_vacancies
    >> load_data_to_db
    >> trigger_next_dag
    >> stop
    >> send_tg
)

# sql="COPY blade.{report} FROM '/opt/airflow/tmp_stage/vacancies.csv' DELIMITER ';' CSV HEADER"
