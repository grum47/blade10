from __future__ import annotations

import datetime
import pendulum
import logging

from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


# logging
log = logging.getLogger(__name__)

DAG_ID = "01_09_dds"
NEXT_DAG_ID = "01_10_mart"

TG_CONN_ID = "telegram_connection"
TG_CHAT_ID = Variable.get("TG_CHAT_ID")

PG_CONN_ID = "docker_blade10_db"
PG_ODS_SCHEMA = Variable.get("PG_CLEAN_SCHEMA")

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    end_date=None,
    max_active_tasks=1,
    max_active_runs=1,
    catchup=True,
    dagrun_timeout=datetime.timedelta(minutes=60),
    tags=["dds", "star"],
    dag_display_name=DAG_ID,
    template_searchpath=["/opt/airflow/include/sql/hh/"],
) as dag:
    
    start = EmptyOperator(
        task_id="start",
    )

    # get_name_tables_ods = PostgresOperator(
    #     task_id="get_name_tables_ods",
    #     postgres_conn_id=PG_CONN_ID,
    #     sql="get_tables_ods.sql",
    #     params={'pg_clean_schema': PG_ODS_SCHEMA}
    # )
    
# <----- group process tables ----->
    with TaskGroup(group_id="ods_tables") as ods_tables:
        for table in [
            'categories', 'cities', 'countries', 'education_level', 'employer_type', 'employers', 
            'employment', 'experience', 'language_level', 'lines', 'messaging_status', 'preferred_contact_type', 
            'roles', 'schedule', 'stations', 'vacancies', 'vacancy_type', 'working_days', 
            'working_time_intervals', 'working_time_modes'
            ]:

            process_tables = PostgresOperator(
                task_id=f"create_{table}",
                postgres_conn_id=PG_CONN_ID,
                sql=f"dds/{table}.sql",
            )

    wait_all_tables = EmptyOperator(
        task_id="wait_all_tables"
    )

    trigger_next_dag = EmptyOperator(
        task_id="trigger_next_dag"
    )

    stop = EmptyOperator(
        task_id="stop"
    )

# pipeline
(
    start
    >> ods_tables
    >> wait_all_tables
    >> trigger_next_dag
    >> stop
)