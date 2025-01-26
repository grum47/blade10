import os
from datetime import datetime, timedelta
import logging

from airflow.models import Variable
from airflow.decorators import dag, task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# logging
logger = logging.getLogger(__name__)

DAG_ID = os.path.realpath(__file__).split("/")[-1].split(".")[0]
NEXT_DAG_ID = "02_04_etl_marts"

default_args = {
    "owner": "Pavel Evtuhovich",
}

@dag(
    dag_id=DAG_ID,
    tags=["02"],
    start_date=datetime(2024, 8, 1),
    default_args=default_args,
    end_date=None,
    schedule=None,
    max_active_tasks=10,
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    template_searchpath=["/opt/airflow/include/sql/hh/"],
)
def process_dag():
    PG_CONN_ID = "postgres_conn"
    PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
    PG_CLEAN_SCHEMA = Variable.get("PG_CLEAN_SCHEMA")

    HH_PROCESS_DS = Variable.get("HH_PROCESS_DS")

    @task()
    def start():
        pass

    @task_group()
    def tg_ods_to_dds():
        for table in [
            'categories', 'areas', 'education_level', 'employer_type', 'employers', 
            'employment', 'experience', 'language_level', 'lines', 'messaging_status', 'preferred_contact_type', 
            'roles', 'schedule', 'stations', 'vacancies', 'vacancy_type', 'working_days', 
            'working_time_intervals', 'working_time_modes'
            ]:

            process_tables = SQLExecuteQueryOperator(
                task_id=f"create_{table}",
                conn_id=PG_CONN_ID,
                sql=f"dds/{table}.sql",
            )

            process_tables

    @task()
    def wait_all_tables():
        pass

    # trigger_next_dag = TriggerDagRunOperator(
    #     task_id=f'trigger_{NEXT_DAG_ID}',
    #     trigger_dag_id=NEXT_DAG_ID,
    #     wait_for_completion=True,
    # )

    @task()
    def end():
        pass

    (
        start()
        >> tg_ods_to_dds()
        >> wait_all_tables()
        # >> trigger_next_dag
        >> end()
    )

process_dag()