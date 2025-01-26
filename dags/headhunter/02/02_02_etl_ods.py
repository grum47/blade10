import os
from datetime import datetime, timedelta
import logging

from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task, task_group
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# logging
logger = logging.getLogger(__name__)

DAG_ID = os.path.realpath(__file__).split("/")[-1].split(".")[0]
NEXT_DAG_ID = "02_03_etl_dds"

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
    template_searchpath=["/opt/airflow/include/sql/hh/", "/opt/airflow/include/sql/hh/ods"],
)
def process_dag():

    PG_CONN_ID = "postgres_conn"
    PG_RAW_SCHEMA = Variable.get("PG_RAW_SCHEMA")
    PG_CLEAN_SCHEMA = Variable.get("PG_CLEAN_SCHEMA")

    HH_PROCESS_DS = Variable.get("HH_PROCESS_DS")
    HH_LIST_CLEAN_OTHER_TABLES = [[x] for x in Variable.get("HH_LIST_CLEAN_OTHER_TABLES").split(', ')]

    @task()
    def start():
        pass

    get_list_total_tables = SQLExecuteQueryOperator(
        task_id="get_list_total_tables",
        conn_id=PG_CONN_ID,
        sql="ods/get_list_total_tables.sql",
        params={'raw_schema': PG_RAW_SCHEMA}
    )

    process_total_tables = SQLExecuteQueryOperator.partial(
        task_id=f"process_total_tables",
        conn_id=PG_CONN_ID,
        map_index_template="table processed: {{ task.parameters[0] }}",
        sql=f"ods/process_total_tables.sql",
        params={
            'pg_clean_schema': PG_CLEAN_SCHEMA,
            'pg_raw_schema': PG_RAW_SCHEMA,
            'process_ds': HH_PROCESS_DS,
            }
    ).expand(parameters=get_list_total_tables.output)

    process_other_tables = SQLExecuteQueryOperator.partial(
        task_id="process_other_tables",
        conn_id=PG_CONN_ID,
        map_index_template="table processed: {{ task.sql[0].split('.')[1].split(';')[0] }}",
        params={
            'pg_clean_schema': PG_CLEAN_SCHEMA,
            'pg_raw_schema': PG_RAW_SCHEMA,
            'process_ds': HH_PROCESS_DS,
            }
    ).expand(sql=HH_LIST_CLEAN_OTHER_TABLES)

    trigger_next_dag = TriggerDagRunOperator(
        task_id=f'trigger_{NEXT_DAG_ID}',
        trigger_dag_id=NEXT_DAG_ID,
        wait_for_completion=True,
        poke_interval=20
    )

    @task()
    def end():
        pass
    
    (
        start()
        >> get_list_total_tables
        >> [process_total_tables, process_other_tables]
        >> trigger_next_dag
        >> end()
    )

process_dag()
