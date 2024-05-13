def copy_to_pg_tmp():
    import pandas as pd
    postgres_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    postgres_engine = postgres_hook.get_sqlalchemy_engine()

    df = pd.read_csv('/opt/airflow/tmp_stage/employers.csv', sep=';')
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


def func_create_variable_list_dict_tables():
    context = get_current_context()
    data = context['ti'].xcom_pull(task_ids="get_list_dictionaries_tables")
    list_dict_tables = [x[0] for x in data]
    Variable.set(key="HH_LIST_DICT_RAW_TABLES", value=list_dict_tables)
    return True