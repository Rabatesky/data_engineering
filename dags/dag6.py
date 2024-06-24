from datetime import datetime
from datetime import timedelta
from airflow import DAG


import pandas as pd
import numpy as np
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.sql import PostgresOperator

default_args = {
    'owner': 'home_pc',                             #Владелец дага
    'retries': 1,                                   #Количество перезапусков в случае падения
    'retry_delay': timedelta(minutes=0.1),            #Время через которое стоит выполнить перезапуск
    'start_date': datetime(2024, 6, 22),             #Дата первого запуска, отработает столько раз сколько прошло времени с первого запуска
    'sla': timedelta(hours=2),                      #Время за которое поидее даг уже должен закончить работу, если нет то придёт уведомление что таск работал дольше
    'email': ['demon-310@mail.ru'],  # Нуже для отправки писем сигнализирующих о падении или перезапуске дага
    'email_on_failure': False,  # Отправка ошибок
    'email_on_retry': False     # Отправка в случае retry
}


dag = DAG('Test_table', default_args=default_args, schedule_interval='0 0 * * *',
          max_active_runs=1, max_active_tasks=10, tags=["idiot"], catchup=False)

def insert_df(df):
    columns = ', '.join([f'"{col}"' for col in df.columns])
    values = ', '.join([f'("{val}")' for val in df.values.flatten()])
    insert_query = f"""
    INSERT INTO new_table ({columns})
    VALUES ({values});
    """
    return insert_query

def read_data(**kwargs):
    pg_hook = PostgresHook('1_my_postgres_test')
    con = pg_hook.get_conn()
    data = pd.read_sql_query("Select * from california.california_housing", con)
    kwargs['ti'].xcom_push(value=data, key='dataframe')

def re_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='dataframe')
    data = data.dtop(['index'], axis=1)
    kwargs['ti'].xcom_push(value=data, key='dataframe_reload')

def load_data(**kwargs):
    pg_hook = PostgresHook('1_my_postgres_test')
    data = kwargs['ti'].xcom_pull(key='dataframe_reload')
    pg_hook.run(insert_df(data))


read_data = PythonOperator(
    task_id='read_data',
    python_callable=read_data,
    provide_context=True,
    dag=dag
)

drop_table_task = PostgresOperator(
    task_id='drop_table_task',
    sql="Drop table california.california_housing",
    postgres_conn_id='1_my_postgres_test',
    dag=dag
)

create_table_task = PostgresOperator(
    task_id='create_table_task',
    sql="CREATE TABLE california.california_housing ("
                      "MedInc float8 NULL,"
                      "HouseAge float8 NULL,"
                      "AveRooms float8 NULL,"
                      "AveBedrms float8 NULL,"
                      "Population float8 NULL,"
                      "AveOccup float8 NULL,"
                      "Latitude float8 NULL,"
                      "Longitude float8 NULL,"
                      "MedHouseVal float8 NULL)",
    postgres_conn_id='1_my_postgres_test',
    dag=dag
)

re_data = PythonOperator(
    task_id='re_data',
    python_callable=re_data,
    provide_context=True,
    dag=dag
)

load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

read_data >> drop_table_task >> [create_table_task, re_data] >> load_data
