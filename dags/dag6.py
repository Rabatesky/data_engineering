from datetime import datetime
from datetime import timedelta
from airflow import DAG
from sqlalchemy import create_engine

import pyarrow as pa
import pandas as pd
import numpy as np
import logging

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

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


def read_data(**kwargs):
    pg_hook = PostgresHook('1_my_postgres_test')
    con = pg_hook.get_conn()
    data = pd.read_sql_query("Select * from california.california_housing", con)
    kwargs['ti'].xcom_push(value=data, key='dataframe')

def re_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='dataframe')
    data = data.drop(['index'], axis=1)
    data.columns = data.columns.str.lower()
    kwargs['ti'].xcom_push(value=data, key='dataframe_reload')
