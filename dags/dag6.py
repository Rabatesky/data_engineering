from datetime import datetime
from datetime import timedelta
from airflow import DAG

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

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


dag = DAG('test_postgres_in', default_args=default_args, schedule_interval='0 0 * * *',
          max_active_runs=1, max_active_tasks=10, tags=["idiot"], catchup=False)

def get_new_table_postgres_in():
    pg_hook = PostgresHook('1_my_postgres_test')
    con = pg_hook.get_conn()
    logging.info('0')
    engine = create_engine(f"postgresql://{con.login}:{con.password}@{con.host}:{con.port}/{con.schema}")
    logging.info('1')
    data = pd.read_sql_query("Select * from california.california_housing", con)
    logging.info('2')
    data.to_sql('california_housing', engine, 'california1', 'replace')
    logging.info('3')


test_connect_in = PythonOperator(
    task_id='test_connect_in',
    python_callable=get_new_table_postgres_in,
    provide_context=True,
    dag=dag
)
