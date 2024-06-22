from datetime import datetime
from datetime import timedelta
from airflow import DAG
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'home_pc',                             #Владелец дага
    'retries': 1,                                   #Количество перезапусков в случае падения
    'retry_delay': timedelta(minutes=1),            #Время через которое стоит выполнить перезапуск
    'start_date': datetime(2024, 6, 22),             #Дата первого запуска, отработает столько раз сколько прошло времени с первого запуска
    'sla': timedelta(hours=2),                      #Время за которое поидее даг уже должен закончить работу, если нет то придёт уведомление что таск работал дольше
}


dag = DAG('test_postgres', default_args=default_args, schedule_interval='0 0 * * *',
          max_active_runs=1, max_active_tasks=10, tags=["idiot"], catchup=False)

def get_new_table_postgres():
    pg_hook = PostgresHook('1_my_postgres_test')
    con = pg_hook.get_conn()
    logging.info(pg_hook, con)


test_connect = PythonOperator(
    task_id='test_connect',
    python_callable=get_new_table_postgres,
    provide_context=True,
    dag=dag
)
