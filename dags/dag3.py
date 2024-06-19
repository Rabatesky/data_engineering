"""
Первый даг интересный
"""
from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'home_pc',
    'start_date': days_ago(2),
    'poke_interval': 600
}

dag = DAG('test_connect_postgres', default_args=default_args, schedule_interval='* * * * *',
          catchup=False, max_active_runs=1, max_active_tasks=10, tags=["idiot"])

dummy = DummyOperator(
    task_id="dummy",
    dag=dag)

echo_ds = BashOperator(
    task_id='echo_ds',
    bash_command='echo {{ ds }}',
    dag=dag
)

def first_func():
    logging.info('hello world')

hello_world = PythonOperator(
    task_id='hello_world',
    python_callable=first_func,
    dag=dag
)
dummy >> [echo_ds, hello_world]