from datetime import datetime
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
import logging

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'home_pc',                             #Владелец дага
    'retries': 1,                                   #Количество перезапусков в случае падения
    'retry_delay': timedelta(minutes=1),            #Время через которое стоит выполнить перезапуск
    'start_date': datetime(2021, 1, 1),             #Дата первого запуска, отработает столько раз сколько прошло времени с первого запуска
    'catchup': False,                               #Если false  то будет запущен только 1 раз в момент запуска(игнор start_date)
    'sla': timedelta(hours=2),                      #Время за которое поидее даг уже должен закончить работу, если нет то придёт уведомление что таск работал дольше
    'execution_timeout': timedelta(seconds=300),    #Максимально время на таск, после этого break
    'trigger_rule': 'all_success'                   #Случаи запуска таска(all_success,all_failed,all_done,one_failed,one_success,none_failed,none_failed_or_skipped,none_skipped,dummy)
}


dag = DAG('test_xcom', default_args=default_args, schedule_interval='10 * * * *',
          max_active_runs=1, max_active_tasks=10, tags=["idiot"])


def explicit_push_func(**kwargs):
    kwargs['ti'].xcom_push(value='Hello world!', key='hi')

def implicit_push_func():
    return 'Some string from function'

explicit_push = PythonOperator(
    task_id='explicit_push',
    python_callable=explicit_push_func,
    provide_context=True,
    dag=dag
)

implicit_push = PythonOperator(
    task_id='implicit_push',
    python_callable=implicit_push_func,
    dag=dag
)