from datetime import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'home_pc',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 19),
    'retries': 0,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=0.1)
}

dag = DAG('test_connect_postgres', default_args=default_args, schedule_interval='* * * * *',
          catchup=False, max_active_runs=1, max_active_tasks=3, tags=["idiot"])
