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

dag = DAG('sync_git', default_args=default_args, schedule_interval='* * * * *',
          catchup=False, max_active_runs=1, max_active_tasks=3, tags=["idiot"])

start_sync = BashOperator(
    task_id = 'start_sync',
    bash_command='python3 /airflow/scripts/dag2/git_sync_airflow.py',
    dag=dag
)

start_sync
