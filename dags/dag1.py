from datetime import datetime
from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator



default_args = {
    'owner': 'test_pc_work',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 13),
    'retries': 1,
    'max_active_runs': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG('my_first_dag', default_args=default_args, schedule_interval='10 * * * *',
          catchup=False, max_active_runs=1, max_active_tasks=3, tags=["idiot"])

man_is_sleep = BashOperator(
    task_id = 'man_is_sleep',
    bash_command='python3 /airflow/scripts/dag1/task1.py',
    dag=dag
)

man_is_utro = BashOperator(
    task_id = 'man_is_utro',
    bash_command='python3 /airflow/scripts/dag1/task2.py',
    dag=dag
)