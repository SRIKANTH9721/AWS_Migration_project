from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='trigger_local_glue_container_job',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='*/5 * * * *',  # every 5 minutes
    catchup=False,
    tags=['glue', 'local'],
) as dag:

    trigger_glue = BashOperator(
        task_id='run_glue_local_script',
        bash_command='docker exec glue spark-submit /home/hadoop/workspace/sri_glue_script.py'
    )
