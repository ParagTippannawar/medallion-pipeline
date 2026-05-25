from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'medallion',
    'retries': 1,
    'retry_delay': timedelta(minutes = 5),
}

with DAG(
    dag_id = 'medallion_dbt_pipeline',
    default_args=default_args,
    description='Runs dbt Bronze → Silver → Gold transformation pipeline',
    schedule_interval='@daily',
    start_date = datetime(2024, 1, 1),
    catchup=False,
    tags = ['medallion', 'dbt'],
) as dag:
    
    dbt_run = BashOperator(
        task_id= 'dbt_run',
        bash_command= 'cd /opt/airflow/medallion_dbt && dbt run --profiles-dir /opt/airflow/medallion_dbt',
    )

    dbt_test = BashOperator(
        task_id= 'dbt_test',
        bash_command='cd /opt/airflow/medallion_dbt && dbt test --profiles-dir /opt/airflow/medallion_dbt',
    )
    
    dbt_run >> dbt_test