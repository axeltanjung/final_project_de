from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 27),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define DAG
dag = DAG('spark_etl_task_dag', default_args=default_args, schedule_interval=None)

# Define tasks
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)
process = SparkSubmitOperator(
    task_id='process',
    conn_id='spark_default',
    application='path/to/spark_etl_task.py',
    dag=dag
)

# Define task dependencies
start >> process >> end
