import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=5),
}

spark_dag = DAG(
        dag_id = "spark_airflow_dag",
        default_args=default_args,
        schedule_interval="0 0 * * *",
        dagrun_timeout=timedelta(minutes=60),
        description='use case of sparkoperator in airflow',
        start_date = airflow.utils.dates.days_ago(1)
)

start = DummyOperator(
    task_id = "start",
    dag = spark_dag
)

process = SparkSubmitOperator(
    application = "/opt/airflow/dags/spark_etl.py",
    conn_id= 'spark_default',
    task_id='spark_submit_task',
    conf={"spark.master": "spark://spark-master:7077"},
    dag=spark_dag
)

end = DummyOperator(
    task_id = "end",
    dag = spark_dag
)

start >> process >> end
