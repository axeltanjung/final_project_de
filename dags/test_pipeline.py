import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd


def read_file():
    data = pd.read_csv("/opt/airflow/dags/data_source/North America Restaurants.csv")
    return data.to_dict(orient="record")

def transform_data(ti):
    data = pd.DataFrame(ti.xcom_pull(task_ids="extract"))
    data_filtered = data[data["state"] == "AB"]

    return data_filtered.to_dict(orient="record")

def save_file(ti):
    data = pd.DataFrame(ti.xcom_pull(task_ids="transform"))
    data.to_csv("/opt/airflow/dags/output/transformed_data.csv", index=False)

with DAG(
    dag_id="test_pipeline",
    start_date=datetime.datetime(2024, 2, 14),
    schedule_interval="5 7 * * *",
    catchup=False
) as dag:
    start = DummyOperator(
        task_id = "start",
        dag = dag
    )

    extract = PythonOperator(
        task_id = "extract",
        python_callable=read_file,
        dag = dag
    )

    load = PythonOperator(
        task_id = "transform",
        python_callable=transform_data,
        dag = dag
    )

    transform = PythonOperator(
        task_id = "load",
        python_callable=save_file,
        dag = dag
    )

    end = DummyOperator(
        task_id = "end",
        dag = dag
    )

    start >> extract >> load >> transform >> end
