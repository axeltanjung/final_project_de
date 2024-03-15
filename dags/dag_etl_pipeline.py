from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import json
import psycopg2
import mysql.connector
import pandas as pd

# Function to fetch data from API
def fetch_data_from_api():
    url = 'http://103.150.197.96:5005/api/v1/rekapitulasi_v2/jabar/harian?level=kab'
    response = requests.get(url)
    data = response.json()

    return data
    # Process data and return

# Function to load data to MySQL
def load_data_to_mysql(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data_from_api')
    records = data['data']['content']

    # Connect to MySQL database
    connection = mysql.connector.connect(
        host='final_project_de-mysql-1',
        user='airflow',
        password='airflow',
        database='staging_area'
    )

    cursor = connection.cursor()
    # Load data to MySQL
    # Example:
    # cursor.execute("INSERT INTO table_name (column1, column2) VALUES (%s, %s)", (value1, value2))
    # Insert data into MySQL table
    
    for record in records:
        insert_query = """
        INSERT INTO staging_table (CLOSECONTACT, CONFIRMATION, PROBABLE, SUSPECT, closecontact_dikarantina, closecontact_discarded, closecontact_meninggal, 
                                confirmation_meninggal, confirmation_sembuh, kode_kab, kode_prov, nama_kab, nama_prov, probable_diisolasi, probable_discarded, 
                                probable_meninggal, suspect_diisolasi, suspect_discarded, suspect_meninggal,tanggal)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (record['CLOSECONTACT'], record['CONFIRMATION'], record['PROBABLE'], record['SUSPECT'], record['closecontact_dikarantina'],
                                      record['closecontact_discarded'], record['closecontact_meninggal'], record['confirmation_meninggal'], record['confirmation_sembuh'], record['kode_kab'],
                                      record['kode_prov'], record['nama_kab'], record['nama_prov'], record['probable_diisolasi'], record['probable_discarded'],
                                      record['probable_meninggal'], record['suspect_diisolasi'], record['suspect_discarded'], record['suspect_meninggal'], record['tanggal']))
    
    connection.commit()
    connection.close()

# Function to aggregate district data
def aggregate_district_data():
    # Fetch data from MySQL (Staging Area)
    # Aggregate district data
    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        host='final_project_de-postgres-1',
        user='postgres',
        password='postgres',
        database='staging_area'
    )
    cur = conn.cursor()

    # Sample data
    data = [
        {
            "CLOSECONTACT": 274,
            "CONFIRMATION": 0,
            "PROBABLE": 26,
            "SUSPECT": 2210,
            "closecontact_dikarantina": 0,
            "closecontact_discarded": 274,
            "closecontact_meninggal": 0,
            "confirmation_meninggal": 0,
            "confirmation_sembuh": 0,
            "kode_kab": "3204",
            "kode_prov": "32",
            "nama_kab": "Kabupaten Bandung",
            "nama_prov": "Jawa Barat",
            "probable_diisolasi": 0,
            "probable_discarded": 0,
            "probable_meninggal": 26,
            "suspect_diisolasi": 31,
            "suspect_discarded": 2179,
            "suspect_meninggal": 0,
            "tanggal": "2020-08-05"
        }
    ]

    # Iterate over the data and insert into the tables
    for record in data:
        # Insert into Province table
        cur.execute("INSERT INTO Province (province_name) VALUES (%s) ON CONFLICT DO NOTHING", (record["nama_prov"],))
        conn.commit()

        # Get the province_id for the inserted province
        cur.execute("SELECT province_id FROM Province WHERE province_name = %s", (record["nama_prov"],))
        province_id = cur.fetchone()[0]

        # Insert into District table
        cur.execute("INSERT INTO District (province_id, district_name) VALUES (%s, %s) ON CONFLICT DO NOTHING", (province_id, record["nama_kab"]))
        conn.commit()

        # Insert into Case table
        for status in ['CLOSECONTACT', 'CONFIRMATION', 'PROBABLE', 'SUSPECT']:
            cur.execute("INSERT INTO Case (status_name, status_detail) VALUES (%s, %s) ON CONFLICT DO NOTHING", (status, None))
            conn.commit()

        # Insert into Province_Daily table
        cur.execute("""
            INSERT INTO Province_Daily (province_id, case_id, date, total)
            VALUES (%s, (SELECT Id FROM Case WHERE status_name = 'CLOSECONTACT'), %s, %s)
        """, (province_id, datetime.strptime(record["tanggal"], "%Y-%m-%d"), record["CLOSECONTACT"]))
        conn.commit()

        # Insert into Province_Monthly table
        month = datetime.strptime(record["tanggal"], "%Y-%m-%d").month
        cur.execute("""
            INSERT INTO Province_Monthly (province_id, case_id, month, total)
            VALUES (%s, (SELECT Id FROM Case WHERE status_name = 'CLOSECONTACT'), %s, %s)
        """, (province_id, month, record["CLOSECONTACT"]))
        conn.commit()

        # Insert into Province_Yearly table
        year = datetime.strptime(record["tanggal"], "%Y-%m-%d").year
        cur.execute("""
            INSERT INTO Province_Yearly (province_id, case_id, year, total)
            VALUES (%s, (SELECT Id FROM Case WHERE status_name = 'CLOSECONTACT'), %s, %s)
        """, (province_id, year, record["CLOSECONTACT"]))
        conn.commit()

        # Insert into District_Monthly table
        cur.execute("""
            INSERT INTO District_Monthly (district_id, case_id, month, total)
            VALUES ((SELECT district_id FROM District WHERE district_name = %s), 
                    (SELECT Id FROM Case WHERE status_name = 'CLOSECONTACT'), 
                    %s, %s)
        """, (record["nama_kab"], month, record["CLOSECONTACT"]))
        conn.commit()

        # Insert into District_Yearly table
        cur.execute("""
            INSERT INTO District_Yearly (district_id, case_id, year, total)
            VALUES ((SELECT district_id FROM District WHERE district_name = %s), 
                    (SELECT Id FROM Case WHERE status_name = 'CLOSECONTACT'), 
                    %s, %s)
        """, (record["nama_kab"], year, record["CLOSECONTACT"]))
        conn.commit()

    # Close the cursor and connection
    cur.close()
    conn.close()

    pass

# Function to load aggregate data to PostgreSQL
def load_aggregate_data_to_postgres():
    # Connect to PostgreSQL database
    connection = psycopg2.connect(
        host='final_project_de-postgres-1',
        user='postgres',
        password='postgres',
        database='data_mart'
    )
    cursor = connection.cursor()
    # Load aggregate data to PostgreSQL
    # Example:
    # cursor.execute("INSERT INTO table_name (column1, column2) VALUES (%s, %s)", (value1, value2))
    connection.commit()
    connection.close()

# Define DAG
with DAG(
    dag_id='covid19_data_pipeline',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2024, 3, 11),
        'retries': 1
    },
    schedule_interval='@daily',
    catchup=True
) as dag:

    # Define tasks
    fetch_data_task = PythonOperator(
        task_id='fetch_data_from_api',
        python_callable=fetch_data_from_api,
        dag=dag
    )

    load_data_to_mysql_task = PythonOperator(
        task_id='load_data_to_mysql',
        python_callable=load_data_to_mysql,
        dag=dag
    )

    aggregate_district_data_task = PythonOperator(
        task_id='aggregate_district_data',
        python_callable=aggregate_district_data,
        dag=dag
    )

    load_aggregate_data_to_postgres_task = PythonOperator(
        task_id='load_aggregate_data_to_postgres',
        python_callable=load_aggregate_data_to_postgres,
        dag=dag
    )

    # Define task dependencies
    fetch_data_task >> load_data_to_mysql_task >> aggregate_district_data_task >> load_aggregate_data_to_postgres_task
