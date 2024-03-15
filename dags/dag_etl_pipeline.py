from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import json
import psycopg2
import mysql.connector
import pandas as pd
from sqlalchemy import create_engine
import pymysql

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
    # Connect to MySQL database
    connection = mysql.connector.connect(
        host='final_project_de-mysql-1',
        user='airflow',
        password='airflow',
        database='staging_area'
    )

    engine = create_engine('mysql+pymysql://airflow:airflow@final_project_de-mysql-1/staging_area')

    # Define your SQL query
    sql_query = "SELECT * FROM staging_table;"

    # Use pandas.read_sql() to execute the query and load data into a DataFrame
    df_staging = pd.read_sql(sql_query, connection)

    # Create Dimension Table
    # Create province table
    province_table = df_staging[['kode_prov', 'nama_prov']]
    province_table = province_table.drop_duplicates()
    province_table.rename(columns={'kode_prov':'province_id', 'nama_prov':'province_name'}, inplace=True)
    
    try:
        province_table.to_sql(name='Province', con=engine, if_exists='append', index=False)
    except:
        pass

    # # Create district table
    district_table = df_staging[['kode_kab', 'kode_prov', 'nama_kab']]
    district_table = district_table.drop_duplicates()
    district_table.rename(columns={'kode_kab':'district_id', 'kode_prov':'province_id', 'nama_kab':'district_name'}, inplace=True)
    
    try:
        district_table.to_sql(name='District', con=engine, if_exists='append', index=False)
    except:
        pass

    # Create case table
    case_table_dict = { 'case_id' : [1,2,3,4,5,6,7,8,9,10,11],
                        'status_name': ['CLOSECONTACT','CLOSECONTACT','CLOSECONTACT',
                              'CONFIRMATION','CONFIRMATION',
                              'PROBABLE','PROBABLE','PROBABLE',
                              'SUSPECT', 'SUSPECT', 'SUSPECT'],
                        'status_detail': ['closecontact_dikarantina', 'closecontact_discarded', 'closecontact_meninggal', 
                                'confirmation_meninggal', 'confirmation_sembuh', 
                                'probable_diisolasi', 'probable_discarded', 'probable_meninggal', 
                                'suspect_diisolasi', 'suspect_discarded', 'suspect_meninggal']
                     }
    
    case_table = pd.DataFrame(case_table_dict)

    try:
        case_table.to_sql(name='Cases', con=engine, if_exists='replace', index=False)
    except:
        pass

    # Create Fact Table
    # Aggregate for Province_Daily
    # Group by province and calculate the total cases for each status
    sql_query_province_daily = '''
    SELECT 
        CASE 
            WHEN t.nama_prov IS NOT NULL THEN p.province_id
            ELSE NULL
        END AS province_id,
        t.tanggal AS date,
        t.CLOSECONTACT + t.CONFIRMATION + t.PROBABLE + t.SUSPECT AS total
    FROM staging_table t
    LEFT JOIN Province p ON t.nama_prov = p.province_name;
    '''
    #c.case_id AS case_id,
    # LEFT JOIN Cases c ON t.tanggal = c.tanggal;
    df_staging_province_daily = pd.read_sql(sql_query_province_daily, connection)

    # Aggregate for Province_Daily
    # Group by province and calculate the total cases for each status
    sql_query_province_monthly = '''
    SELECT 
        CASE 
            WHEN t.nama_prov IS NOT NULL THEN p.province_id
            ELSE NULL
        END AS province_id,
        MONTH(t.tanggal) AS month,
        SUM(t.CLOSECONTACT + t.CONFIRMATION + t.PROBABLE + t.SUSPECT) AS total
    FROM staging_table t
    LEFT JOIN Province p ON t.nama_prov = p.province_name
    GROUP BY 
        CASE 
            WHEN t.nama_prov IS NOT NULL THEN p.province_id
            ELSE NULL
        END,
        MONTH(t.tanggal);
    '''
    df_staging_province_monthly = pd.read_sql(sql_query_province_monthly, connection)

    # Aggregate for Province_Yearly
    # Group by province and calculate the total cases for each status
    sql_query_province_yearly = '''
    SELECT 
        CASE 
            WHEN t.nama_prov IS NOT NULL THEN p.province_id
            ELSE NULL
        END AS province_id,
        YEAR(t.tanggal) AS year,
        SUM(t.CLOSECONTACT + t.CONFIRMATION + t.PROBABLE + t.SUSPECT) AS total
    FROM staging_table t
    LEFT JOIN Province p ON t.nama_prov = p.province_name
    GROUP BY 
        CASE 
            WHEN t.nama_prov IS NOT NULL THEN p.province_id
            ELSE NULL
        END,
        YEAR(t.tanggal);
    '''
    df_staging_province_yearly = pd.read_sql(sql_query_province_yearly, connection)
    
    # Aggregate for District_Monthly
    # Group by province and calculate the total cases for each status
    sql_query_district_monthly = '''
    SELECT 
        CASE 
            WHEN t.nama_kab IS NOT NULL THEN d.district_id
            ELSE NULL
        END AS district_id,
        MONTH(t.tanggal) AS month,
        SUM(t.CLOSECONTACT + t.CONFIRMATION + t.PROBABLE + t.SUSPECT) AS total
    FROM staging_table t
    LEFT JOIN District d ON t.nama_kab = d.district_name
    GROUP BY 
    CASE 
        WHEN t.nama_kab IS NOT NULL THEN d.district_id
        ELSE NULL
    END,
    MONTH(t.tanggal);
    '''
    df_staging_district_monthly = pd.read_sql(sql_query_district_monthly, connection)

    # Aggregate for District_yearly
    # Group by province and calculate the total cases for each status
    sql_query_district_yearly = '''
    SELECT 
        CASE 
            WHEN t.nama_kab IS NOT NULL THEN d.district_id
            ELSE NULL
        END AS district_id,
        YEAR(t.tanggal) AS year,
        SUM(t.CLOSECONTACT + t.CONFIRMATION + t.PROBABLE + t.SUSPECT) AS total
    FROM staging_table t
    LEFT JOIN District d ON t.nama_kab = d.district_name
    GROUP BY 
    CASE 
        WHEN t.nama_kab IS NOT NULL THEN d.district_id
        ELSE NULL
    END,
    YEAR(t.tanggal);
    '''
    df_staging_district_yearly = pd.read_sql(sql_query_district_yearly, connection)

    return df_staging_province_daily, df_staging_province_monthly, df_staging_province_yearly, df_staging_district_monthly, df_staging_district_yearly

# Function to load aggregate data to PostgreSQL
def load_aggregate_data_to_postgres(**kwargs):
    ti = kwargs['ti']
    # Retrieve DataFrames from XCom
    df_staging_province_daily = ti.xcom_pull(task_ids='aggregate_district_data')[0]
    df_staging_province_monthly = ti.xcom_pull(task_ids='aggregate_district_data')[1]
    df_staging_province_yearly = ti.xcom_pull(task_ids='aggregate_district_data')[2]
    df_staging_district_monthly = ti.xcom_pull(task_ids='aggregate_district_data')[3]
    df_staging_district_yearly = ti.xcom_pull(task_ids='aggregate_district_data')[4]

    # Connect to PostgreSQL database
    connection = psycopg2.connect(
        host='final_project_de-postgres-1',
        user='postgres',
        password='postgres',
        database='data_mart'
    )
    # Create a connection string
    connection_string = f'postgresql://postgres:postgres@final_project_de-postgres-1/data_mart'

    # Create SQLAlchemy engine
    engine = create_engine(connection_string)
    # Load aggregate data to PostgreSQL
    cursor = connection.cursor()

    # Write DataFrame to PostgreSQL database
    df_staging_province_daily.to_sql(name='Province_Daily', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
    df_staging_province_monthly.to_sql(name='Province_Monthly', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
    df_staging_province_yearly.to_sql(name='Province_Yearly', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
    df_staging_district_monthly.to_sql(name='District_Monthly', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)
    df_staging_district_yearly.to_sql(name='District_Yearly', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)

    df_staging_province_daily.to_csv('df_staging_province_daily.csv', index=False)
    df_staging_province_monthly.to_csv('df_staging_province_monthly.csv', index=False)
    df_staging_province_yearly.to_csv('df_staging_province_yearly.csv', index=False)
    df_staging_district_monthly.to_csv('df_staging_district_monthly.csv', index=False)
    df_staging_district_yearly.to_csv('df_staging_district_yearly.csv', index=False)
    
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
