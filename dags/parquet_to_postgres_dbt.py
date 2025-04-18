from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator 
from datetime import datetime, timedelta
import pandas as pd
import os
import psycopg2


default_args = {
    'start_date': datetime(2025, 4, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_pg_conn(): # new conection with every run
    return psycopg2.connect(
        host="postgres_container",
        dbname="airflow_db",
        user="airflow_user",
        password="airflow_password",
        port=5432
    )
def convert_parquet_to_csv():
    parquet_dir = '/opt/airflow/data/parquet_files'
    csv_dir = '/opt/airflow/data/csv_files'
    
    os.makedirs(csv_dir, exist_ok=True)
    
    for file in os.listdir(parquet_dir):
        if file.endswith('.parquet'):
            try:
                df = pd.read_parquet(os.path.join(parquet_dir, file))
                csv_file = os.path.join(csv_dir, file.replace('.parquet', '.csv'))
                df.to_csv(csv_file, index=False)
                print(f"Successfully converted {file} to CSV")
            except Exception as e:
                print(f"Error converting {file}: {str(e)}")
                raise

def load_csv_to_postgres():
    csv_dir = '/opt/airflow/data/csv_files'
    
    with get_pg_conn() as conn:
        for file in os.listdir(csv_dir):
            if file.endswith('.csv'):
                file_path = os.path.join(csv_dir, file)
                
                try:
                    df = pd.read_csv(file_path)
                    
                    if 'id' not in df.columns:
                        print(f"File {file} doesn't have ID column")
                        continue
                        
                    df['csv_data'] = df.apply(lambda row: row.to_json(), axis=1) # to json
                    
                    with conn.cursor() as cur: # load to postgres
                        for _, row in df.iterrows():
                            try:
                                cur.execute("""
                                    INSERT INTO main_table (id, csv_data)
                                    VALUES (%s, %s)
                                    ON CONFLICT (id) DO UPDATE SET
                                        csv_data = EXCLUDED.csv_data;
                                """, (row['id'], row['csv_data']))
                            except KeyError as e:
                                print(f"No key {e} in a row")
                                continue
                        conn.commit()
                    print(f"Loaded {file}")
                    
                except Exception as e:
                    print(f"Error loading {file}: {str(e)}")
                    conn.rollback()
                    raise



with DAG(
    'parquet_to_csv_to_postgres_loader',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    convert_task = PythonOperator(
        task_id='convert_parquet_to_csv',
        python_callable=convert_parquet_to_csv
    )

    create_staging = SQLExecuteQueryOperator(
        task_id='create_staging_table',
        conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS staging_table (
            id INT,
            name TEXT,
            age INT
        );
        """,
        autocommit=True
    )
    
    create_main = SQLExecuteQueryOperator(
        task_id='create_main_table',
        conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS main_table (
            id INT PRIMARY KEY,
            name TEXT,
            age INT,
            csv_data JSONB
        );
        """,
        autocommit=True
    )
    
    load_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres
    )

create_staging >> create_main >> convert_task >> load_task