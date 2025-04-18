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

def get_pg_conn():
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
                print(f"Сonverted {file} to CSV")
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
                    
                    with conn.cursor() as cur:
                        for _, row in df.iterrows():
                            cur.execute("""
                            INSERT INTO houses (
                                price, area, bedrooms, bathrooms, stories,
                                mainroad, guestroom, basement, hotwaterheating,
                                airconditioning, parking, prefarea, furnishingstatus
                            ) VALUES (
                                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                            )
                            ON CONFLICT (price, area, bedrooms, bathrooms)
                            DO UPDATE SET
                                stories = EXCLUDED.stories,
                                mainroad = EXCLUDED.mainroad,
                                guestroom = EXCLUDED.guestroom,
                                basement = EXCLUDED.basement,
                                hotwaterheating = EXCLUDED.hotwaterheating,
                                airconditioning = EXCLUDED.airconditioning,
                                parking = EXCLUDED.parking,
                                prefarea = EXCLUDED.prefarea,
                                furnishingstatus = EXCLUDED.furnishingstatus;
                            """, (
                                row['price'], row['area'], row['bedrooms'],
                                row['bathrooms'], row['stories'], row['mainroad'],
                                row['guestroom'], row['basement'], row['hotwaterheating'],
                                row['airconditioning'], row['parking'], row['prefarea'],
                                row['furnishingstatus']
                            ))
                        conn.commit()
                    print(f"Loaded {file}")
                    
                except Exception as e:
                    print(f"Error loading {file}: {str(e)}")
                    conn.rollback()
                    raise

with DAG(
    'house_price_loader',
    default_args=default_args,
    schedule_interval='*/5 * * * *',
    catchup=False,
    max_active_runs=1
) as dag:

    convert_task = PythonOperator(
        task_id='convert_parquet_to_csv',
        python_callable=convert_parquet_to_csv
    )

    create_table = SQLExecuteQueryOperator(
        task_id='create_houses_table',
        conn_id='postgres_default',
        sql="""
        CREATE TABLE IF NOT EXISTS houses (
    price INTEGER,
    area INTEGER,
    bedrooms INTEGER,
    bathrooms INTEGER,
    stories INTEGER,
    mainroad VARCHAR(3),
    guestroom VARCHAR(3),
    basement VARCHAR(3),
    hotwaterheating VARCHAR(3),
    airconditioning VARCHAR(3),
    parking INTEGER,
    prefarea VARCHAR(3),
    furnishingstatus VARCHAR(20),
    
    CONSTRAINT houses_unique_key UNIQUE (price, area, bedrooms, bathrooms)
        );
        """,
        autocommit=True
    )
    
    load_task = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=load_csv_to_postgres
    )

    create_table >> convert_task >> load_task
