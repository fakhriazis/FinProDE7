from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
from sqlalchemy import create_engine
import os

# Konfigurasi MinIO dan PostgreSQL
MINIO_ENDPOINT = "http://minio:9000"  # Ganti dengan endpoint MinIO 
MINIO_ACCESS_KEY = "admin" 
MINIO_SECRET_KEY = "password" 
BUCKET_NAME = "arsenal-data"  
POSTGRES_CONN_STR = 'postgresql://airflow:airflow@airflow-db:5432/premierleague'

# File dan tabel target
CSV_FILE = "arsenalfc_premierleague_2023-2024.csv"
POSTGRES_TABLE = "arsenalfc_premierleague_match"

# Fungsi ETL untuk file CSV
def etl_csv_to_postgres():
    # Koneksi MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )

    # Koneksi PostgreSQL
    engine = create_engine(POSTGRES_CONN_STR)

    # Unduh file dari MinIO
    local_file = f"/tmp/{CSV_FILE}"
    s3_client.download_file(BUCKET_NAME, CSV_FILE, local_file)

    # Load file CSV
    df = pd.read_csv(local_file)

    # Transformasi data (contoh pembersihan)
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]  # Normalisasi nama kolom

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('').str.strip()

    for col in df.select_dtypes(include=['number']).columns:
        df[col] = df[col].fillna(0)

    # Muat data ke PostgreSQL
    df.to_sql(POSTGRES_TABLE, engine, if_exists='replace', index=False)

    # Hapus file lokal
    os.remove(local_file)

# Definisi DAG Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='arsenalfc_to_postgres_etl',
    default_args=default_args,
    description='ETL CSV Arsenal FC ke PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

etl_task = PythonOperator(
    task_id='etl_csv_to_postgres',
    python_callable=etl_csv_to_postgres,
    dag=dag
)
