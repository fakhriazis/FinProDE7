from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd
import boto3
from sqlalchemy import create_engine
import os

# Konfigurasi koneksi
# MINIO_ENDPOINT = "http://localhost:9000"  # Ganti dengan endpoint MinIO 
MINIO_ENDPOINT = "http://minio:9000"  # Ganti dengan endpoint MinIO 
MINIO_ACCESS_KEY = "admin" 
MINIO_SECRET_KEY = "password" 
BUCKET_NAME = "arsenal-data"  
MINIO_FILE_KEY = 'david_raya_match_logs_2023_2024.json'
# POSTGRES_CONN_STR = 'postgresql://airflow:airflow-db@localhost:5434/postgres'
POSTGRES_CONN_STR = 'postgresql://airflow:airflow@airflow-db:5432/premierleague'

LOCAL_FILE = '/tmp/data.json'

# Fungsi Extract
def extract_data_from_minio():
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    s3.download_file(BUCKET_NAME, MINIO_FILE_KEY, LOCAL_FILE)
    print(f"File {MINIO_FILE_KEY} downloaded to {LOCAL_FILE}")

# Fungsi Transform
def transform_data():
    # Load data
    df = pd.read_json(LOCAL_FILE)

    # Bersihkan nilai kosong pada kolom tertentu (tanpa menghapus baris atau kolom)
    if 'gk_save_pct' in df.columns:
        df['gk_save_pct'] = df['gk_save_pct'].fillna(0)  # Isi nilai kosong dengan 0 untuk kolom ini

    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')  # Ubah format tanggal (jika valid)

    # Normalisasi teks (jika diperlukan)
    if 'team' in df.columns:
        df['team'] = df['team'].str.replace('eng ', '').str.strip()  # Hilangkan awalan 'eng '

    # Validasi data numerik (tanpa menghapus baris)
    for col in ['gk_save_pct', 'gk_clean_sheets']:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # Simpan data yang sudah dibersihkan ke file sementara (tanpa memodifikasi sumber)
    df.to_csv('/tmp/cleaned_data.csv', index=False)
    print("Data cleaned and saved to /tmp/cleaned_data.csv")


# Fungsi Load
def load_data_to_postgres():
    engine = create_engine(POSTGRES_CONN_STR)

    # Load cleaned data
    df = pd.read_csv('/tmp/cleaned_data.csv')

    # Muat data ke PostgreSQL
    df.to_sql('david_raya_logs', engine, if_exists='replace', index=False)
    print("Data loaded to PostgreSQL")

# Definisi DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1)
}

dag = DAG(
    dag_id= 'david_raya_to_postgres_etl',
    default_args=default_args,
    description='ETL DAG for MinIO to PostgreSQL',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
)

# Tugas DAG
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_from_minio,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_postgres,
    dag=dag
)

# Dependency
extract_task >> transform_task >> load_task
