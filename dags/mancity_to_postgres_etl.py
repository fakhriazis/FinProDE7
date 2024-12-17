from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
from sqlalchemy import create_engine
import os

# Konfigurasi MinIO dan PostgreSQL
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'admin'
MINIO_SECRET_KEY = 'password'
MINIO_BUCKET = 'mancity-data'
POSTGRES_CONN_STR = 'postgresql://airflow:airflow@airflow-db:5432/premierleague'

# File di MinIO dan tabel PostgreSQL
FILES_AND_TABLES = {
    "erling_haaland_match_logs_2023_2024.json": "erling_halaand_logs",
    "jack_grealish_match_logs_2023_2024.json": "jack_grealish_logs",
    "josko_gvardiol_match_logs_2023_2024.json": "josko_gvardiol_logs",
    "kyle_walker_match_logs_2023_2024.json": "kyle_walker_logs",
    "manuel_akanji_match_logs_2023_2024.json": "manuel_akanji_logs",
    "mateo_kovacic_match_logs_2023_2024.json": "mateo_kovacic_logs",
    "nathan_ake_match_logs_2023_2024.json": "nathan_ake_logs",
    "phil_foden_match_logs_2023_2024.json": "phil_foden_logs",
    "kevin_de_bruyne_match_logs_2023_2024.json": "kevin_de_bruyne_logs",
    "rodri_match_logs_2023_2024.json": "rodri_logs"
}

# Fungsi ETL untuk satu file
def etl_file_to_postgres(file_name, table_name):
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
    local_file = f"/tmp/{file_name}"
    s3_client.download_file(MINIO_BUCKET, file_name, local_file)

    # Load file JSON
    df = pd.read_json(local_file)

    # Log nama kolom untuk debugging
    print("Columns before cleaning:", df.columns.tolist())

    # Bersihkan nama kolom kosong atau tidak valid
    df.columns = [col if col.strip() else f"unnamed_{i}" for i, col in enumerate(df.columns)]

    # Log nama kolom setelah pembersihan
    print("Columns after cleaning:", df.columns.tolist())

    # Transformasi data
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')

    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].fillna('').str.strip()

    for col in df.select_dtypes(include=['number']).columns:
        df[col] = df[col].fillna(0)

    # Muat data ke PostgreSQL
    df.to_sql(table_name, engine, if_exists='replace', index=False)

    # Hapus file lokal
    os.remove(local_file)


# Definisi DAG Airflow
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1),
}

dag = DAG(
    dag_id='mancity_to_postgres_etl',
    default_args=default_args,
    description='ETL dari MinIO ke PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Membuat tugas untuk setiap file
tasks = []
for file_name, table_name in FILES_AND_TABLES.items():
    task = PythonOperator(
        task_id=f'etl_{table_name}',
        python_callable=etl_file_to_postgres,
        op_kwargs={'file_name': file_name, 'table_name': table_name},
        dag=dag
    )
    tasks.append(task)

# Menentukan urutan tugas (jika diperlukan, saat ini paralel)
for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]
