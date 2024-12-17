from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import boto3
from sqlalchemy import create_engine
import os
import uuid  # Import UUID untuk player_id

# Konfigurasi koneksi
MINIO_ENDPOINT = "http://minio:9000"  # Ganti dengan endpoint MinIO 
MINIO_ACCESS_KEY = "admin" 
MINIO_SECRET_KEY = "password" 
BUCKET_NAME = "arsenal-data"  
MINIO_FILE_KEY = 'david_raya_match_logs_2023_2024.json'
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
    # Ambil nama file dari path dan proses dua kata pertama
    filename = os.path.basename(MINIO_FILE_KEY)  # Ambil nama file
    name_parts = filename.split('_')
    player_name = ' '.join(name_parts[:2])  # Dua kata pertama sebagai nama pemain

    print(f"Player name extracted from file: {player_name}")

    # Load data
    df = pd.read_json(LOCAL_FILE)

    # Transformasi data sesuai spesifikasi tabel 'players'
    transformed_df = pd.DataFrame()

    # Tambahkan kolom 'player_id' dengan UUID unik
    transformed_df['player_id'] = [str(uuid.uuid4())]  # UUID untuk player_id

    # Kolom 'nama' dari dua kata pertama nama file
    transformed_df['nama'] = player_name
    transformed_df['position'] = df.get('position', 'Unknown')  # Default 'Unknown' jika kolom tidak ada
    transformed_df['nationality'] = df.get('nationality', 'Unknown')  # Default 'Unknown' jika kolom tidak ada

    # Simpan hasil transformasi
    transformed_df.to_csv('/tmp/players_data.csv', index=False)
    print("Transformed data saved to /tmp/players_data.csv")

# Fungsi Load
def load_data_to_postgres():
    engine = create_engine(POSTGRES_CONN_STR)

    # Load transformed data
    df = pd.read_csv('/tmp/players_data.csv')

    # Muat data ke PostgreSQL ke tabel 'players'
    df.to_sql('players', engine, if_exists='replace', index=False)
    print("Data loaded to PostgreSQL in table 'players'")

# Definisi DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=1)
}

dag = DAG(
    dag_id='players_data_to_postgres_etl',
    default_args=default_args,
    description='ETL DAG for MinIO to PostgreSQL with Players Table',
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
