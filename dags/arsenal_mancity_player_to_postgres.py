from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import json
import boto3
import psycopg2
from psycopg2.extras import execute_values
import os

# Konfigurasi MinIO
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME_ARSENAL = "arsenal-data"
BUCKET_NAME_MANCITY = "mancity-data"

# Koneksi PostgreSQL menggunakan Connection String
POSTGRES_CONN_STR = 'postgresql://airflow:airflow@airflow-db:5432/pl_analysis'

# File dan Direktori Lokal
LOCAL_DIR = "/tmp/"
ARSENAL_FILE = 'arsenal_player.json'
MANCITY_FILE = 'mancity_player.json'

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
    'start_date': datetime(2024, 6, 8),
}

# Fungsi untuk mendownload file JSON dari MinIO
def download_file_from_minio(bucket_name, file_name):
    local_file_path = os.path.join(LOCAL_DIR, file_name)
    print(f"Downloading '{file_name}' from bucket '{bucket_name}'...")
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY
    )
    s3.download_file(bucket_name, file_name, local_file_path)
    print(f"File '{file_name}' successfully downloaded to '{local_file_path}'.")
    return local_file_path

# Fungsi untuk memuat data ke PostgreSQL
def load_json_to_postgres(file_path):
    print(f"Loading data from '{file_path}' into PostgreSQL...")

    # Membuka file JSON
    with open(file_path, "r") as f:
        data = json.load(f)

    # Koneksi PostgreSQL
    conn = psycopg2.connect(POSTGRES_CONN_STR)
    cursor = conn.cursor()

    # Query INSERT
    insert_query = """
        INSERT INTO players (player_id, player_name, position, footed, club, nationality)
        VALUES %s
        ON CONFLICT (player_id) DO UPDATE SET
        player_name = EXCLUDED.player_name,
        position = EXCLUDED.position,
        footed = EXCLUDED.footed,
        club = EXCLUDED.club,
        nationality = EXCLUDED.nationality
    """

    # Mengumpulkan data
    records = [
        (
            player.get("player_id"),
            player.get("player_name"),
            player.get("position"),
            player.get("footed"),
            player.get("club"),
            player.get("nationality"),
        )
        for player in data
    ]

    # Eksekusi query
    execute_values(cursor, insert_query, records)
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data from '{file_path}' successfully loaded into PostgreSQL.")

# Fungsi gabungan untuk proses download dan load
def download_and_load(bucket_name, file_name):
    file_path = download_file_from_minio(bucket_name, file_name)
    load_json_to_postgres(file_path)

# Definisi DAG
with DAG(
    dag_id='arsenal_mancity_player_to_postgres',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    start_task = EmptyOperator(task_id='start')

    process_arsenal_player = PythonOperator(
        task_id='process_arsenal_player',
        python_callable=download_and_load,
        op_args=[BUCKET_NAME_ARSENAL, ARSENAL_FILE],
    )

    process_mancity_player = PythonOperator(
        task_id='process_mancity_player',
        python_callable=download_and_load,
        op_args=[BUCKET_NAME_MANCITY, MANCITY_FILE],
    )

    end_task = EmptyOperator(task_id='end')

    # Urutan DAG
    start_task >> [process_arsenal_player, process_mancity_player] >> end_task
