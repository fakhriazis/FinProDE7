import json
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import io

# Default Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=3),
}

# MinIO Configuration
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "arsenal-data"
MINIO_FILE_NAME = 'ben_white_match_logs_2023_2024.json'

# ETL Function: Extract, Transform, Load
def etl_minio_to_postgres():
    """Extract data from MinIO and load directly into PostgreSQL."""
    try:
        # Step 1: Connect to MinIO and fetch the file
        s3_client = boto3.client(
            's3',
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        # Download JSON file from MinIO
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=MINIO_FILE_NAME)
        data = response['Body'].read().decode('utf-8')
        records = json.loads(data)

        # Step 2: Transform data
        # Add player_id for Ben White (assume player_id = 1)
        transformed_data = []
        for record in records:
            transformed_data.append({
                'player_id': 1,
                'match_id': record['match_id'],
                'minutes_played': record['minutes_played'],
                'goals': record['goals'],
                'assists': record['assists'],
                'shots': record['shots'],
                'shots_on_target': record['shots_on_target'],
                'tackles': record['tackles'],
                'interceptions': record['interceptions'],
                'passes_completed': record['passes_completed'],
                'xg': record['xg'],
                'xa': record['xa'],
            })

        # Step 3: Load data into PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='shared_postgres')

        # Using `copy_expert` for efficient bulk insert
        columns = [
            'player_id', 'match_id', 'minutes_played', 'goals', 'assists',
            'shots', 'shots_on_target', 'tackles', 'interceptions',
            'passes_completed', 'xg', 'xa'
        ]
        copy_sql = f"""
        COPY playerstats ({', '.join(columns)})
        FROM STDIN WITH CSV HEADER
        """

        # Convert transformed data to CSV format
        csv_buffer = io.StringIO()
        csv_buffer.write(','.join(columns) + '\n')  # Add header row
        for row in transformed_data:
            csv_buffer.write(','.join(map(str, row.values())) + '\n')
        csv_buffer.seek(0)

        # Execute COPY command
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        cursor.copy_expert(copy_sql, csv_buffer)
        connection.commit()
        cursor.close()
        print("Data successfully loaded into PlayerStats")
    except Exception as e:
        print(f"Error during ETL process: {e}")

# Define DAG
with DAG(
    dag_id='etl_minio_to_postgres',
    default_args=default_args,
    description='ETL from MinIO to PlayerStats in PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task: Execute ETL
    etl_task = PythonOperator(
        task_id='etl_minio_to_postgres',
        python_callable=etl_minio_to_postgres,
    )
