import requests
from bs4 import BeautifulSoup
import json
import boto3
from botocore.exceptions import NoCredentialsError

# URL untuk Thomas Partey Match Logs
URL = "https://fbref.com/en/players/529f49ab/matchlogs/2023-2024/Thomas-Partey-Match-Logs"

# Konfigurasi MinIO
MINIO_ENDPOINT = "localhost:9000"  
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "arsenal-data"
OBJECT_NAME = "thomas_partey_match_logs_2023_2024.json"

# Fungsi untuk upload file ke MinIO
def upload_to_minio(file_path, bucket_name, object_name):
    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{MINIO_ENDPOINT}",
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        s3.upload_file(file_path, bucket_name, object_name)
        print(f"File '{object_name}' successfully uploaded to bucket '{bucket_name}' on MinIO.")
    except FileNotFoundError:
        print("The file was not found.")
    except NoCredentialsError:
        print("Credentials not available.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Fungsi untuk scraping match logs
def scrape_match_logs_to_json(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Failed to retrieve the webpage. Status code:", response.status_code)
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Coba temukan semua tabel
    tables = soup.find_all("table")
    if not tables:
        print("No tables found on the page!")
        return None

    print(f"Found {len(tables)} tables. Checking for match logs...")

    # Iterasi tabel untuk menemukan yang berisi match logs
    for table in tables:
        if "matchlogs" in table.get("id", ""):  
            print("Match logs table found!")

            # Mengambil header tabel
            headers = [th['data-stat'] for th in table.find("thead").find_all("th") if 'data-stat' in th.attrs]

            # Mengambil data dari setiap baris
            rows = table.find("tbody").find_all("tr")
            match_data = []
            for row in rows:
                if row.find("th", {"scope": "row"}):  
                    row_data = {
                        header: row.find("td", {"data-stat": header}).text.strip() if row.find("td", {"data-stat": header}) else ''
                        for header in headers
                    }
                    match_data.append(row_data)

            # Menyimpan data ke file JSON
            json_file_path = "thomas_partey_match_logs_2023_2024.json"
            with open(json_file_path, "w", encoding="utf-8") as json_file:
                json.dump(match_data, json_file, indent=4, ensure_ascii=False)

            print("Data has been saved to 'thomas_partey_match_logs_2023_2024.json'")

            # Upload ke MinIO
            upload_to_minio(json_file_path, BUCKET_NAME, OBJECT_NAME)

            return match_data

    print("No match logs table found!")
    return None

# Scraping data dan menyimpan ke MinIO
if __name__ == "__main__":
    match_logs = scrape_match_logs_to_json(URL)
    if match_logs is not None:
        print("Sample data:", match_logs[:3])
