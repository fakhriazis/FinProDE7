import requests
from bs4 import BeautifulSoup
import json
import boto3
from botocore.exceptions import NoCredentialsError
from urllib.parse import urlparse

# Konfigurasi MinIO
MINIO_ENDPOINT = "localhost:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
BUCKET_NAME = "arsenal-data"

# Daftar URL pemain
PLAYER_URLS = [
    "https://fbref.com/en/players/fed7cb61/Kai-Havertz",
    "https://fbref.com/en/players/48a5a5d6/Gabriel-Martinelli",
    "https://fbref.com/en/players/1c95bcca/Bukayo-Saka"
    "https://fbref.com/en/players/79300479/Martin-Odegaard"
    "https://fbref.com/en/players/1c7012b8/Declan-Rice"
    "https://fbref.com/en/players/529f49ab/Thomas-Partey"
    "https://fbref.com/en/players/972aeb2a/William-Saliba"
    "https://fbref.com/en/players/67ac5bb8/Gabriel-Magalhaes"
    "https://fbref.com/en/players/35e413f1/Ben-White"
    "https://fbref.com/en/players/51cf8561/Oleksandr-Zinchenko"
    "https://fbref.com/en/players/98ea5115/David-Raya"
]

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
    except Exception as e:
        print(f"An error occurred while uploading to MinIO: {e}")

# Fungsi untuk mengambil ID pemain dari URL
def extract_player_id(url):
    parsed_url = urlparse(url)
    path_parts = parsed_url.path.split("/")
    if "players" in path_parts:
        player_index = path_parts.index("players") + 1
        if player_index < len(path_parts):
            return path_parts[player_index]
    return "Unknown"

# Fungsi untuk scraping biodata pemain
def scrape_player_biodata(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Failed to retrieve the webpage for URL {url}. Status code:", response.status_code)
        return None

    soup = BeautifulSoup(response.text, "html.parser")
    player_info = {}

    # Mengambil ID pemain dari URL
    player_info['player_id'] = extract_player_id(url)

    # Mengambil nama pemain
    player_name = soup.find("h1").text.strip()
    player_info['player_name'] = player_name

    # Posisi
    position_element = soup.find("strong", string="Position:")
    if position_element:
        player_info['position'] = position_element.next_sibling.strip()

    # Footed
    footed_element = soup.find("strong", string="Footed:")
    if footed_element:
        player_info['footed'] = footed_element.next_sibling.strip()

    # Club
    club_element = soup.find("strong", string="Club:")
    if club_element:
        club_link = club_element.find_next("a")
        if club_link:
            player_info['club'] = club_link.text.strip()
        else:
            player_info['club'] = "Unknown"
    else:
        player_info['club'] = "Unknown"

    # Nationality
    nationality_element = soup.find("strong", string="National Team:")
    if nationality_element:
        nationality_link = nationality_element.find_next("a")
        if nationality_link:
            player_info['nationality'] = nationality_link.text.strip()
        else:
            player_info['nationality'] = "Unknown"
    else:
        player_info['nationality'] = "Unknown"

    # Nama file JSON berdasarkan club dan nama pemain
    club_cleaned = player_info['club'].replace(" ", "_").lower()
    player_cleaned = player_info['player_name'].replace(" ", "_").lower()
    json_file_path = f"{club_cleaned}_{player_cleaned}_biodata.json"

    # Menyimpan data ke file JSON
    with open(json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(player_info, json_file, indent=4, ensure_ascii=False)

    print(f"Data for {player_name} has been saved to '{json_file_path}'")

    # Upload ke MinIO dengan nama objek dinamis
    object_name = json_file_path  # Nama file sama dengan nama object
    upload_to_minio(json_file_path, BUCKET_NAME, object_name)

    return player_info

# Scraping data banyak pemain dan menyimpan ke MinIO
if __name__ == "__main__":
    for url in PLAYER_URLS:
        print(f"Scraping data for URL: {url}")
        player_biodata = scrape_player_biodata(url)
        if player_biodata is not None:
            print("Player Biodata:", player_biodata)
        print("-" * 50)
