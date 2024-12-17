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
BUCKET_NAME = "mancity-data"
OBJECT_NAME = "mancity_player.json"

# Daftar URL pemain
PLAYER_URLS = [
    "https://fbref.com/en/players/1f44ac21/Erling-Haaland",
    "https://fbref.com/en/players/b0b4fd3e/Jack-Grealish",
    "https://fbref.com/en/players/ed1e53f3/Phil-Foden",
    "https://fbref.com/en/players/e46012d4/Kevin-De-Bruyne",
    "https://fbref.com/en/players/6434f10d/Rodri",
    "https://fbref.com/en/players/79c0821a/Mateo-Kovacic",
    "https://fbref.com/en/players/eaeca114/Nathan-Ake",
    "https://fbref.com/en/players/89ac64a6/Manuel-Akanji",
    "https://fbref.com/en/players/5ad50391/Josko-Gvardiol",
    "https://fbref.com/en/players/86dd77d1/Kyle-Walker",
    "https://fbref.com/en/players/3bb7b8b4/Ederson"
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

    return player_info

# Scraping data banyak pemain dan menyimpan ke satu file JSON
if __name__ == "__main__":
    all_players_data = []  # List untuk menyimpan data semua pemain

    for url in PLAYER_URLS:
        print(f"Scraping data for URL: {url}")
        player_biodata = scrape_player_biodata(url)
        if player_biodata is not None:
            all_players_data.append(player_biodata)
            print(f"Added data for {player_biodata['player_name']}")
        print("-" * 50)

    # Menyimpan semua data pemain ke satu file JSON
    json_file_path = "mancity_player.json"
    with open(json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(all_players_data, json_file, indent=4, ensure_ascii=False)

    print(f"All player data has been saved to '{json_file_path}'")

    # Upload file JSON ke MinIO
    upload_to_minio(json_file_path, BUCKET_NAME, OBJECT_NAME)
