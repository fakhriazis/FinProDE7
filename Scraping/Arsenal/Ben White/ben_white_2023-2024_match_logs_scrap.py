import requests
from bs4 import BeautifulSoup
import pandas as pd
import json

# URL untuk Ben White Match Logs
URL = "https://fbref.com/en/players/35e413f1/matchlogs/2023-2024/Ben-White-Match-Logs"

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
            with open("ben_white_match_logs_2023_2024.json", "w", encoding="utf-8") as json_file:
                json.dump(match_data, json_file, indent=4, ensure_ascii=False)

            print("Data has been saved to 'ben_white_match_logs_2023_2024.json'")
            return match_data

    print("No match logs table found!")
    return None

# Scraping data dan menyimpan ke file JSON
if __name__ == "__main__":
    match_logs = scrape_match_logs_to_json(URL)
    if match_logs is not None:
        print("Sample data:", match_logs[:3])
