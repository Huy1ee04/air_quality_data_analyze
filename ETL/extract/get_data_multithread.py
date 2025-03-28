import os
import json
import requests
import time
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Tải biến môi trường từ .env
load_dotenv()

# OpenWeather API Key
API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution/history"

# Phạm vi tọa độ của Việt Nam
lat_start, lat_end = 8.0, 23.5
lon_start, lon_end = 102.0, 110.0
step = 0.25
file_path = "air_quality_data.json"
MAX_WORKERS = 10  # Số lượng luồng chạy song song

def frange(start, stop, step):
    """Hàm tạo danh sách tọa độ với bước nhảy"""
    while start <= stop:
        yield round(start, 2)
        start += step

def fetch_air_quality(lat, lon):
    """Hàm gọi API để lấy dữ liệu"""
    params = {
        "lat": lat,
        "lon": lon,
        "start": 1672531200,  # 1/1/2023 00:00:00 UTC
        "end": 1735689600,    # 1/1/2025 00:00:00 UTC
        "appid": API_KEY
    }

    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        data = response.json()
        if "list" in data and data["list"]:
            return [
                {
                    "dt": entry["dt"],
                    "lat": lat,
                    "lon": lon,
                    "main_aqi": entry["main"]["aqi"],
                    "co": entry["components"]["co"],
                    "no": entry["components"]["no"],
                    "no2": entry["components"]["no2"],
                    "o3": entry["components"]["o3"],
                    "so2": entry["components"]["so2"],
                    "pm2_5": entry["components"]["pm2_5"],
                    "pm10": entry["components"]["pm10"],
                    "nh3": entry["components"]["nh3"]
                }
                for entry in data["list"]
            ]
    return []

def fetch_air_quality_data():
    """Chạy đa luồng để lấy dữ liệu nhanh hơn"""
    all_data = []
    coords = [(lat, lon) for lat in frange(lat_start, lat_end, step) for lon in frange(lon_start, lon_end, step)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_coord = {executor.submit(fetch_air_quality, lat, lon): (lat, lon) for lat, lon in coords}

        for future in as_completed(future_to_coord):
            lat, lon = future_to_coord[future]
            try:
                data = future.result()
                if data:
                    all_data.extend(data)
                    print(f"✅ Data saved for lat={lat}, lon={lon}")
            except Exception as e:
                print(f"❌ Error fetching data for lat={lat}, lon={lon}: {e}")

    # Lưu file JSON
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=4)

    print(f"🎉 Dữ liệu đã được thu thập xong! Lưu tại {file_path}")

fetch_air_quality_data()