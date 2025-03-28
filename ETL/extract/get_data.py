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
MAX_WORKERS = 10  # Số luồng chạy song song

def frange(start, stop, step):
    """Hàm tạo danh sách tọa độ với bước nhảy"""
    while start <= stop:
        yield round(start, 2)
        start += step

def fetch_air_quality(lat, lon, retries=3, delay=5):
    """Gọi API với cơ chế retry nếu bị lỗi kết nối"""
    params = {
        "lat": lat,
        "lon": lon,
        "start": 1672531200,  # 1/1/2023 00:00:00 UTC
        "end": 1735689600,    # 1/1/2025 00:00:00 UTC
        "appid": API_KEY
    }

    for attempt in range(retries):
        try:
            response = requests.get(BASE_URL, params=params, timeout=10)  # Thêm timeout
            response.raise_for_status()  # Gây lỗi nếu mã status không phải 200
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

        except (requests.ConnectionError, requests.Timeout, requests.RequestException) as e:
            print(f"⚠️ Attempt {attempt+1}/{retries} failed for lat={lat}, lon={lon}: {e}")
            if attempt < retries - 1:
                time.sleep(delay)  # Đợi trước khi thử lại
            else:
                print(f"❌ Failed to fetch data for lat={lat}, lon={lon} after {retries} attempts.")
                return []

def fetch_air_quality_data():
    """Chạy đa luồng để lấy dữ liệu nhanh hơn và lưu từng phần"""
    coords = [(lat, lon) for lat in frange(lat_start, lat_end, step) for lon in frange(lon_start, lon_end, step)]

    # Kiểm tra nếu file đã tồn tại, mở theo chế độ append
    is_first_entry = not os.path.exists(file_path) or os.stat(file_path).st_size == 0
    with open(file_path, "a", encoding="utf-8") as f:
        if is_first_entry:
            f.write("[\n")  # Mở mảng JSON

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_coord = {executor.submit(fetch_air_quality, lat, lon): (lat, lon) for lat, lon in coords}

            for future in as_completed(future_to_coord):
                lat, lon = future_to_coord[future]
                try:
                    data = future.result()
                    if data:
                        for record in data:
                            json.dump(record, f, ensure_ascii=False)
                            f.write(",\n")  # Dùng `,` để tránh lỗi JSON

                        print(f"✅ Data saved for lat={lat}, lon={lon}")
                except Exception as e:
                    print(f"❌ Error fetching data for lat={lat}, lon={lon}: {e}")

        f.seek(f.tell() - 2, os.SEEK_SET)  # Xóa dấu `,` cuối cùng
        f.write("\n]")  # Đóng mảng JSON

    print(f"🎉 Dữ liệu đã được thu thập và lưu tại {file_path}")

fetch_air_quality_data()
