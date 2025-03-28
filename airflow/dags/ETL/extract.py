import requests
import json
import time
import os
from dotenv import load_dotenv

# Load API key từ .env
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution/history"

# Phạm vi tọa độ Việt Nam (tăng step để giảm request)
lat_start, lat_end = 8.0,  23.5
lon_start, lon_end = 102.0, 110.0
step = 0.25  

file_path = "/opt/airflow/dags/air_quality_data.json"

def fetch_air_quality_data():
    # Tải dữ liệu cũ nếu file đã tồn tại
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                all_data = json.load(f)
            except json.JSONDecodeError:
                all_data = []  # Nếu file lỗi, tạo danh sách mới
    else:
        all_data = []

    # Lặp qua từng điểm lat, lon
    lat = lat_start
    while lat <= lat_end:
        lon = lon_start
        while lon <= lon_end:
            print(f"Fetching data for lat={lat}, lon={lon}...")

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
                if "list" in data and data["list"]:  # Kiểm tra dữ liệu hợp lệ
                    for entry in data["list"]:
                        record = {
                            "dt": entry["dt"],
                            "lat": lat,
                            "lon": lon,
                            "main.aqi": entry["main"]["aqi"],
                            "components.co": entry["components"]["co"],
                            "components.no": entry["components"]["no"],
                            "components.no2": entry["components"]["no2"],
                            "components.o3": entry["components"]["o3"],
                            "components.so2": entry["components"]["so2"],
                            "components.pm2_5": entry["components"]["pm2_5"],
                            "components.pm10": entry["components"]["pm10"],
                            "components.nh3": entry["components"]["nh3"]
                        }
                        all_data.append(record)

                    # Ghi vào file từng dòng
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(all_data, f, ensure_ascii=False, indent=4)

                    print(f"Data saved for lat={lat}, lon={lon}")
                else:
                    print(f"No valid data for lat={lat}, lon={lon}")

            elif response.status_code == 429:  # Quá giới hạn request
                print("Rate limit exceeded. Sleeping for 60 seconds...")
                time.sleep(60)
                continue  # Thử lại cùng tọa độ sau khi chờ

            else:
                print(f"Failed to fetch data: {response.status_code}")

            lon += step  # Luôn tăng giá trị lon, tránh vòng lặp vô tận

            time.sleep(2)  # Tránh rate limit

        lat += step  # Tăng lat sau khi quét xong 1 hàng

    print("✅ Done! All data collected.")

