import requests
import json
import time
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from great_expectations.dataset.pandas_dataset import PandasDataset

# Load API key từ .env
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    raise ValueError("API key is missing. Please set OPENWEATHER_API_KEY in your .env file.")

BASE_URL = "http://api.openweathermap.org/data/2.5/air_pollution/history"

# Phạm vi tọa độ Việt Nam (tăng step để giảm request)
lat_start, lat_end = 8.0, 23.5
lon_start, lon_end = 102.0, 110.5
step = 0.25  

# Đường dẫn đến file lưu trữ dữ liệu
file_path = "/opt/airflow/dags/air_quality_data.json"
start_time_file = "/opt/airflow/start_time.txt"

# Đọc thời gian bắt đầu từ file lưu trữ
def read_start_time():
    if os.path.exists(start_time_file):
        try:
            with open(start_time_file, "r") as f:
                return int(f.read().strip())
        except (ValueError, IOError) as e:
            print(f"Error reading start_time from {start_time_file}: {e}")
            return 1672531200   # Mặc định: 1/1/2023 00:00:00 UTC
    else:
        return 1672531200       # Mặc định: 1/1/2023 00:00:00 UTC

def write_new_start_time(end_time):
    with open(start_time_file, "w") as f:
        f.write(str(end_time))

def fetch_air_quality_data():
    # Lấy thời gian hiện tại
    current_unix_time = int(time.time())

    # Chuyển thành datetime
    current_dt = datetime.fromtimestamp(current_unix_time)

    # Lấy 23h00 cùng ngày
    end_of_day = current_dt.replace(hour=23, minute=0, second=0, microsecond=0)

    # Nếu thời gian hiện tại đã quá 23h thì dùng hôm nay, còn nếu chưa thì dùng hôm trước
    if current_dt.hour < 23:
        end_of_day = end_of_day - timedelta(days=1)

    # Chuyển về UNIX timestamp
    end_time = int(end_of_day.timestamp())

    lat = lat_start
    with open(file_path, "w", encoding="utf-8") as f:
        f.write("[")  # mở list JSON

        first = True
        while lat <= lat_end:
            lon = lon_start
            while lon <= lon_end:
                print(f"Fetching data for lat={lat}, lon={lon}...")

                params = {
                    "lat": lat,
                    "lon": lon,
                    "start": read_start_time(),
                    "end": end_time,
                    "appid": API_KEY
                }

                response = requests.get(BASE_URL, params=params)

                if response.status_code == 200:
                    data = response.json()
                    if "list" in data and data["list"]:
                        for entry in data["list"]:
                            record = {
                                "dt": entry["dt"],
                                "lat": lat,
                                "lon": lon,
                                "aqi_level": entry["main"]["aqi"],
                                "co": entry["components"]["co"],
                                "no": entry["components"]["no"],
                                "no2": entry["components"]["no2"],
                                "o3": entry["components"]["o3"],
                                "so2": entry["components"]["so2"],
                                "pm2_5": entry["components"]["pm2_5"],
                                "pm10": entry["components"]["pm10"],
                                "nh3": entry["components"]["nh3"]
                            }
                            if not first:
                                f.write(",\n")
                            f.write(json.dumps(record, ensure_ascii=False))
                            first = False
                    else:
                        print(f"No valid data for lat={lat}, lon={lon}")
                elif response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    print(f"Rate limit exceeded. Sleeping for {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                else:
                    print(f"Failed to fetch data: {response.status_code}")
                    print(f"Response: {response.text}")

                lon += step
                time.sleep(0.5)
            lat += step

        f.write("]")  # đóng list JSON

    write_new_start_time(end_time)
    print(f"Data written incrementally to {file_path}")
    print(f"New start time saved: {end_time}")

    # Đọc dữ liệu JSON đã lưu thành DataFrame
    df = pd.read_json(file_path)

    # Bọc bằng Great Expectations
    gx_df = PandasDataset(df)

    # Thêm các expectation kiểm tra dữ liệu
    gx_df.expect_column_values_to_not_be_null("dt")
    gx_df.expect_column_values_to_be_between("aqi_level", min_value=1, max_value=5)
    gx_df.expect_column_values_to_be_in_set("lat", list(np.arange(8.0, 23.5 + 0.25, 0.25)))

    # Thực thi và hiển thị kết quả
    results = gx_df.validate()
    print(results)