import os
import json
import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = "ap-southeast-1"
DYNAMODB_TABLE = "air-pollution-data"
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
file_path = "/opt/airflow/dags/air_quality_data.json"

if not ACCESS_KEY or not SECRET_KEY:
    raise ValueError("ACCESS_KEY hoặc SECRET_KEY không được tìm thấy! Kiểm tra lại .env file.")

# Kết nối DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    region_name=AWS_REGION,
    aws_access_key_id=ACCESS_KEY,
    aws_secret_access_key=SECRET_KEY
)
table = dynamodb.Table(DYNAMODB_TABLE)

def upload_to_dynamodb():
    """Tải dữ liệu từ JSON lên DynamoDB theo từng batch nhỏ"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"JSON data file not found at {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    BATCH_SIZE = 25  # DynamoDB chỉ cho phép tối đa 25 items mỗi batch
    total_records = len(data)
    uploaded = 0

    for i in range(0, total_records, BATCH_SIZE):
        batch_data = data[i:i+BATCH_SIZE]

        with table.batch_writer() as batch:
            for item in batch_data:
                lat_lon = f"{item['lat']}_{item['lon']}"
                clean_item = {
                    "lat_lon": lat_lon,
                    "dt": int(item["dt"]),
                    "main_aqi": item.get("main.aqi", None),
                }

                components_cleaned = {k.replace(".", "_"): str(v) for k, v in item.items() if k.startswith("components.")}

                clean_item.update(components_cleaned)
                batch.put_item(Item=clean_item)

        uploaded += len(batch_data)
        print(f"Đã tải {uploaded}/{total_records} bản ghi vào DynamoDB...")

    print(f"Hoàn thành tải {total_records} bản ghi vào DynamoDB!")

