import os
import ijson
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

# Load environment variables
load_dotenv()

GCS_BUCKET = "project-bigdata-bucket"
GCS_FOLDER = "air_quality_data"
GOOGLE_SERVICE_ACCOUNT_KEY = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
LOCAL_FILE_PATH = "/opt/airflow/dags/air_quality_data.json"
if not GOOGLE_SERVICE_ACCOUNT_KEY:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY không được tìm thấy! Kiểm tra lại file .env")

# Set Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_SERVICE_ACCOUNT_KEY

# Khởi tạo GCS
gcs = pa.fs.GcsFileSystem()

# Initialize GCS Client
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET)


def upload_large_json_to_gcs(batch_size=25000):
    def write_batch_to_gcs(batch, batch_num):
        df = pd.DataFrame(batch)

        df["dt"] = pd.to_datetime(df["dt"], unit="s", errors='coerce')
        df["year"] = df["dt"].dt.year
        df["month"] = df["dt"].dt.month
        df["day"] = df["dt"].dt.day
        df["dt"] = df["dt"].astype('int64') // 1_000_000_000

        float_columns = ["co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3", "lat", "lon"]
        for col in float_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype(float).round(2)

        # Ghi từng nhóm partition theo ngày riêng
        grouped = df.groupby(["year", "month", "day"])

        # Define consistent schema
        schema = pa.schema([
            ("dt", pa.int64()),
            ("lat", pa.float64()),
            ("lon", pa.float64()),
            ("co", pa.float64()),
            ("no", pa.float64()),
            ("no2", pa.float64()),
            ("o3", pa.float64()),
            ("so2", pa.float64()),
            ("pm2_5", pa.float64()),
            ("pm10", pa.float64()),
            ("nh3", pa.float64()),
            ("aqi_level", pa.string()),
            # add other fields if needed
        ])

        for (year, month, day), group_df in grouped:
            # Bỏ 3 trường phân vùng khỏi DataFrame
            group_df = group_df.drop(columns=["year", "month", "day"])

            table = pa.Table.from_pandas(group_df, schema=schema, preserve_index=False)
            partition_path = f"{GCS_FOLDER}/year={year}/month={month}/day={day}/batch_{batch_num}.parquet"

            with gcs.open_output_stream(f"{GCS_BUCKET}/{partition_path}") as out_stream:
                pq.write_table(table, out_stream)

            print(f"✅ Batch {batch_num}: Đã ghi {len(group_df)} bản ghi vào {partition_path}")

    with open(LOCAL_FILE_PATH, "r", encoding="utf-8") as f:
        parser = ijson.items(f, "item")
        batch = []
        count = 0

        for record in parser:
            batch.append(record)
            if len(batch) >= batch_size:
                write_batch_to_gcs(batch, count)
                count += 1
                batch = []

        if batch:
            write_batch_to_gcs(batch, count)

    print(f"🎉 Hoàn tất tải dữ liệu lên GCS theo từng batch Parquet!")