import os
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Google Cloud Storage Configuration
GCS_BUCKET = "project-bigdata-bucket"
GCS_FOLDER = "air_quality_data/"
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
LOCAL_FILE_PATH = "/opt/airflow/dags/air_quality_data.json"

if not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS không được tìm thấy! Kiểm tra lại .env file.")

# Initialize GCS Client
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET)

def upload_to_gcs():
    """Upload JSON data file to Google Cloud Storage as partitioned Parquet."""
    if not os.path.exists(LOCAL_FILE_PATH):
        raise FileNotFoundError(f"JSON data file not found at {LOCAL_FILE_PATH}")

    with open(LOCAL_FILE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    # Ensure time column is datetime
    if not pd.api.types.is_datetime64_any_dtype(df['dt']):
        df['dt'] = pd.to_datetime(df['dt'], unit='s')
    
    df["year"] = df["dt"].dt.year
    df["month"] = df["dt"].dt.month
    df["day"] = df["dt"].dt.day

    df['dt'] = df['dt'].astype('int64') // 1_000_000_000

    table = pa.Table.from_pandas(df)
    gcs = pa.fs.GcsFileSystem()
    
    pq.write_to_dataset(
        table,
        root_path=f"{GCS_BUCKET}/{GCS_FOLDER}",
        partition_cols=["year", "month", "day"],
        filesystem=gcs
    )
    
    print(f"Dữ liệu đã được tải lên GCS dưới dạng phân vùng Parquet tại gs://{GCS_BUCKET}/{GCS_FOLDER}")

upload_to_gcs()
