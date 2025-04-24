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
GOOGLE_SERVICE_ACCOUNT_KEY = os.getenv("GOOGLE_SERVICE_ACCOUNT_KEY")
LOCAL_FILE_PATH = "/opt/airflow/dags/air_quality_data.json"

if not GOOGLE_SERVICE_ACCOUNT_KEY:
    raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY không được tìm thấy! Kiểm tra lại file .env")

# Set Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_SERVICE_ACCOUNT_KEY

# Initialize GCS Client
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET)

def upload_to_gcs():
    """Upload JSON data file to Google Cloud Storage as partitioned Parquet."""
    if not os.path.exists(LOCAL_FILE_PATH):
        raise FileNotFoundError(f"JSON data file not found at {LOCAL_FILE_PATH}")

    with open(LOCAL_FILE_PATH, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Error decoding JSON file: {e}")
    
    df = pd.DataFrame(data)
    
    # Ensure 'dt' column exists and is datetime
    if "dt" not in df.columns:
        raise KeyError("Column 'dt' not found in the JSON data.")
    
    try:
        df['dt'] = pd.to_datetime(df['dt'], unit='s')
    except Exception as e:
        raise ValueError(f"Error converting 'dt' column to datetime: {e}")
    
    df["year"] = df["dt"].dt.year
    df["month"] = df["dt"].dt.month
    df["day"] = df["dt"].dt.day

    # Convert 'dt' back to integer timestamp
    df['dt'] = df['dt'].astype('int64') // 1_000_000_000

    # Ensure partition columns exist
    if not all(col in df.columns for col in ["year", "month", "day"]):
        raise KeyError("Partition columns 'year', 'month', or 'day' are missing in the DataFrame.")

    table = pa.Table.from_pandas(df)
    gcs = pa.fs.GcsFileSystem()

    try:
        pq.write_to_dataset(
            table,
            root_path=f"{GCS_BUCKET}/{GCS_FOLDER}",
            partition_cols=["year", "month", "day"],
            filesystem=gcs
        )
    except Exception as e:
        raise RuntimeError(f"Error uploading data to GCS: {e}")
    
    print(f"Dữ liệu đã được tải lên GCS dưới dạng phân vùng Parquet tại gs://{GCS_BUCKET}/{GCS_FOLDER}")
