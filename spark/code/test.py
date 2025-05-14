from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, from_unixtime, hour, to_date, year, month, dayofmonth, monotonically_increasing_id,udf, hash
from pyspark.sql.functions import round as spark_round
from pyspark.sql.types import DoubleType, DecimalType
import re
import os
from dotenv import load_dotenv
from google.cloud import bigquery, storage
from datetime import datetime,timedelta
from decimal import Decimal, ROUND_HALF_UP
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
import pandas as pd
from gcsfs import GCSFileSystem
from decimal import Decimal, ROUND_HALF_UP
import pyarrow.parquet as pq
import pyarrow.fs
import pandas as pd

from pyarrow.fs import GcsFileSystem

# Load environment variables
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS không được tìm thấy! Kiểm tra lại .env file.")

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("Test spark") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.jars", "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar, /opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
    .config("spark.hadoop.google.cloud.project.id", "iron-envelope-455716-g8") \
    .getOrCreate()

# Khởi tạo GCS filesystem
gcs = GcsFileSystem()

# Đường dẫn thư mục chứa các file parquet
gcs_path = "project-bigdata-bucket/air_quality_data/year=2024/month=12/day=31"
files = gcs.get_file_info(pyarrow.fs.FileSelector(gcs_path, recursive=True))

# Chỉ giữ các file parquet
parquet_files = [f.path for f in files if f.is_file and f.path.endswith(".parquet")]

# Cột cần đọc (bỏ qua cột 'year', 'month', 'day', '__index_level_0__')
columns_needed = ["dt", "lat", "lon", "aqi_level", "co", "no", "no2", "o3", "so2", "pm2_5", "pm10", "nh3"]

# Hàm xử lý lat
def convert_to_decimal(val):
    try:
        if pd.isnull(val):
            return None
        return Decimal(val).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    except:
        return None

dfs = []

for file_path in parquet_files:
    with gcs.open_input_file(file_path) as f:
        table = pq.read_table(f, columns=columns_needed)
        df = table.to_pandas()
        df["lat"] = df["lat"].apply(convert_to_decimal)
        dfs.append(df)

spark_df = pd.concat(dfs, ignore_index=True)

# Hàm tính AQI từ PM2.5
def calculate_aqi_pm25(concentration):
    try:
        concentration = Decimal(concentration)
        breakpoints = [
            (Decimal('0.0'), Decimal('12.0'), Decimal('0'), Decimal('50')),
            (Decimal('12.1'), Decimal('35.4'), Decimal('51'), Decimal('100')),
            (Decimal('35.5'), Decimal('55.4'), Decimal('101'), Decimal('150')),
            (Decimal('55.5'), Decimal('150.4'), Decimal('151'), Decimal('200')),
            (Decimal('150.5'), Decimal('250.4'), Decimal('201'), Decimal('300')),
            (Decimal('250.5'), Decimal('350.4'), Decimal('301'), Decimal('400')),
            (Decimal('350.5'), Decimal('500.4'), Decimal('401'), Decimal('500'))
        ]
        for bp_lo, bp_hi, i_lo, i_hi in breakpoints:
            if bp_lo <= concentration <= bp_hi:
                return float(((i_hi - i_lo) / (bp_hi - bp_lo)) * (concentration - bp_lo) + i_lo)
    except:
        return None

aqi_pm25_udf = udf(calculate_aqi_pm25, DoubleType())

# Các bước xử lý cột Spark
spark_df = spark_df \
    .withColumn("datetime", from_unixtime(col("dt"))) \
    .withColumn("date", to_date(col("datetime"))) \
    .withColumn("hour", hour(col("datetime"))) \
    .withColumn("lat", (spark_round(col("lat") / 0.25, 0) * 0.25).cast(DecimalType(5, 2))) \
    .withColumn("lon", (spark_round(col("lon") / 0.25, 0) * 0.25).cast(DecimalType(5, 2))) \
    .withColumn("pm2_5", col("pm2_5").cast(DecimalType(5, 2))) \
    .withColumn("co", col("co").cast(DecimalType(5, 2))) \
    .withColumn("aqi", aqi_pm25_udf(col("pm2_5")).cast(DecimalType(5, 2)))

# In kết quả
spark_df.select("dt", "lat", "lon", "pm2_5", "co", "date", "aqi", "hour").show(20, truncate=False)
