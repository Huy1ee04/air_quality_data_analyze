from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, from_unixtime, hour, to_date, year, month, dayofmonth, monotonically_increasing_id
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("AirQualitySchema") \
    .getOrCreate()

# Đọc từ GCS
df = spark.read.parquet("gs://project-bigdata-bucket/air_quality_data/")

print("DataFrame loaded successfully.")
