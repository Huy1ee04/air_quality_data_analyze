from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, from_unixtime, hour, to_date, year, month, dayofmonth, monotonically_increasing_id,udf, hash, round as spark_round
from pyspark.sql.types import DoubleType, DecimalType, StructType, StructField, LongType, StringType
import re
import os
from dotenv import load_dotenv
from google.cloud import bigquery, storage
from datetime import datetime,timedelta
from decimal import Decimal
import pyarrow
from pyarrow.fs import GcsFileSystem

# Load environment variables
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS không được tìm thấy! Kiểm tra lại .env file.")

# Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("Read from GCS") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.jars", "/opt/bitnami/spark/jars/gcs-connector-hadoop3-latest.jar, /opt/bitnami/spark/jars/spark-bigquery-with-dependencies_2.12-0.36.1.jar") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
    .config("spark.hadoop.google.cloud.project.id", "iron-envelope-455716-g8") \
    .getOrCreate()

client = storage.Client()
bucket = client.get_bucket("project-bigdata-bucket")
prefix = "air_quality_data/"
folders = set()

with open("/opt/spark/start_date.txt", "r") as f:
    start_date_str = f.read().strip()
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

latest_date = None

# Lấy các phân vùng có dạng year=YYYY/month=MM/day=DD
for blob in bucket.list_blobs(prefix=prefix):
    match = re.match(r"air_quality_data/year=(\d+)/month=(\d+)/day=(\d+)/", blob.name)
    if match:
        y, m, d = map(int, match.groups())
        folder_date = datetime(y, m, d)
        
        if folder_date >= start_date:
            folder_path = f"air_quality_data/year={y}/month={m}/day={d}"
            folders.add(folder_path)
            if latest_date is None or folder_date > latest_date:
                latest_date = folder_date

folder_paths = sorted(folders)
print(f"✅ Tìm được {len(folder_paths)} ngày dữ liệu phân vùng.")


full_path = f"gs://project-bigdata-bucket/{folder_paths[0]}"
print(f"🚀 Đang đọc dữ liệu: {full_path}")

df_all = spark.read.parquet(full_path)

dim_location = df_all.select("lat", "lon").distinct() \
    .withColumn("location_id", hash("lat", "lon"))

# ✅ Ghi dim_location 1 lần duy nhất
dim_location.write.format("bigquery") \
    .option("table", "iron-envelope-455716-g8.aq_data.dim_location") \
    .option("parentProject", "iron-envelope-455716-g8") \
    .option("temporaryGcsBucket", "project-bigdata-bucket") \
    .mode("append") \
    .save()

for folder in folder_paths[::-1]:
    full_path = f"gs://project-bigdata-bucket/{folder}"
    print(f"🚀 Đang xử lý phân vùng: {full_path}")

    # Đọc toàn bộ thư mục (không cần lặp từng file nữa)
    df = spark.read.parquet(full_path)

    # Tiền xử lý dữ liệu
    df = df.withColumn("date", to_date(from_unixtime(col("dt")))) \
           .withColumn("hour", hour(from_unixtime(col("dt")))) \

    dim_date = df.select("date").distinct() \
        .withColumn("year", year("date")) \
        .withColumn("month", month("date")) \
        .withColumn("day", dayofmonth("date")) \
        .withColumn("date_id", (col("year")*10000 + col("month")*100 + col("day")).cast("long"))

    # AQI UDF
    def calculate_aqi_pm25(concentration):
        # Định nghĩa các khoảng nồng độ và AQI tương ứng
        breakpoints = [
            (0.0, 12.0, 0, 50),
            (12.1, 35.4, 51, 100),
            (35.5, 55.4, 101, 150),
            (55.5, 150.4, 151, 200),
            (150.5, 250.4, 201, 300),
            (250.5, 350.4, 301, 400),
            (350.5, 500.4, 401, 500)
        ]
        for bp_lo, bp_hi, i_lo, i_hi in breakpoints:
            if bp_lo <= concentration <= bp_hi:
                return ((i_hi - i_lo) / (bp_hi - bp_lo)) * (concentration - bp_lo) + i_lo
        return None

    aqi_pm25_udf = udf(calculate_aqi_pm25, DoubleType())
    df = df.withColumn("aqi", aqi_pm25_udf(df["pm2_5"]))


    # ========== Fact Table ==========
    df_with_date_id = df.join(dim_date, on="date", how="left")
    df_with_ids = df_with_date_id.join(dim_location, on=["lat", "lon"], how="left")

    # ✅ Ghi dim_date và dim_location 1 lần duy nhất
    dim_date.write.format("bigquery") \
        .option("table", "iron-envelope-455716-g8.aq_data.dim_date") \
        .option("parentProject", "iron-envelope-455716-g8") \
        .option("temporaryGcsBucket", "project-bigdata-bucket") \
        .mode("append") \
        .save()

    fact_air_quality = df_with_ids.select(
        "date_id", "hour", "location_id", "pm2_5", "pm10", "o3", "so2", "no2", "co", "aqi", "aqi_level"
    ).withColumn("fact_id", hash("date_id", "hour", "location_id"))

    daily_avg_df = fact_air_quality.groupBy("date_id", "location_id").agg(
        avg("aqi").alias("daily_avg_aqi"),
        avg("pm2_5").alias("daily_avg_pm2_5"),
        avg("pm10").alias("daily_avg_pm10"),
        avg("o3").alias("daily_avg_o3"),
        avg("so2").alias("daily_avg_so2"),
        avg("no2").alias("daily_avg_no2"),
        avg("co").alias("daily_avg_co")
    ).orderBy("date_id", "location_id")


    # ==================== Hiển thị ====================
    # print("✅ Dimension Date:")
    # dim_date.show()
    # print("✅ Dimension Location:")
    # dim_location.show()
    # print("✅ Fact Table:")
    # fact_air_quality.show()
    # print("✅ Avg per day Table:")
    # daily_avg_df.show()

    # ==================== Tuỳ chọn: Ghi ra GCS ====================
    # dim_date.write.mode("overwrite").parquet("gs://project-bigdata-bucket/star/dim_date")
    # dim_location.write.mode("overwrite").parquet("gs://project-bigdata-bucket/star/dim_location")
    # fact_air_quality.write.mode("overwrite").parquet("gs://project-bigdata-bucket/star/fact_air_quality")
    # daily_avg_aqi_df.write.mode("overwrite").parquet("gs://project-bigdata-bucket/star/daily_avg_aqi")

    # ==================== Ghi ra BigQuery ====================
    fact_air_quality.write.format("bigquery") \
        .option("table", "iron-envelope-455716-g8.aq_data.fact_air_quality") \
        .option("parentProject", "iron-envelope-455716-g8") \
        .option("temporaryGcsBucket", "project-bigdata-bucket") \
        .mode("append") \
        .save()

    daily_avg_df.write.format("bigquery") \
        .option("table", "iron-envelope-455716-g8.aq_data.daily_avg") \
        .option("parentProject", "iron-envelope-455716-g8") \
        .option("temporaryGcsBucket", "project-bigdata-bucket") \
        .mode("append") \
        .save()

print("✅ Đã ghi dữ liệu vào BigQuery thành công!")

# Sau vòng lặp, lưu ngày kế tiếp của latest_date vào file nếu có
if latest_date:
    next_date = latest_date + timedelta(days=1)
    with open("/opt/spark/start_date.txt", "w") as f:
        f.write(next_date.strftime("%Y-%m-%d"))
    print(f"✅ Đã lưu ngày tiếp theo: {next_date.strftime('%Y-%m-%d')}")
else:
    print("⚠️ Không có ngày nào phù hợp để cập nhật.")