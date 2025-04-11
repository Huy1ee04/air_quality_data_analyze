from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, from_unixtime, hour, to_date, year, month, dayofmonth, monotonically_increasing_id
import os
from dotenv import load_dotenv
from google.cloud import bigquery
from datetime import datetime

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

# Tìm thời điểm lưu cuối cùng
bq_client = bigquery.Client()
query = """
    SELECT MAX(TIMESTAMP(datetime)) as max_datetime
    FROM `iron-envelope-455716-g8.aq_data.fact_air_quality`
"""
result = bq_client.query(query).result()
max_datetime = None
row = result.next()
max_datetime = row["max_datetime"]


# Đọc từ GCS
df = spark.read.parquet("gs://project-bigdata-bucket/air_quality_data/")

# Chuyển đổi timestamp
df = df.withColumn("datetime", from_unixtime(col("dt"))) \
       .withColumn("date", to_date(col("datetime"))) \
       .withColumn("hour", hour(col("datetime")))

# Lọc dữ liệu chưa lưu
if max_datetime:
    spark_datetime = datetime.strptime(str(max_datetime), "%Y-%m-%d %H:%M:%S.%f")
    df = df.filter(col("datetime") > spark_datetime)

# ==================== Dimension Tables ====================

# dim_date: date + các thành phần thời gian
dim_date = df.select("date") \
    .distinct() \
    .withColumn("year", year("date")) \
    .withColumn("month", month("date")) \
    .withColumn("day", dayofmonth("date")) \
    .withColumn("date_id", monotonically_increasing_id())

# dim_location: lat + lon
dim_location = df.select("lat", "lon") \
    .distinct() \
    .withColumn("location_id", monotonically_increasing_id())

# ==================== Join để gắn ID vào fact ====================

# Join dim_date để lấy date_id
df_with_date_id = df.join(dim_date, on="date", how="left")

# Join dim_location để lấy location_id
df_with_ids = df_with_date_id.join(dim_location, on=["lat", "lon"], how="left")

# ==================== Fact Table ====================
fact_air_quality = df_with_ids.select(
    "date_id",
    "hour",
    "location_id",
    "pm2_5",
    "pm10",
    "o3",
    "so2",
    "no2",
    "co",
    "aqi"
).withColumn("fact_id", monotonically_increasing_id())

# ===== Bảng mới: Trung bình các chỉ số theo ngày + tọa độ =====
daily_avg_df = fact_air_quality.groupBy("date_id", "location_id").agg(
    avg("aqi").alias("daily_avg_aqi"),
    avg("pm2_5").alias("daily_avg_pm2_5"),
    avg("pm10").alias("daily_avg_pm10"),
    avg("o3").alias("daily_avg_o3"),
    avg("so2").alias("daily_avg_so2"),
    avg("no2").alias("daily_avg_no2"),
    avg("co").alias("daily_avg_co"),
).orderBy("date_id", "location_id")


# ==================== Hiển thị ====================
print("✅ Dimension Date:")
dim_date.show()
print("✅ Dimension Location:")
dim_location.show()
print("✅ Fact Table:")
fact_air_quality.show()
print("✅ Avg per day Table:")
daily_avg_df.show()

# ==================== Tuỳ chọn: Ghi ra GCS ====================
# dim_date.write.mode("overwrite").parquet("gs://project-bigdata-bucket/star/dim_date")
# dim_location.write.mode("overwrite").parquet("gs://project-bigdata-bucket/star/dim_location")
# fact_air_quality.write.mode("overwrite").parquet("gs://project-bigdata-bucket/star/fact_air_quality")
# daily_avg_aqi_df.write.mode("overwrite").parquet("gs://project-bigdata-bucket/star/daily_avg_aqi")

# ==================== Tuỳ chọn: Ghi ra BigQuery ====================
dim_date.write.format("bigquery") \
    .option("table", "iron-envelope-455716-g8.aq_data.dim_date") \
    .option("parentProject", "iron-envelope-455716-g8") \
    .option("temporaryGcsBucket", "project-bigdata-bucket") \
    .mode("append") \
    .save()

dim_location.write.format("bigquery") \
    .option("table", "iron-envelope-455716-g8.aq_data.dim_location") \
    .option("parentProject", "iron-envelope-455716-g8") \
    .option("temporaryGcsBucket", "project-bigdata-bucket") \
    .mode("append") \
    .save()

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
