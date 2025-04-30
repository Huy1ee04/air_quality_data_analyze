import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType
from dotenv import load_dotenv

# Thiết lập logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load biến môi trường
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not found in environment variables")

KAFKA_ADDRESS = os.getenv('KAFKA_ADDRESS')
kafka_bootstrap_servers = f"{KAFKA_ADDRESS}:{os.getenv('BROKER1')},{KAFKA_ADDRESS}:{os.getenv('BROKER2')}"
kafka_topic = os.getenv('AQI_TOPIC')
bq_table = os.getenv('BQ_TABLE')
gcp_project_id = os.getenv('PROJECT_ID')
gcs_bucket = os.getenv('GCS_BUCKET')

# Tạo Spark session
def create_spark_session(app_name="AQIConsumer"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.jars", "/Users/buihung/project bigdata/kafka/jars/spark-3.5-bigquery-0.42.1.jar, /Users/buihung/project bigdata/kafka/jars/kafka-clients-3.5.1.jar, /Users/buihung/project bigdata/kafka/jars/spark-sql-kafka-0-10_2.12-3.5.5.jar, /Users/buihung/project bigdata/kafka/jars/spark-token-provider-kafka-0-10_2.12-3.5.5.jar") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
        .config("spark.hadoop.google.cloud.project.id", gcp_project_id) \
        .getOrCreate()

# Định nghĩa schema dữ liệu
def define_schema():
    return StructType() \
        .add("uid", StringType()) \
        .add("lat", DoubleType()) \
        .add("lon", DoubleType()) \
        .add("aqi", StringType()) \
        .add("time", StringType()) \
        .add("station", StringType())

# Ghi từng batch nhỏ vào BigQuery
def write_to_bigquery_batch(batch_df, batch_id):
    batch_df.write \
        .format("bigquery") \
        .option("table", bq_table) \
        .option("temporaryGcsBucket", gcs_bucket) \
        .mode("append") \
        .save()
    logger.info(f"Batch {batch_id} written to BigQuery.")

# Hàm xử lý và ghi
def process_and_write_to_bq(spark_session):
    df = spark_session.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()

    value_df = df.selectExpr("CAST(value AS STRING) as json_str")

    schema = define_schema()
    parsed_df = value_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")
    parsed_df = parsed_df.withColumn("time", to_timestamp(col("time")))

    # Ghi từng batch nhỏ
    query = parsed_df.writeStream \
        .foreachBatch(write_to_bigquery_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "/tmp/aqi_bq_checkpoint") \
        .trigger(processingTime="10 seconds") \
        .start()

    query.awaitTermination()

# Chạy pipeline
spark_session = create_spark_session()
process_and_write_to_bq(spark_session)
