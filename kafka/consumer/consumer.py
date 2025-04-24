import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from dotenv import load_dotenv
import pyspark.sql.types as T
from pyspark.sql.types import StructType, StringType, DoubleType
import logging
from src.schema import WEATHER_SCHEMA
from pyspark.sql.functions import from_json, col, to_timestamp
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if not GOOGLE_APPLICATION_CREDENTIALS:
    raise ValueError("GOOGLE_APPLICATION_CREDENTIALS not found in environment variables")

KAFKA_ADDRESS = os.getenv('KAFKA_ADDRESS')
kafka_bootstrap_servers = f"{KAFKA_ADDRESS}:{os.getenv('BROKER1')},{KAFKA_ADDRESS}:{os.getenv('BROKER2')}"  # Cấu hình Kafka
kafka_topic = os.getenv('AQI_TOPIC')
bq_table = "your_project_id.dataset.aqi_table"
gcp_project_id = os.getenv('PROJECT_ID')

def create_spark_session(app_name="AQIConsumer"):
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()

def define_schema():
    return StructType() \
        .add("uid", StringType()) \
        .add("lat", DoubleType()) \
        .add("lon", DoubleType()) \
        .add("aqi", StringType()) \
        .add("time", StringType()) \
        .add("station", StringType())

def process_and_write_to_bq(spark, kafka_bootstrap_servers, kafka_topic, bq_table, gcp_project_id):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()

    value_df = df.selectExpr("CAST(value AS STRING) as json_str")

    schema = define_schema()
    parsed_df = value_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

    # Chuyển cột `time` về kiểu timestamp
    parsed_df = parsed_df.withColumn("time", to_timestamp(col("time")))

    # Ghi vào BigQuery
    query = parsed_df.writeStream \
        .format("bigquery") \
        .option("table", bq_table) \
        .option("checkpointLocation", "/tmp/aqi_bq_checkpoint") \
        .option("temporaryGcsBucket", "your-temp-gcs-bucket") \
        .outputMode("append") \
        .start()

    query.awaitTermination()

spark = create_spark_session()
process_and_write_to_bq(spark, kafka_bootstrap_servers, kafka_topic, bq_table, gcp_project_id)