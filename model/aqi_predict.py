from google.cloud import bigquery
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.tuning import TrainValidationSplit, ParamGridBuilder

# 1. Khởi tạo Spark session
spark = SparkSession.builder \
    .appName("AQI Forecasting") \
    .getOrCreate()

# 2. Truy vấn dữ liệu từ BigQuery
client = bigquery.Client()
query = """
WITH location_filtered AS (
  SELECT location_id
  FROM `iron-envelope-455716-g8.aq_data.dim_location`
  WHERE lat = 21 AND lon = 105.75
),
date_filtered AS (
  SELECT date_id, EXTRACT(DAYOFWEEK FROM date) AS day_of_week
  FROM `iron-envelope-455716-g8.aq_data.dim_date`
  WHERE EXTRACT(MONTH FROM date) = 4
)
SELECT
  df.day_of_week,
  f.hour,
  f.aqi
FROM `iron-envelope-455716-g8.aq_data.fact_air_quality` f
JOIN location_filtered l ON f.location_id = l.location_id
JOIN date_filtered df ON f.date_id = df.date_id
"""
df_pd = client.query(query).to_dataframe()
df_pd.dropna(inplace=True)

# 3. Chuyển sang Spark DataFrame
df_spark = spark.createDataFrame(df_pd)

# 4. Tạo vector đặc trưng đầu vào
feature_cols = ["day_of_week", "hour"]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# 5. Chia tập train/test
train_data, test_data = df_spark.randomSplit([0.8, 0.2], seed=42)

# 6. Khởi tạo và huấn luyện mô hình
rf = RandomForestRegressor(featuresCol="features", labelCol="aqi", numTrees=100, seed=42)

pipeline = Pipeline(stages=[assembler, rf])
model = pipeline.fit(train_data)

# 7. Dự đoán và đánh giá
predictions = model.transform(test_data)
evaluator = RegressionEvaluator(labelCol="aqi", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)

print(f"✅ RMSE của mô hình: {rmse:.2f}")

# 8. Lưu mô hình
model.save("aqi_model")
print("✅ Mô hình Spark đã được lưu tại 'aqi_model'")
