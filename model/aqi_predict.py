from google.cloud import bigquery
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import joblib

# 1. Khởi tạo client BigQuery (sẽ dùng credentials mặc định từ GOOGLE_APPLICATION_CREDENTIALS)
client = bigquery.Client()

# 2. Truy vấn dữ liệu
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

df = client.query(query).to_dataframe()

# 3. Tiền xử lý
df = df.dropna()
X = df[['day_of_week', 'hour']]
y = df['aqi']

# 4. Train/test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# 5. Huấn luyện mô hình
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# 6. Đánh giá đơn giản
preds = model.predict(X_test)

# 7. Lưu mô hình
joblib.dump(model, 'aqi_model.pkl')
print("✅ Mô hình đã lưu thành 'aqi_model.pkl'")
