from google.cloud import bigquery
import os
from datetime import datetime, timedelta,  timezone

# Thiết lập credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/buihung/project bigdata/visualize/google-key.json"

# Khởi tạo client
client = bigquery.Client()

# Truy vấn thử dữ liệu
query = """
    SELECT * 
    FROM `iron-envelope-455716-g8.aq_data.streaming_table` 
    LIMIT 1000
"""

# Truy vấn dữ liệu AQI 5 giờ gần nhất từ BigQuery
now = datetime.now(timezone.utc)
five_hours_ago = now - timedelta(hours=5)
QUERY_STREAMING = f"""
        SELECT
            uid AS station_id,
            lat AS latitude,
            lon AS longitude,
            aqi,
            time AS ts
        FROM
            `iron-envelope-455716-g8.aq_data.streaming_table`
        # WHERE
        #     time BETWEEN TIMESTAMP('{five_hours_ago.isoformat()}')
        #                AND TIMESTAMP('{now.isoformat()}')
    """

query_job = client.query(QUERY_STREAMING)  # Gửi truy vấn
rows = query_job.result()        # Đợi kết quả

# In thử vài dòng kết quả
for i, row in enumerate(rows):
    print(row)
    if i >= 4:
        break  # In 5 dòng thôi
