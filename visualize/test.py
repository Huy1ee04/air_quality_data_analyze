from google.cloud import bigquery
import os
from datetime import datetime, timedelta,  timezone

# Thiết lập credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/buihung/project bigdata/visualize/google-key.json"

# Khởi tạo client
client = bigquery.Client()

lat = 16.0
lon = 106.0
# Truy vấn thử dữ liệu
query = """
    SELECT
            fa.hour,
            fa.aqi,
            fa.so2,
            fa.no2,
            fa.co,
            d.date,
            l.lat,
            l.lon
        FROM
            `iron-envelope-455716-g8.aq_data.fact_air_quality` fa
        JOIN
            `iron-envelope-455716-g8.aq_data.dim_date` d
        ON
            fa.date_id = d.date_id
        JOIN
            `iron-envelope-455716-g8.aq_data.dim_location` l
        ON
            fa.location_id = l.location_id
        WHERE
            d.date = '{selected_date.strftime("%Y-%m-%d")}'
            AND l.lat = {lat}
            AND l.lon = {lon}
        ORDER BY
            fa.hour
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

query_job = client.query(query)  # Gửi truy vấn
rows = query_job.result()        # Đợi kết quả

# In thử vài dòng kết quả
for i, row in enumerate(rows):
    print(row)
    if i >= 4:
        break  # In 5 dòng thôi
