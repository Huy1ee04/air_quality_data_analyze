import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
from shapely.geometry import box
from PIL import Image
import io
import imageio
from matplotlib.colors import LinearSegmentedColormap, Normalize
import plotly.graph_objects as go
import os
from google.cloud import bigquery
import folium
from streamlit_folium import st_folium
from folium.plugins import HeatMap
from datetime import datetime, timedelta, timezone, date
import time as pytime
import joblib
import matplotlib.pyplot as plt

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/buihung/project bigdata/visualize/google-key.json"
client = bigquery.Client()

st.set_page_config(page_title='Air quality data', layout='wide', page_icon=':ambulance:')

# shape = gpd.read_file('world-administrative-boundaries').to_crs("EPSG:4326")
# bbox = box(102, 8, 112, 24)
# cropped_shape = shape.clip(bbox)

page = st.sidebar.radio("Side menu", ["Yearly Analysis", "Monthly Analysis","Daily Analysis", "Streaming Analysis", "Forecasting Analysis"])

client = bigquery.Client()

if page == "Daily Analysis":
    st.title("Daily Air quality Data Analysis")
    selected_date = st.date_input(
        "Chọn ngày",
        value=date(2025, 6, 7),
        min_value=date(2023, 1, 1),
        max_value=date(2025, 12, 31)
    )

    lat_col, lon_col = st.columns([1,1])
    with lat_col:
        lat_slider = st.slider("Select Latitude", 8.0, 23.5, 16.0, step=0.25) 
    with lon_col:
        lon_slider = st.slider("Select Longitude", 102.0, 110.5, 106.0, step=0.25) 

    lat = float(lat_slider)
    lon = float(lon_slider)
    
    # Câu truy vấn
    # QUERY1 = f"""
    #     SELECT
    #         fa.hour,
    #         fa.aqi,
    #         fa.so2,
    #         fa.no2,
    #         fa.co,
    #         d.date,
    #         l.lat,
    #         l.lon
    #     FROM
    #         `iron-envelope-455716-g8.aq_data.fact_air_quality` fa
    #     JOIN (
    #         SELECT DISTINCT date_id, date
    #         FROM `iron-envelope-455716-g8.aq_data.dim_date`
    #     ) d
    #     ON fa.date_id = d.date_id
    #     JOIN (
    #         SELECT DISTINCT location_id, lat, lon
    #         FROM `iron-envelope-455716-g8.aq_data.dim_location`
    #     ) l
    #     ON fa.location_id = l.location_id
    #     WHERE
    #         d.date = '{selected_date.strftime("%Y-%m-%d")}'
    #         AND l.lat = {lat}
    #         AND l.lon = {lon}
    #     ORDER BY
    #         fa.hour
    # """

    QUERY1 = f"""
    SELECT
        fa.hour,
        fa.aqi,
        fa.so2,
        fa.no2,
        fa.co
    FROM
        `iron-envelope-455716-g8.aq_data.fact_air_quality` fa
    WHERE
        fa.date_id = 20250607
        AND fa.location_id = 688953053
    ORDER BY
        fa.hour
"""

    # Thực hiện truy vấn
    query_job = client.query(QUERY1)
    results = query_job.result().to_dataframe()

     # Phân tích thống kê cơ bản cho AQI
    if True:
        st.subheader("Phân tích thống kê AQI")
        aqi_stats = {
            "Trung bình": results['aqi'].mean(),
            "Tối thiểu": results['aqi'].min(),
            "Tối đa": results['aqi'].max(),
            "Độ lệch chuẩn": results['aqi'].std()
        }
        st.write(aqi_stats)
    else:
        st.warning("Không có dữ liệu để phân tích thống kê cho ngày và vị trí đã chọn.")
    
    # Hiển thị kết quả
    st.write("Dữ liệu AQI theo giờ:")
    st.dataframe(results)

    col1, col2 = st.columns([1, 1])
    with col1:
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=results['hour'],
            y=results['aqi'],
            mode='lines+markers',
            name='AQI',
            line=dict(color='royalblue'),
            marker=dict(size=6)
        ))

        fig.update_layout(
            title="Biểu đồ AQI theo giờ",
            xaxis_title="Giờ",
            yaxis_title="Chỉ số AQI",
            template="plotly_white",
        )
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=results['hour'],
            y=results['so2'],
            mode='lines+markers',
            name='SO2',
            line=dict(color='orange'),
            marker=dict(size=6)
        ))

        fig.update_layout(
            title="Biểu đồ SO2 theo giờ",
            xaxis_title="Giờ",
            yaxis_title="Nồng độ SO2",
            template="plotly_white"
        )
        st.plotly_chart(fig, use_container_width=True)

    col3, col4 = st.columns([1, 1])
    with col3:
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=results['hour'],
            y=results['no2'],
            mode='lines+markers',
            name='NO2',
            line=dict(color='green'),
            marker=dict(size=6)
        ))

        fig.update_layout(
            title="Biểu đồ NO2 theo giờ",
            xaxis_title="Giờ",
            yaxis_title="Nồng độ NO2",
            template="plotly_white"
        )
        st.plotly_chart(fig, use_container_width=True)

    with col4:
        fig = go.Figure()

        fig.add_trace(go.Scatter(
            x=results['hour'],
            y=results['co'],
            mode='lines+markers',
            name='CO',
            line=dict(color='red'),
            marker=dict(size=6)
        ))

        fig.update_layout(
            title="Biểu đồ CO theo giờ",
            xaxis_title="Giờ",
            yaxis_title="Nồng độ CO",
            template="plotly_white"
        )
        st.plotly_chart(fig, use_container_width=True)
        
elif page == "Monthly Analysis":
    st.title("Monthly Air quality Data Analysis")

    years = list(range(2023, 2026))

    year,month = st.columns([1, 1])
    with year: 
        selected_year = st.selectbox("Select Year", years, index=years.index(2025))
    with month:
        selected_month = st.selectbox("Select Month", range(1, 13), format_func=lambda x: str(x).zfill(2), index=3)

    lat, lon = st.columns([1, 1])
    with lat:
        lat_slider = st.slider("Select Latitude", 8.0, 23.5, 16.0, step=0.25)
    with lon:
        lon_slider = st.slider("Select Longitude", 102.0, 110.5, 106.0, step=0.25)
    lat = float(lat_slider)
    lon = float(lon_slider)

    QUERY = f"""
                SELECT
                    d.date,
                    da.daily_avg_aqi,
                    da.daily_avg_so2,
                    da.daily_avg_no2,
                    da.daily_avg_co,
                    da.daily_avg_o3,

                FROM
                    `iron-envelope-455716-g8.aq_data.daily_avg` da
                JOIN
                    `iron-envelope-455716-g8.aq_data.dim_date` d
                ON
                    da.date_id = d.date_id
                JOIN
                    `iron-envelope-455716-g8.aq_data.dim_location` l
                ON
                    da.location_id = l.location_id
                WHERE
                    d.year = {selected_year}
                    AND d.month = {selected_month}
                    AND l.lat = {lat}
                    AND l.lon = {lon}
                ORDER BY
                    d.date
            """
    query_job = client.query(QUERY)
    results = query_job.result().to_dataframe()

    # ========= Biểu đồ 1: AQI trung bình ==========
    fig_aqi = go.Figure()
    fig_aqi.add_trace(go.Bar(
        x=results['date'],
        y=results['daily_avg_aqi'],
        marker_color='tomato',
        name="AQI trung bình"
    ))

    fig_aqi.update_layout(
        title=f"AQI trung bình mỗi ngày - Tháng {selected_month}/{selected_year}",
        xaxis_title="Ngày",
        yaxis_title="AQI",
        template='plotly_white'
    )

    st.plotly_chart(fig_aqi)

    # ========= Biểu đồ 2: SO₂, NO₂, Ozone ==========
    fig_gas = go.Figure()

    # SO₂
    fig_gas.add_trace(go.Scatter(
        x=results['date'],
        y=results['daily_avg_so2'],
        mode='lines+markers',
        name='SO₂ trung bình',
        line=dict(color='green', width=2),
        marker=dict(symbol='circle', size=6)
    ))

    # NO₂
    fig_gas.add_trace(go.Scatter(
        x=results['date'],
        y=results['daily_avg_no2'],
        mode='lines+markers',
        name='NO₂ trung bình',
        line=dict(color='blue', width=2),
        marker=dict(symbol='triangle-up', size=6)
    ))

    # O3
    fig_gas.add_trace(go.Scatter(
        x=results['date'],
        y=results['daily_avg_o3'],
        mode='lines+markers',
        name='Ozone trung bình',
        line=dict(color='brown', width=2),
        marker=dict(symbol='triangle-up', size=6)
    ))

    fig_gas.update_layout(
        title=f"Nồng độ các chất trung bình mỗi ngày - Tháng {selected_month}/{selected_year}",
        xaxis_title="Ngày",
        yaxis_title="Nồng độ (µg/m³)",
        template='plotly_white'
    )

    st.plotly_chart(fig_gas)


elif page == "Yearly Analysis":
    st.title("Yearly Air quality Data Analysis")

    years = list(range(2023, 2026)) 

    selected_year = st.selectbox("Select Year", years, index=years.index(2025))
    lat, lon = st.columns([1,1])
    with lat:
        lat_slider = st.slider("Select Latitude", 8.0, 24.0, 16.0, step=0.25) 
    with lon:
        lon_slider = st.slider("Select Longitude", 102.0, 112.0, 106.0, step=0.25) 
    lat = float(lat_slider)
    lon = float(lon_slider)

    QUERY = f"""
            SELECT
                d.month,
                ROUND(AVG(da.daily_avg_aqi), 2) AS avg_aqi,
                ROUND(AVG(da.daily_avg_so2), 2) AS avg_so2,
                ROUND(AVG(da.daily_avg_no2), 2) AS avg_no2
            FROM
                `iron-envelope-455716-g8.aq_data.daily_avg` da
            JOIN
                `iron-envelope-455716-g8.aq_data.dim_date` d
                ON da.date_id = d.date_id
            JOIN
                `iron-envelope-455716-g8.aq_data.dim_location` l
                ON da.location_id = l.location_id
            WHERE
                d.year = {selected_year}
                AND l.lat = {lat}
                AND l.lon = {lon}
            GROUP BY
                d.month
            ORDER BY
                d.month
            """

    query_job = client.query(QUERY)
    results = query_job.result().to_dataframe()
    # -------- Biểu đồ 1: AQI theo tháng --------
    fig_aqi = go.Figure()
    fig_aqi.add_trace(go.Bar(
        x=results['month'],
        y=results['avg_aqi'],
        marker_color='tomato',
        name="AQI trung bình"
    ))

    fig_aqi.update_layout(
        title=f"AQI trung bình theo tháng - Năm {selected_year}",
        xaxis_title="Tháng",
        yaxis_title="AQI",
        template="plotly_white"
    )

    st.plotly_chart(fig_aqi)

    # -------- Biểu đồ 2: SO₂ & NO₂ theo tháng --------
    fig_gas = go.Figure()

    # SO₂
    fig_gas.add_trace(go.Scatter(
        x=results['month'],
        y=results['avg_so2'],
        mode='lines+markers',
        name='SO₂ trung bình',
        line=dict(color='green', width=2),
        marker=dict(symbol='circle', size=6)
    ))

    # NO₂
    fig_gas.add_trace(go.Scatter(
        x=results['month'],
        y=results['avg_no2'],
        mode='lines+markers',
        name='NO₂ trung bình',
        line=dict(color='blue', width=2),
        marker=dict(symbol='triangle-up', size=6)
    ))

    fig_gas.update_layout(
        title=f"SO₂ và NO₂ trung bình theo tháng - Năm {selected_year}",
        xaxis_title="Tháng",
        yaxis_title="Nồng độ (µg/m³)",
        template="plotly_white"
    )

    st.plotly_chart(fig_gas)

if page == "Streaming Analysis":
    st.title("Streaming Air quality Data Analysis")

    # Truy vấn dữ liệu AQI 5 giờ gần nhất từ BigQuery
    now = datetime.now(timezone.utc)
    five_hours_ago = now - timedelta(hours=6)

    QUERY_STREAMING = f"""
        SELECT DISTINCT
            uid AS station_id,
            lat AS latitude,
            lon AS longitude,
            aqi,
            time AS ts,
            station
        FROM
            `iron-envelope-455716-g8.aq_data.streaming_table`
        WHERE
            time BETWEEN TIMESTAMP('{five_hours_ago.isoformat()}')
                       AND TIMESTAMP('{now.isoformat()}')
    """

    query_job = client.query(QUERY_STREAMING)
    rows = query_job.result()

    # Chuyển sang DataFrame
    df = pd.DataFrame([
        {
            "station_id": row["station_id"],
            "lat": row["latitude"],
            "lon": row["longitude"],
            "aqi": row["aqi"],
            "time": row["ts"]
        }
        for row in rows
    ])

    st.write("Columns:", df.columns.tolist())
    st.write(df.head())


    # Làm tròn phút
    df["time"] = pd.to_datetime(df["time"])
    df["time_minute"] = df["time"].dt.floor("min")
    df = df.sort_values("time_minute")

    # Danh sách các mốc thời gian
    time_steps = sorted(df["time_minute"].unique())

    st.subheader("Replay AQI Heatmap (5 giờ gần nhất)")
    st.markdown("⏳ Mỗi khung thời gian được cập nhật mỗi **2 giây**")

    map_placeholder = st.empty()

    # Bắt đầu tua lại
    t = time_steps[0]
    df_t = df[df["time_minute"] == t]

    m = folium.Map(location=[16, 106], zoom_start=6)
    heat_data = [
        [row["lat"], row["lon"], float(row["aqi"])] for _, row in df_t.iterrows()
    ]
    HeatMap(heat_data, radius=12).add_to(m)

    with map_placeholder.container():
        st_folium(m, width=700, height=500)
        st.caption(f"🕒 Dữ liệu hiển thị tại thời điểm: **{t.strftime('%Y-%m-%d %H:%M UTC')}**")

elif page == "Forecasting Analysis":
    st.title("Forecasting Air Quality Data")
    # 🧠 Load mô hình đã huấn luyện
    model = joblib.load("/Users/buihung/project bigdata/model/aqi_model.pkl")

    # 🔢 Tạo dữ liệu input cho ngày 
    day = pd.Timestamp("2025-06-14")
    hours = list(range(24))

    predict_df = pd.DataFrame({
        "day_of_week": [day.dayofweek + 1] * 24,
        "hour": hours
    })

    # Sắp xếp đúng thứ tự cột như khi training (quan trọng!)
    predict_df = predict_df[["day_of_week", "hour"]]

    # 🔮 Dự đoán AQI
    predicted_aqi = model.predict(predict_df)

    # 🎨 Hiển thị biểu đồ dự đoán
    st.subheader("📈 Dự đoán AQI tại HUST theo giờ (ngày 14/06/2025)")
    fig, ax = plt.subplots(figsize=(6, 4))
    ax.plot(hours, predicted_aqi, marker="o", color="blue", label="Dự đoán AQI")
    ax.set_xlabel("Giờ")
    ax.set_ylabel("AQI")
    ax.set_title("Dự đoán AQI theo giờ trong ngày")
    ax.grid(True)
    ax.legend()
    st.pyplot(fig)



    # # Attribute selection
    # selected_attr = st.selectbox("Select Attribute for Heatmap", options=list(attributes.keys()))
    # selected_column = attributes[selected_attr]

    # selected_date = st.date_input("Select Date", value=pd.to_datetime("2024-01-01"))
    # selected_date_str = selected_date.strftime("%Y-%m-%d")

    # colors = ["purple", "blue", "cyan", "green", "yellow", "orange", "red", "white"]
    # custom_cmap = LinearSegmentedColormap.from_list("custom_purple_red", colors)
    # data_dict ={}

    # if 'data_dict' not in st.session_state or st.session_state.selected_date != selected_date:
    #     # Query all attributes for the selected date
    #     for attribute in attributes.values():
    #         QUERY = f'''
    #             SELECT
    #                 {attribute}
    #             FROM
    #                 strong-ward-437213-j6.bigdata_20241.dashboard_main
    #             WHERE
    #                 valid_time >= '{selected_date_str} 00:00:00 UTC'
    #                 AND valid_time <= '{selected_date_str} 23:00:00 UTC'
    #             ORDER BY
    #                 valid_time, latitude DESC, longitude
    #         '''
            
    #         # Execute the query and get results
    #         query_job = client.query(QUERY)
    #         rows = query_job.result()

    #         # Convert results to a numpy array
    #         data = [row[0] for row in rows]

    #         # Process data and create a dictionary of 3D arrays for each attribute
    #         data_dict[attribute] =  np.reshape(data, (24, 65, 41)) 

    #     QUERY_2 = f"""
    #         SELECT 
    #             * 
    #         FROM 
    #             `strong-ward-437213-j6.bigdata_20241.storms` 
    #         WHERE 
    #             time >= '{selected_date_str} 00:00:00 UTC'
    #             AND time <= '{selected_date_str} 23:00:00 UTC'
    #         ORDER BY
    #             time
    #      """
        
    #     # query_job = client.query(QUERY_2)
    #     # rows = query_job.result()

    #     any_storms_detected = False

    #     storm_data = {
    #         "Time": [],
    #         "ID": [],
    #         "Longitude": [],
    #         "Latitude": [],
    #         "Wind Speed": [],
    #         "Amplitude": [],
    #         "Area": [],
    #         }

    #     for row in rows:
    #         any_storms_detected = True
    #         storm_data["Time"].append(row[0])
    #         storm_data["ID"].append(row[1])
    #         storm_data["Longitude"].append(row[2])
    #         storm_data["Latitude"].append(row[3])
    #         storm_data["Wind Speed"].append(row[4])
    #         storm_data["Amplitude"].append(row[5])
    #         storm_data["Area"].append(row[6])
    #     storm_df = pd.DataFrame(storm_data)


    #     # Store data and selected date in session state
    #     st.session_state.data_dict = data_dict
    #     st.session_state.selected_date = selected_date
    #     st.session_state.storm_df = storm_df
    #     st.session_state.any_storm_decteted = any_storms_detected
    # else:
    #     data_dict = st.session_state.data_dict
    #     storm_df = st.session_state.storm_df
    #     any_storms_detected = st.session_state.any_storm_decteted


    # # Use selected attribute data
    # data_array = data_dict[selected_column]

    # # GIF creation (only if new data is loaded)
    # if 'gif_bytes' not in st.session_state or st.session_state.selected_attr != selected_attr:
    #     data_array = data_dict[selected_column]
    #     frames = []
    #     for i in range(24):
    #         fig, ax = plt.subplots(figsize=(12, 10))  
    #         intensity = data_array[i][::-1]
            
    #         # Plot the geographic boundary and the heatmap
    #         cropped_shape.boundary.plot(ax=ax, color='black', linewidth=2)
    #         img = ax.imshow(intensity, cmap=custom_cmap, interpolation='lanczos', extent=[102, 112, 8, 24], origin='lower')
            
    #         ax.set_title(f"{selected_attr} - Frame {i + 1}", fontsize=14)
    #         plt.axis("off")
            
    #         fig.tight_layout()
            
    #         # Save frame to in-memory buffer
    #         buf = io.BytesIO()
    #         fig.savefig(buf, dpi=100)
    #         buf.seek(0)
    #         frames.append(Image.open(buf))
    #         plt.close(fig)

    #     # Save GIF to session state
    #     gif_bytes_io = io.BytesIO()
    #     with imageio.get_writer(gif_bytes_io, format='GIF', duration=0.5, loop=0) as writer:
    #         for frame in frames:
    #             writer.append_data(frame)
    #     st.session_state.gif_bytes = gif_bytes_io.getvalue()
    #     st.session_state.selected_attr = selected_attr