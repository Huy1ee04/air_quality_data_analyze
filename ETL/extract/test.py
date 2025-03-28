import pandas as pd

# Đọc file Parquet
file_path = "air_quality_data.parquet"  # Thay bằng đường dẫn file của bạn
df = pd.read_parquet(file_path, engine="pyarrow")  # Hoặc engine="fastparquet"

# Chuyển đổi sang CSV
csv_path = "air_quality_data.csv"
df.to_csv(csv_path, index=False)

# Hiển thị một số dòng đầu tiên
print(df.head(10))

print(f"File CSV đã được lưu tại: {csv_path}")
