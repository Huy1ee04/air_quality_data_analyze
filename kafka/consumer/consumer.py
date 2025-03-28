from confluent_kafka import Consumer

# Cấu hình Kafka Consumer
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'number-consumer-group',
    'auto.offset.reset': 'earliest'  # Đọc từ đầu nếu chưa có dữ liệu
}

consumer = Consumer(config)
topic = "my_topic"

# Đăng ký Consumer với topic
consumer.subscribe([topic])

print("Consumer đang chạy...")

# Nhận và in dữ liệu liên tục
try:
    while True:
        msg = consumer.poll(1.0)  # Đợi 1 giây để lấy tin nhắn mới
        
        if msg is None:
            continue
        if msg.error():
            print(f"Error: {msg.error()}")
            continue

        print(f"Received: {msg.value().decode()}")  # In số ra màn hình

except KeyboardInterrupt:
    print("Consumer dừng lại...")
finally:
    consumer.close()
