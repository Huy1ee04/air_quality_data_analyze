from confluent_kafka import Producer
import time

# Cấu hình Kafka Producer
config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'number-producer',
}

producer = Producer(config)
topic = "my_topic"

# Hàm callback kiểm tra gửi tin thành công
def delivery_report(err, msg):
    if err:
        print(f"Failed: {err}")
    else:
        print(f"Sent: {msg.value().decode()}")

# Phát số liên tục
counter = 1
while True:
    message = str(counter)  # Chuyển số thành chuỗi
    producer.produce(topic, key=str(counter), value=message, callback=delivery_report)
    producer.flush()
    
    counter += 1
    time.sleep(1)  # Chờ 1 giây trước khi gửi số tiếp theo
