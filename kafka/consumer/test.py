import os
import json
import argparse
from kafka import KafkaConsumer
from dotenv import load_dotenv

def run_consumer(topic: str, kafka_address: str, broker_port: str, group_id: str):
    load_dotenv()

    bootstrap_server = f"{kafka_address}:{broker_port}"

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_server],
        auto_offset_reset='earliest',  # đọc từ đầu
        enable_auto_commit=True,
        group_id=group_id,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    print(f"Listening to topic '{topic}' on {bootstrap_server}...\n")

    try:
        for message in consumer:
            print(f"[Key: {message.key}] {json.dumps(message.value, indent=2)}")
    except KeyboardInterrupt:
        print("Consumer stopped.")

parser = argparse.ArgumentParser()
parser.add_argument("--topic", type=str, required=True, help="Kafka topic name")
parser.add_argument("--kafka_address", type=str, default="localhost", help="Kafka bootstrap server IP")
parser.add_argument("--broker_port", type=str, default="9092", help="Kafka broker port")
parser.add_argument("--group_id", type=str, default="aqi_consumer_group", help="Kafka consumer group id")
args = parser.parse_args()

run_consumer(args.topic, args.kafka_address, args.broker_port, args.group_id)
