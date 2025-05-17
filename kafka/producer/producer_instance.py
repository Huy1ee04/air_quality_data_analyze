import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import sys
import argparse
import time
import json
import requests
from time import sleep
from typing import Dict, List, Tuple
from kafka import KafkaAdminClient, KafkaProducer
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import NewTopic
import os
from dotenv import load_dotenv


def fetch_aqi_data(region: List[float]):
    token = os.getenv("AQI_API_TOKEN")
    if not token:
        raise ValueError("API token not found in environment variables")

    lat1, lon1, lat2, lon2 = region
    try:
        url = f"https://api.waqi.info/map/bounds/?latlng={lat1},{lon1},{lat2},{lon2}&token={token}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if data.get("status") != "ok":
            raise ValueError(f"API returned error: {data.get('data')}")

        results = []
        for item in data.get("data", []):
            aqi = item.get("aqi")
            if aqi == "-" or aqi is None:
                continue

            lat = item.get("lat")
            lon = item.get("lon")
            time = item.get("station", {}).get("time")
            station = item.get("station", {}).get("name")

            results.append({
                "uid": item.get("uid"),
                "lat": lat,
                "lon": lon,
                "aqi": aqi,
                "time": time,
                "station": station
            })

        return results

    except Exception as e:
        print(f"Error fetching AQI data: {e}")
        return []
    
class AQIProducer:
    def __init__(self, props: Dict):
        self.producer = KafkaProducer(**props)

    @staticmethod
    def read_records(region: List[float]) -> List[Tuple[str, str]]:
        records = []
        try:
            data = fetch_aqi_data(region)  # Gọi API và lọc theo vùng
            for item in data:
                key = str(item.get("uid"))
                value = json.dumps(item)  # Chuẩn JSON
                records.append((key, value))
        except Exception as e:
            print(f"Error reading records: {e}")
        return records

    def publish(self, topic: str, records: List[Tuple[str, str]], producer_name: str, sleep_time: float = 0.5):
        for key, value in records:
            try:
                future = self.producer.send(
                    topic=topic,
                    key=key.encode("utf-8"),
                    value=value.encode("utf-8")
                )
                result = future.get(timeout=10)
                print(f"Produced record to topic {result.topic} partition [{result.partition}] "
                      f"@ offset {result.offset} from {producer_name}")
            except Exception as e:
                print(f"Error producing record: {e}")
            sleep(sleep_time)

        self.producer.flush()


def producer_instance(topic, kafka_address,broker,producer_name,region):
    load_dotenv()
    BOOTSTRAP_SERVERS = [kafka_address+":"+broker]
    parser = argparse.ArgumentParser()
    parser.add_argument('--time', type=float, default=0.5, help='Time interval between each message')
    args = parser.parse_args(sys.argv[1:])

    try:
        client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)

        existing_topics = client.list_topics()
        if topic not in existing_topics:
            try:
                client.create_topics([NewTopic(name=topic, num_partitions=4, replication_factor=2)])
                print(f"Topic '{topic}' created successfully.")
            except TopicAlreadyExistsError:
                print(f"Topic '{topic}' already exists.")
        else:
            print(f"Topic '{topic}' already exists.")
    except Exception as e:
        print(e)
        pass
            
    except Exception as e:
        print(f"Admin client exception: {e}")
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': None,  
        'value_serializer': None,  
        'acks': 'all',
        'client_id':producer_name
    }
    producer = AQIProducer(props=config)
    
    aqi_records = producer.read_records(region)
    print(f"Producing records to topic: {topic}")
    start_time = time.time()
    print(start_time)
    producer.publish(records=aqi_records, sleep_time=args.time,topic=topic,producer_name=producer_name)
    end_time = time.time()
    print(f"Producing records took: {end_time - start_time} seconds by {producer_name}")