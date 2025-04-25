import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
import time
from dotenv import load_dotenv
from producer_instance import producer_instance
import datetime
import threading
load_dotenv()

AQI_TOPIC = os.getenv('AQI_TOPIC')
kafka_address = os.getenv('KAFKA_ADDRESS')
broker1= os.getenv('BROKER1')
broker2 = os.getenv('BROKER2')

def producer_1():
    while True:
        try:
            print("Running producer 1...")
            producer_instance(
                topic=AQI_TOPIC, 
                kafka_address=kafka_address, 
                broker=broker1, 
                producer_name="producer1", 
                region=[8, 102, 16, 110]
            )
        except Exception as e:
            print(f"Error in producer1: {e}")
            return
        print("Producer 1 waiting for 1 hour before the next run...")
        time.sleep(3600)

def producer_2():
    while True:
        try:
            print("Running producer 2...")
            producer_instance(
                topic=AQI_TOPIC, 
                kafka_address=kafka_address, 
                broker=broker2, 
                producer_name="producer2", 
                region=[8, 102, 16, 110]
            )
        except Exception as e:
            print(f"Error in producer2: {e}")
            return
        print("Producer 2 waiting for 1 hour before the next run...")
        time.sleep(3600)

thread_1 = threading.Thread(target=producer_1)
thread_2 = threading.Thread(target=producer_2)

thread_1.start()
thread_2.start()
thread_1.join()
thread_2.join()