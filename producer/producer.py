from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
print("IoT sensor started...")

while True:
    sensor_data = {
        "device_id": "sensor_01",
        "timestamp": datetime.now().isoformat(),
        "temperature": round(random.uniform(20,35), 2),
        "humidity": round(random.uniform(40,80), 2)
    }

    producer.send("sensor-data",value=sensor_data)
    print(sensor_data)
    time.sleep(2)