import json
import time
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values

BATCH_SIZE = 50
FLUSH_INTERVAL_SEC = 10.0

db = psycopg2.connect(
    host="localhost",
    port=5432,
    database="iot_data",
    user="***",
    password= "***"
)

cursor= db.cursor()

consumer = KafkaConsumer(
    "sensor-data",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    group_id="iot-consumer-group",
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

INSERT_SQL = '''
    INSERT INTO sensor_data
    (device_id,timestamp,temperature,humidity)
    VALUES %s
'''
batch = []
last_flush_time = time.time()

total_message = 0
valid_message = 0
invalid_message = 0
print("Consumer started...")

def validate_sensor_data(data):
    required_field = [
        "device_id",
        "timestamp",
        "temperature",
        "humidity"
    ]

    # check req fields
    if not all(field in data for field in required_field):
        return False
    # validate temp
    if not -50 <= data["temperature"]<=60:
        return False
    # validate humidity
    if not 0 <= data["humidity"] <= 100:
        return False
    return True

def flush_batch():
    global batch,last_flush_time
    if not batch:
        return
    execute_values(cursor,INSERT_SQL,batch)
    db.commit()
    # after postgres succession
    consumer.commit()
    print(f"Inserted {len(batch)} records into PostgreSQL")
    batch.clear()
    last_flush_time = time.time()

try:
    for message in consumer:
        data = message.value
        total_message += 1
        if not validate_sensor_data(data):
            invalid_message += 1
            print(f"Invalid sensor data: {data}")
            continue
        valid_message += 1
        batch.append((
            data["device_id"],
            data["timestamp"],
            data["temperature"],
            data["humidity"]
        ))
        if len(batch) >= BATCH_SIZE:
            flush_batch()
        elif time.time() - last_flush_time >= FLUSH_INTERVAL_SEC:
            flush_batch()
except KeyboardInterrupt:
    print("\nShutting_down...")
    flush_batch()
finally:
    cursor.close()
    db.close()
    consumer.close()