# Real-Time IoT Data Streaming Pipeline

An end-to-end real-time data engineering pipeline that simulates IoT sensor data, streams it through Apache Kafka, validates and processes incoming events, and stores the processed data in PostgreSQL.

## Architecture

```text
Python IoT Sensor Simulator
            |
            v
      Kafka Producer
            |
            v
     Kafka Topic
     sensor-data
            |
            v
      Kafka Consumer
            |
            v
    Data Validation
            |
            v
     Batch Processing
            |
            v
       PostgreSQL
```

## Tech Stack

- Python
- Apache Kafka
- Docker
- PostgreSQL
- kafka-python
- psycopg2
- DBeaver

## Project Overview

The project simulates a real-time IoT environment where sensors continuously generate temperature and humidity readings.

The data follows this flow:

1. A Python producer generates simulated sensor readings.
2. The producer serializes the Python dictionary into JSON and sends it to Kafka.
3. Kafka stores the events in the `sensor-data` topic.
4. A Kafka consumer reads the events.
5. Incoming data is validated before processing.
6. Valid records are accumulated into batches.
7. Batches are written to PostgreSQL.
8. PostgreSQL commits the transaction.
9. Kafka consumer offsets are committed after successful database insertion.

## Sensor Data

Each sensor event contains:

```json
{
  "device_id": "sensor_01",
  "timestamp": "2026-10-02T12:30:15",
  "temperature": 27.45,
  "humidity": 65.32
}
```

### Validation Rules

The consumer validates:

- Required fields are present.
- Temperature must be between `-50°C` and `60°C`.
- Humidity must be between `0%` and `100%`.

Invalid records are rejected and counted separately.

## Kafka Configuration

The Kafka topic is:

```text
sensor-data
```

The topic uses 3 partitions.

Partitions allow Kafka to distribute workload among consumers within a consumer group.

The consumer uses:

```text
Group ID: iot-consumer-group
```

Kafka offsets are manually committed after successful PostgreSQL insertion.

This provides an **at-least-once processing approach**.

## Batch Processing

The consumer uses two conditions for flushing data to PostgreSQL:

```text
50 records
     OR
10 seconds elapsed
```

Whichever condition is reached first triggers a database insert.

This provides a balance between:

- Database efficiency through batch inserts
- Low latency for low-volume streams

The batch is cleared after a successful database commit so that new records can be collected.

## PostgreSQL

The processed sensor data is stored in the following table:

```sql
CREATE TABLE sensor_data (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    temperature DECIMAL(5,2),
    humidity DECIMAL(5,2)
);
```

## Project Structure

```text
realtime_iot_streaming/
│
├── producer/
│   └── producer.py
│
├── consumer/
│   └── consumer.py
│
├── sql/
│   └── schema.sql
│
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

## Setup

### 1. Clone the Repository

```bash
git clone <your-repository-url>
cd realtime_iot_streaming
```

### 2. Start Kafka

Make sure Docker Desktop is running.

```bash
docker compose up -d
```

Check that Kafka is running:

```bash
docker ps
```

### 3. Create the Kafka Topic

```bash
docker exec -it iot_kafka /opt/kafka/bin/kafka-topics.sh --create --topic sensor-data --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

Verify the topic:

```bash
docker exec -it iot_kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 4. Configure PostgreSQL

Create a PostgreSQL database named:

```text
iot_data
```

Then execute the SQL from:

```text
sql/schema.sql
```

Configure the PostgreSQL connection settings in `consumer.py`.

> For production use, database credentials should be stored in environment variables rather than directly in source code.

### 5. Install Python Dependencies

Create and activate a Python environment if required.

Then install the dependencies:

```bash
pip install -r requirements.txt
```

## Running the Pipeline

### Start the Producer

```bash
python producer/producer.py
```

The producer continuously generates simulated sensor readings and sends them to Kafka.

### Start the Consumer

Open another terminal and run:

```bash
python consumer/consumer.py
```

The consumer reads Kafka events, validates them, batches valid records, and inserts them into PostgreSQL.

## Monitoring the Data

Verify the stored data in PostgreSQL:

```sql
SELECT *
FROM sensor_data
ORDER BY id DESC;
```

Check the total number of records:

```sql
SELECT COUNT(*)
FROM sensor_data;
```

Calculate basic sensor statistics:

```sql
SELECT
    device_id,
    COUNT(*) AS readings,
    ROUND(AVG(temperature), 2) AS avg_temperature,
    ROUND(AVG(humidity), 2) AS avg_humidity
FROM sensor_data
GROUP BY device_id;
```

## Key Data Engineering Concepts Demonstrated

- Real-time data streaming
- Kafka producers and consumers
- Kafka topics
- Kafka partitions
- Consumer groups
- Kafka offsets
- JSON serialization and deserialization
- Data validation
- Batch processing
- Time-based flushing
- PostgreSQL ingestion
- Manual Kafka offset commits
- At-least-once processing
- Dockerized Kafka environment
- Graceful shutdown and resource cleanup

## Future Improvements

Possible extensions include:

- Move database credentials to environment variables
- Add more sensor devices and realistic sensor behavior
- Add additional data quality checks
- Build a Power BI dashboard for sensor analytics

## Author

**Anis Shah**
