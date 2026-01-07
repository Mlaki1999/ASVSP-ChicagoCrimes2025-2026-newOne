import os
import time
import json
import kafka.errors
from kafka import KafkaProducer
from hdfs import InsecureClient

HDFS_NAMENODE = os.environ["HDFS_NAMENODE"]
KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka1:19092,kafka2:19092")
TOPIC = "chicagocrimes"

# Povezivanje na Kafka
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")  # sve u JSON
        )
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable:
        print("Kafka not available, retrying...")
        time.sleep(3)

# Povezivanje na HDFS
client = InsecureClient(HDFS_NAMENODE)
file_path = "/user/root/data-lake/raw/batch_data.csv"

# chicago_crimes_head10

while True:
    try:
        client.status(file_path)
        break
    except Exception:
        print("Invalid location, retrying...")
        time.sleep(3)

print(f"Reading file: {file_path}")
with client.read(file_path, encoding="utf-8", delimiter="\n") as reader:
    print("File successfully opened.")
    first_line = next(reader)  # header
    column_names = first_line.strip().split(",")

    for line in reader:
        try:
            data = line.strip().split(",")
            row = dict(zip(column_names, data))

            print(f"Sending record to Kafka topic {TOPIC}: {row}")
            producer.send(TOPIC, key=str(row.get("ID", "0")).encode("utf-8"), value=row)
            time.sleep(5)

        except Exception as e:
            print("Exception while sending:", e)
