import os
import time
import json
import random
import kafka.errors
import requests

from kafka import KafkaProducer
from datetime import datetime, timedelta


KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka1:19092,kafka2:19092")
TOPIC = "chicagocrimes"
SEND_DELAY_MS = 200  # 5 records per second

# Chicago Data Portal API
API_ENDPOINT = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
API_TOKEN = os.environ.get("CHICAGO_API_TOKEN", None)


def create_kafka_producer() -> KafkaProducer:
    """Create and connect to a Kafka producer."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k
            )
            print(">> Connected to Kafka.")
            return producer
        except kafka.errors.NoBrokersAvailable as e:
            print(">> No brokers available:", e)
            time.sleep(3)
        except Exception as e:
            print(">> Error connecting to Kafka:", e)
            time.sleep(3)


def fetch_crimes_from_api(limit: int = 1000, offset: int = 0) -> list:
    """Fetch crime data from Chicago Data Portal API."""
    try:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "date DESC",
            "$where": "date >= '2023-01-01T00:00:00' AND date <= '2024-12-31T23:59:59'"
        }
        
        headers = {}
        if API_TOKEN:
            headers["X-App-Token"] = API_TOKEN
        
        print(f">> Fetching {limit} crimes from API (offset: {offset})...")
        
        response = requests.get(API_ENDPOINT, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(f">> Received {len(data)} records from API")
            return data
        else:
            print(f">> API error: {response.status_code}")
            return []
            
    except Exception as e:
        print(f">> Error fetching from API: {e}")
        return []


def normalize_crime_record(api_record: dict) -> dict:
    """Transform API response to match consumer schema with current timestamp."""
    # Use current timestamp for streaming demo
    time_offset_seconds = random.randint(0, 5)
    crime_timestamp = datetime.now() - timedelta(seconds=time_offset_seconds)
    current_time = crime_timestamp.strftime("%Y-%m-%dT%H:%M:%S.000")
    
    return {
        "ID": str(api_record.get("id", "0")),
        "Case Number": str(api_record.get("case_number", "")),
        "Date": current_time,
        "Primary Type": str(api_record.get("primary_type", "UNKNOWN")),
        "Description": str(api_record.get("description", "")),
        "Location Description": str(api_record.get("location_description", "")),
        "Arrest": "true" if api_record.get("arrest") == True else "false",
        "Domestic": "true" if api_record.get("domestic") == True else "false",
        "Latitude": str(api_record.get("latitude", "")),
        "Longitude": str(api_record.get("longitude", ""))
    }


def send_records_to_kafka(producer: KafkaProducer, records: list, send_delay_ms: int = SEND_DELAY_MS) -> None:
    """Send crime records to Kafka one by one."""
    total_sent = 0
    
    for record in records:
        try:
            normalized = normalize_crime_record(record)
            record_id = normalized["ID"]
            
            print(f">> Sending crime to Kafka topic {TOPIC}")
            print(f"   key={record_id}, type={normalized['Primary Type'][:30]}, date={normalized['Date'][:19]}")
            
            producer.send(
                TOPIC,
                key=record_id,
                value=normalized
            )
            
            total_sent += 1
            time.sleep(send_delay_ms / 1000.0)
            
        except Exception as e:
            print(f">> Error processing record: {e}")
            continue
    
    print(f"\n>> Finished sending {total_sent} records")


def main() -> None:
    """Main entrypoint for the producer."""
    
    print(">> Connecting to Kafka...")
    producer = create_kafka_producer()
    
    print(">> Fetching crime data from Chicago API...")
    crimes = fetch_crimes_from_api(limit=1000)
    
    if not crimes:
        print(">> No data available from API. Exiting...")
        return
    
    print(f">> Loaded {len(crimes)} crimes from API")
    print(f">> Starting to send records to Kafka (delay: {SEND_DELAY_MS}ms)...")
    
    send_records_to_kafka(producer, crimes, send_delay_ms=SEND_DELAY_MS)
    
    print(">> Producer finished.")


if __name__ == "__main__":
    main()
