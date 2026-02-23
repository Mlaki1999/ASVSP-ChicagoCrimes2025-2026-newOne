import os
import time
import json
import kafka.errors
import requests
from kafka import KafkaProducer
from datetime import datetime, timedelta

KAFKA_BROKERS = os.environ.get("KAFKA_BROKERS", "kafka1:19092,kafka2:19092")
TOPIC = "chicagocrimes"

# Chicago Data Portal API Configuration (Socrata SODA API)
API_ENDPOINT = "https://data.cityofchicago.org/resource/ijzp-q8t2.json"
API_TOKEN = os.environ.get("CHICAGO_API_TOKEN", None)  # Optional: improves rate limits
REQUEST_LIMIT = 1000  # Records per API request
STREAM_DELAY = 3  # Seconds between sending records

print("="*80)
print(" CHICAGO CRIME DATA STREAM PRODUCER")
print(" Source: Chicago Data Portal API (Socrata SODA)")
print("="*80)

# Connect to Kafka
print("\nConnecting to Kafka brokers...")
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKERS.split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k
        )
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable:
        print("Kafka not available, retrying in 3s...")
        time.sleep(3)

# Function to fetch recent crimes from Chicago Data Portal API
def fetch_recent_crimes(limit=1000, offset=0, use_date_filter=True):
    """
    Fetch crime data from Chicago Data Portal API.
    Fetches from 2023-2024 for sufficient data volume.
    """
    try:
        # API parameters - fetch from 2023-2024 for good data volume
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "date DESC"
        }
        
        # Add date filter for 2023-2024 data (plenty of records)
        if use_date_filter:
            params["$where"] = "date >= '2023-01-01T00:00:00' AND date <= '2024-12-31T23:59:59'"
        
        # Add API token if available (increases rate limit)
        headers = {}
        if API_TOKEN:
            headers["X-App-Token"] = API_TOKEN
        
        print(f"\n→ Fetching crimes from API (offset: {offset}, limit: {limit})...")
        if use_date_filter:
            print(f"   Date range: 2023-2024")
        
        response = requests.get(API_ENDPOINT, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            print(f" Received {len(data)} records from API")
            if len(data) == 0 and use_date_filter:
                print("  No data in date range, trying without filter...")
                return fetch_recent_crimes(limit, offset, use_date_filter=False)
            return data
        else:
            print(f" API error: {response.status_code} - {response.text}")
            return []
            
    except Exception as e:
        print(f" Error fetching from API: {e}")
        return []

# Function to normalize API data to match expected schema
def normalize_crime_record(api_record):
    """
    Transform Socrata API response to match expected schema.
    The consumer expects CSV-style column names with exact casing.
    """
    # Extract location data if it exists
    latitude = ""
    longitude = ""
    if "latitude" in api_record:
        latitude = str(api_record["latitude"])
    if "longitude" in api_record:
        longitude = str(api_record["longitude"])
    
    # Handle arrest and domestic as strings "true"/"false" to match CSV format
    arrest = "true" if api_record.get("arrest") == True or str(api_record.get("arrest", "")).upper() == "TRUE" else "false"
    domestic = "true" if api_record.get("domestic") == True or str(api_record.get("domestic", "")).upper() == "TRUE" else "false"
    
    return {
        "ID": str(api_record.get("id", api_record.get("unique_key", "0"))),
        "Case Number": str(api_record.get("case_number", api_record.get("case_", ""))),
        "Date": str(api_record.get("date", "")),
        "Primary Type": str(api_record.get("primary_type", api_record.get("_primary_decsription", "UNKNOWN"))),
        "Description": str(api_record.get("description", "")),
        "Location Description": str(api_record.get("location_description", "")),
        "Arrest": arrest,
        "Domestic": domestic,
        "Latitude": latitude,
        "Longitude": longitude
    }

# Main streaming loop
print("\n" + "="*80)
print(" STARTING REAL-TIME CRIME DATA STREAM")
print("="*80)

records_sent = 0
offset = 0
continuous_mode = True

while True:
    try:
        # Fetch batch of crimes from API
        crimes = fetch_recent_crimes(limit=REQUEST_LIMIT, offset=offset)
        
        if not crimes:
            print("\n No more records at offset {offset}.")
            if offset == 0:
                print("   No data available from API. Waiting 60s...")
                time.sleep(60)
                continue
            else:
                print("   Reached end of data. Restarting from beginning...")
                offset = 0  # Reset to get latest crimes
                time.sleep(5)  # Short delay before restarting
                continue
        
        # Stream each crime to Kafka
        for crime in crimes:
            try:
                # Normalize the record
                normalized = normalize_crime_record(crime)
                
                # Ensure all values are strings (consumer expects string values)
                for key in normalized:
                    if normalized[key] is None:
                        normalized[key] = ""
                    normalized[key] = str(normalized[key])
                
                # Use ID as Kafka key for partitioning
                key = str(normalized.get("ID", "0"))
                
                # Send to Kafka
                future = producer.send(TOPIC, key=key, value=normalized)
                # Wait for send to complete to catch errors
                future.get(timeout=10)
                
                records_sent += 1
                
                if records_sent <= 3 or records_sent % 50 == 0:
                    print(f"[{records_sent}] Sent: ID={normalized['ID']}, "
                          f"Type={normalized['Primary Type'][:30]}, "
                          f"Date={normalized['Date'][:19]}")
                    if records_sent == 1:
                        # Print first full record for debugging
                        print(f"   DEBUG - Full first record: {normalized}")
                
                # Delay between records to simulate real-time stream
                time.sleep(STREAM_DELAY)
                
            except Exception as e:
                print(f" Error sending record {records_sent + 1}: {e}")
                print(f"   Problematic data: {crime}")
                continue
        
        # Move to next batch
        offset += REQUEST_LIMIT
        
        # Optional: Cycle back to beginning after processing all available records
        if len(crimes) < REQUEST_LIMIT:
            print("\n Reached end of available data. Restarting from latest...")
            offset = 0
            time.sleep(30)  # Wait before restarting cycle
            
    except KeyboardInterrupt:
        print("\n\n Shutdown signal received. Stopping producer...")
        break
    except Exception as e:
        print(f"\n Unexpected error: {e}")
        print("Retrying in 10s...")
        time.sleep(10)

print(f"\n Producer stopped. Total records sent: {records_sent}")
producer.close()
