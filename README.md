
# Chicago Crimes Analytics - Big Data Processing System

## Project Overview

This project implements a big data analytics system for analyzing crime patterns in Chicago using both batch processing and real-time stream processing. The system provides insights for law enforcement resource allocation and crime pattern detection.

**Domain**: Crime prevention and public safety analytics  
**Objective**: Analyze historical crime trends and detect real-time crime patterns for strategic and tactical decision-making

**Stakeholders**:
- Police Chief - Strategic planning and policy decisions
- District Commander - Real-time tactical operations and patrol deployment

---

## Datasets

### Primary Dataset: Chicago Crimes Historical Data (Batch Processing)
- **Source**: [City of Chicago Data Portal](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2)
- **Access Method**: Downloaded CSV file
- **Size**: >300 MB
- **Purpose**: Historical trend analysis
- **Storage**: HDFS Data Lake

### Secondary Dataset: Chicago Crimes Stream (Stream Processing)
- **Source**: [Chicago Data Portal API](https://data.cityofchicago.org/resource/ijzp-q8t2.json)
- **Access Method**: Live REST API (Socrata SODA)
- **Purpose**: Real-time stream analytics
- **Storage**: Kafka → MongoDB

**Note**: The two datasets use different access methods (file download vs. API) representing different data source types with different infrastructure and processing paradigms.

---

## Technology Stack

**Storage**: Hadoop HDFS, PostgreSQL, MongoDB  
**Processing**: Apache Spark 3.0+, Kafka, Zookeeper  
**Visualization**: Metabase, Mongo Express, Kafka UI  
**Orchestration**: Docker & Docker Compose

---

### 1. Prepare Data

Download data from the following source and place it in the `data/` folder:
- Chicago Crimes CSV [link](https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-Present/ijzp-q8t2), rename to `chicago_crimes.csv`
- Download the PostgreSQL JDBC driver [link](https://jdbc.postgresql.org/download/) and save as `data/postgresql-42.7.0.jar`

Ensure the file is >300 MB (filter for multiple years, e.g., 2020-2024).

**Note**: For stream processing, no manual download is needed - the producer fetches data from the API automatically.

```
data/
├── chicago_crimes.csv
└── postgresql-42.7.0.jar
```

---

### 2. Architecture Setup

From the root folder, make scripts executable:

```bash
chmod +x *.sh
```

If there is an error on initial setup (Windows line endings), run:
```bash
dos2unix *.sh
```

**Option 1: Run batch processing only**
```bash
./run_pipeline.sh
```

**Option 2: Run complete streaming setup**
```bash
./fix_streaming.sh
```

**Option 3: Manual setup**
```bash
# Start batch cluster
cd batch_processing
docker-compose up -d
cd ..

# Wait 60 seconds for services to initialize

# Start streaming cluster
cd stream-processing
docker-compose up -d
cd ..
```

If you want to shut down the cluster:
```bash
cd batch_processing
docker-compose down
cd ../stream-processing
docker-compose down
```

**Architecture diagram**:

```
DATA SOURCES
│
├─ Historical CSV (chicago_crimes.csv) ──> HDFS Raw Zone
│                                           │
│                                           ▼
│                                          Spark Batch Processing
│                                           │
│                                           ▼
│                                          PostgreSQL (Curated Zone)
│                                           │
│                                           ▼
│                                          Metabase Dashboards
│
└─ Live API Stream ──> Kafka ──> Spark Streaming ──> MongoDB ──> Mongo Express
```

**Containers**:
- Batch: namenode, datanode1, datanode2, spark-master, spark-worker1, spark-worker2, postgresql, metabase-app
- Stream: zoo1, kafka1, kafka2, kafka_producer, stream_consumer, mongodb, mongo-express, kafka-ui

---

### 3. Data Upload Verification

Verify that data has been successfully uploaded to HDFS:
- Navigate to http://localhost:9870/explorer.html#/user/root/data-lake
- Check for `raw/batch_data.csv` and `transform/chicago_crimes.csv`

---

### 4. Batch Processing

The batch processing pipeline consists of two scripts:

**Preprocessing** (`batch_processing/spark/preprocessing.py`):
- Loads raw CSV from HDFS
- Cleans and transforms data (type casting, null handling)
- Adds derived features (day_of_week, month, year, season)
- Saves to transformation zone

**Processing** (`batch_processing/spark/processing.py`):
- Loads preprocessed data
- Executes 10 analytical queries
- Uses window functions (row_number, rank, partitionBy)
- Saves results to PostgreSQL

To verify Spark jobs are running:
- Spark Master UI: http://localhost:8080
- Active job details: http://localhost:4040

---

### 5. Batch Processing Visualization

Once processing is complete:

1. Open Metabase: http://localhost:3000
2. First time setup: Create admin account
3. Add database connection:
   - Database type: PostgreSQL
   - Host: `postgresql`
   - Port: `5432`
   - Database: `big_data`
   - Username: `postgres`
   - Password: `postgres`
4. Click "Sync database schema"
5. Browse to http://localhost:3000/browse/databases
6. Select `big_data` and explore the 10 result tables

---

### 6. Stream Processing

The streaming pipeline has two components:

**Producer** (`stream-processing/producer/producer.py`):
- Fetches data from Chicago Data Portal API
- Publishes records to Kafka topic "chicagocrimes"
- Sends 5 records per second

**Consumer** (`stream-processing/consumer/consumer.py`):
- Reads from Kafka topic in real-time
- Implements 5 complex transformations using Spark Structured Streaming
- Stores results in MongoDB

To verify streaming ingestion:
- Kafka UI: http://localhost:8091
- Check topic "chicagocrimes" → View Messages

---

### 7. Stream Processing Visualization

Stream results are stored in MongoDB and visible in real-time.

1. Ensure MongoDB is running: http://localhost:27018
2. Open Mongo Express: http://localhost:8083
   - Username: `admin`
   - Password: `admin123`
3. Select database: `chicago_crimes`
4. Browse collections:
   - `stream_crime_hotspots`
   - `stream_pattern_analysis`
   - `stream_violence_escalation`
   - `stream_domestic_correlation`
   - `stream_temporal_patterns`

---

### 8. Queries List

**Batch Processing (10 queries)**:

1. *Which days of the week have the highest crime rates?*  
   Analysis: Aggregation by day of week

2. *What is the most common crime type each year?*  
   Analysis: Window function with row_number() partitioned by year

3. *Which locations have the highest crime rates?*  
   Analysis: Aggregation by location description

4. *Is there a correlation between crime types and arrest rates?*  
   Analysis: Grouped aggregation with arrest flag

5. *How has the trend of violent crimes changed over the years?*  
   Analysis: Filtered aggregation for violent crime categories (HOMICIDE, ASSAULT, BATTERY, ROBBERY)

6. *Which locations are most dangerous for specific crime types?*  
   Analysis: Multi-dimensional grouping by location and crime type

7. *What are the monthly crime patterns throughout the year?*  
   Analysis: Temporal aggregation by month

8. *Are domestic incidents associated with higher arrest rates?*  
   Analysis: Filtered aggregation comparing domestic vs non-domestic incidents

9. *What are the most common crime types by season?*  
   Analysis: Window function with seasonal partitioning (Winter, Spring, Summer, Fall)

10. *How is crime geographically distributed across the city over time?*  
    Analysis: Geo-temporal aggregation with latitude/longitude coordinates

**Stream Processing (5 queries)**:

1. *Where are crime hotspots emerging in real-time?*  
   Analysis: Sliding window aggregation (5-min windows) with geographic clustering and severity classification

2. *Do incoming incidents match historical patterns or are they anomalies?*  
   Analysis: Stream-to-batch join between live events and historical patterns with anomaly scoring

3. *Are violent crimes escalating in specific areas?*  
   Analysis: Windowed aggregation (10-min) on violent crime categories with escalation risk scoring

4. *What is the relationship between domestic and non-domestic crimes in the same area?*  
   Analysis: Stream-to-stream self-join with temporal and geographic conditions

5. *What temporal patterns emerge from real-time crime data?*  
   Analysis: Complex sliding window (15-min) with crime intensity classification and geographic dispersion metrics

---

## Verification Commands

**Check HDFS data**:
```bash
docker exec namenode hdfs dfs -ls /user/root/data-lake/raw
docker exec namenode hdfs dfs -ls /user/root/data-lake/transform
```

**Query PostgreSQL results**:
```bash
docker exec -it postgresql psql -U postgres -d big_data -c "\dt"
```

**Check Kafka topics**:
```bash
docker exec kafka1 kafka-topics --list --bootstrap-server kafka1:19092
```

**View consumer logs**:
```bash
docker logs stream_consumer -f
```

**Check MongoDB collections**:
```bash
docker exec -it mongodb mongosh -u root -p mongodb123 --authenticationDatabase admin
> use chicago_crimes
> show collections
> db.stream_crime_hotspots.find().limit(5)
```

---

## Troubleshooting

**HDFS in safe mode**:
```bash
docker exec namenode hdfs dfsadmin -safemode leave
```

**Kafka consumer not receiving messages**:
```bash
./fix_streaming.sh
```

**Out of memory errors**:
```bash
# Increase Docker memory to 8GB+
# Docker Desktop → Settings → Resources → Memory
```

---

## Project Structure

```
ASVSP-ChicagoCrimes2025-2026-newOne/
├── README.md
├── run_pipeline.sh              # Batch processing automation
├── fix_streaming.sh             # Complete streaming setup
├── rebuild.sh                   # Rebuild containers
├── data/
│   ├── chicago_crimes.csv       # Primary dataset
│   └── postgresql-42.7.0.jar    # JDBC driver
├── batch_processing/
│   ├── docker-compose.yml
│   └── spark/
│       ├── preprocessing.py     # Data cleaning
│       └── processing.py        # 10 queries
└── stream-processing/
    ├── docker-compose.yml
    ├── producer/
    │   ├── producer.py          # API data fetch
    │   └── Dockerfile
    └── consumer/
        ├── consumer.py          # 5 transformations
        └── Dockerfile
```

---

**Contributors**: Marko Pavlovic  
**Last Updated**: February 26, 2026  
**Course**: Advanced Systems for Large-Scale Data Processing---

## Web Interfaces

- Hadoop NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- Spark Job Details: http://localhost:4040
- Metabase: http://localhost:3000
- Mongo Express: http://localhost:8083 (admin / admin123)
- Kafka UI: http://localhost:8091

## Database Connections

**PostgreSQL**:
```
Host: localhost (or postgresql from containers)
Port: 5432
Database: big_data
Username: postgres
Password: postgres
```

**MongoDB**:
```
Host: localhost (or mongodb from containers)
Port: 27018
Database: chicago_crimes
Username: root
Password: mongodb123
```

---
