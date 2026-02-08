import json
import time
import uuid
import shutil
import os
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pymongo import MongoClient

TOPIC = "chicagocrimes"

# Generate unique session ID to prevent conflicts
SESSION_ID = str(uuid.uuid4())[:8]
print(f"🔧 Starting streaming session: {SESSION_ID}")

# Cleanup function for checkpoints
def cleanup_checkpoints():
    """Clean up old checkpoint directories"""
    checkpoint_dirs = [
        "/tmp/checkpoint",
        "/tmp/checkpoint_console", 
        "/tmp/checkpoint_hotspots",
        "/tmp/checkpoint_violence",
        "/tmp/checkpoint_domestic", 
        "/tmp/checkpoint_patterns",
        "/tmp/checkpoint_temporal"
    ]
    
    for dir_path in checkpoint_dirs:
        try:
            if os.path.exists(dir_path):
                shutil.rmtree(dir_path)
                print(f"🧹 Cleaned checkpoint: {dir_path}")
        except Exception as e:
            print(f"⚠️ Could not clean {dir_path}: {e}")

# Clean up before starting
print("🧹 Cleaning up old checkpoints...")
cleanup_checkpoints()
time.sleep(2)

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName(f"ChicagoCrimesStreaming_{SESSION_ID}") \
    .config("spark.sql.streaming.checkpointLocation", f"/tmp/checkpoint_{SESSION_ID}") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

quiet_logs(spark)

# MongoDB Configuration
MONGODB_HOST = "mongodb"  
MONGODB_PORT = 27017
MONGODB_USERNAME = "root"
MONGODB_PASSWORD = "mongodb123"
MONGODB_DATABASE = "chicago_crimes"
MONGODB_URI = f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}/{MONGODB_DATABASE}?authSource=admin"

# Initialize MongoDB client
mongo_client = None
while not mongo_client:
    try:
        mongo_client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        # Test connection
        mongo_client.server_info()
        print(" Connected to MongoDB!")
        break
    except Exception as e:
        print(f" Connecting to MongoDB: {e}... retrying in 3s")
        time.sleep(3)

db = mongo_client[MONGODB_DATABASE]

# Helper function to write to MongoDB
def write_to_mongodb_batch(df, collection_name):
    """Write DataFrame to MongoDB collection"""
    try:
        if df.count() > 0:
            # Convert DataFrame to list of dictionaries
            records = df.toPandas().to_dict('records')
            
            # Convert timestamps and handle NaN values
            for record in records:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, pd.Timestamp):
                        record[key] = value.to_pydatetime()
            
            # Add metadata
            for record in records:
                record['_inserted_at'] = datetime.now()
                record['_session_id'] = SESSION_ID
            
            # Insert into MongoDB
            collection = db[collection_name]
            result = collection.insert_many(records)
            print(f"✓ Successfully saved {len(result.inserted_ids)} records to {collection_name}")
    except Exception as e:
        print(f"✗ Error saving to {collection_name}: {str(e)}")

def write_to_mongodb_streaming(df, collection_name, checkpoint_location):
    """Write streaming DataFrame to MongoDB using foreachBatch"""
    def write_batch(batch_df, batch_id):
        print(f"📝 Writing batch {batch_id} to {collection_name} ({batch_df.count()} records)")
        if batch_df.count() > 0:
            write_to_mongodb_batch(batch_df, collection_name)
    
    return df.writeStream \
        .foreachBatch(write_batch) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime='30 seconds') \
        .start()

# -----------------------------
# UDF za parsiranje JSON-a
def parse_json_safe(s):
    try:
        return json.loads(s)
    except:
        return {}  # prazna mapa za loše JSON-e

parse_udf = udf(lambda s: parse_json_safe(s), MapType(StringType(), StringType()))

# -----------------------------
# Čitanje iz Kafka sa timestamp informacijama .option("kafka.enable.auto.commit", "true") \
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("startingOffsets", "earliest") \
    .option("includeHeaders", "true") \
    .option("kafka.request.timeout.ms", "120000") \
    .option("kafka.session.timeout.ms", "60000") \
    .option("kafka.max.poll.interval.ms", "300000") \
    .option("kafka.fetch.max.wait.ms", "10000") \
    .option("kafka.connections.max.idle.ms", "540000") \
    .option("kafka.metadata.max.age.ms", "30000") \
    .load()

# Parsiranje JSON sa timestamp informacijama
df_parsed = df_stream.selectExpr("CAST(value AS STRING)", "timestamp") \
    .withColumn("parsed", parse_udf(col("value"))) \
    .filter(col("parsed")["ID"].isNotNull()) \
    .withColumn("event_time", col("timestamp")) \
    .withWatermark("event_time", "10 seconds")

# Ekstrakt polja iz mape sa dodatnim tipovima podataka
def extract(colname):
    return col("parsed")[colname]

df_crimes_enriched = df_parsed.select(
    extract("ID").cast(IntegerType()).alias("crime_id"),
    extract("Case Number").alias("case_number"),
    to_timestamp(extract("Date"), "MM/dd/yyyy hh:mm:ss a").alias("crime_date"),
    extract("Primary Type").alias("primary_type"),
    extract("Description").alias("description"),
    extract("Location Description").alias("location_description"),
    extract("Arrest").cast(BooleanType()).alias("arrest"),
    extract("Domestic").cast(BooleanType()).alias("domestic"),
    extract("Latitude").cast(DoubleType()).alias("latitude"),
    extract("Longitude").cast(DoubleType()).alias("longitude"),
    col("event_time")
).filter(col("crime_id").isNotNull() & col("crime_date").isNotNull())

# Dodaj dodatne kolone za analizu
df_crimes_enriched = df_crimes_enriched \
    .withColumn("hour_of_day", hour("crime_date")) \
    .withColumn("day_of_week", date_format("crime_date", "EEEE")) \
    .withColumn("month", month("crime_date")) \
    .withColumn("year", year("crime_date")) \
    .withColumn("is_violent", when(col("primary_type").isin("HOMICIDE", "ASSAULT", "BATTERY", "ROBBERY"), True).otherwise(False)) \
    .withColumn("is_weekend", when(col("day_of_week").isin("Saturday", "Sunday"), True).otherwise(False))

print("Starting Advanced Stream Processing with 5 Complex Transformations...")
print("=" * 80)

# ===============================================================================================================
# TRANSFORMATION 1: REAL-TIME CRIME HOTSPOT DETECTION WITH WINDOWING
# ===============================================================================================================

print("\n TRANSFORMATION 1: Real-time Crime Hotspot Detection")
crime_hotspots = df_crimes_enriched \
    .filter(col("latitude").isNotNull() & col("longitude").isNotNull()) \
    .withColumn("geo_grid", concat(
        round(col("latitude"), 2).cast(StringType()), 
        lit("_"), 
        round(col("longitude"), 2).cast(StringType())
    )) \
    .groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("geo_grid")
    ) \
    .agg(
        count("*").alias("crime_count"),
        approx_count_distinct("primary_type").alias("crime_variety"),
        avg("latitude").alias("avg_lat"),
        avg("longitude").alias("avg_lon"),
        collect_list("primary_type").alias("crime_types")
    ) \
    .filter(col("crime_count") >= 2) \
    .withColumn("hotspot_severity", 
        when(col("crime_count") >= 5, "HIGH")
        .when(col("crime_count") >= 3, "MEDIUM")
        .otherwise("LOW")
    ) \
    .withColumn("analysis_timestamp", current_timestamp()) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "geo_grid", "avg_lat", "avg_lon", "crime_count", 
        "crime_variety", "hotspot_severity", "crime_types", "analysis_timestamp"
    )

# ===============================================================================================================
# TRANSFORMATION 2: STREAM-TO-BATCH JOIN - HISTORICAL CRIME PATTERN MATCHING
# ===============================================================================================================

print("\n TRANSFORMATION 2: Stream-to-Batch Join - Historical Pattern Matching")

# Create batch reference data (this would typically come from your batch processing results)
batch_crime_patterns = spark.sql("""
    SELECT 
        primary_type,
        location_description,
        hour(timestamp) as hour_pattern,
        dayofweek(timestamp) as dow_pattern,
        avg(latitude) as typical_lat,
        avg(longitude) as typical_lon,
        count(*) as historical_frequency
    FROM (
        SELECT 
            'THEFT' as primary_type,
            'STREET' as location_description, 
            current_timestamp() as timestamp,
            41.8781 as latitude,
            -87.6298 as longitude
        UNION ALL
        SELECT 
            'BATTERY' as primary_type,
            'RESIDENCE' as location_description,
            current_timestamp() as timestamp,
            41.8818 as latitude,
            -87.6231 as longitude
    ) dummy_data
    GROUP BY primary_type, location_description, hour_pattern, dow_pattern
""")

# Stream-to-batch join
stream_pattern_analysis = df_crimes_enriched \
    .withColumn("current_hour", hour("event_time")) \
    .withColumn("current_dow", dayofweek("event_time")) \
    .alias("stream") \
    .join(
        broadcast(batch_crime_patterns.alias("batch")),
        (col("stream.primary_type") == col("batch.primary_type")) &
        (col("stream.current_hour") == col("batch.hour_pattern")) &
        (col("stream.current_dow") == col("batch.dow_pattern")),
        "left_outer"
    ) \
    .withColumn("pattern_match", 
        when(col("batch.historical_frequency").isNotNull(), True).otherwise(False)
    ) \
    .withColumn("anomaly_score",
        when(col("pattern_match") == False, 1.0).otherwise(0.0)
    ) \
    .select(
        col("stream.crime_id"), col("stream.crime_date"), col("stream.primary_type"), 
        col("stream.location_description"), "pattern_match", "anomaly_score", 
        col("batch.historical_frequency"), col("stream.event_time")
    )

# ===============================================================================================================
# TRANSFORMATION 3: COMPLEX WINDOWED AGGREGATION - VIOLENCE ESCALATION DETECTION
# ===============================================================================================================

print("\n TRANSFORMATION 3: Violence Escalation Detection with Complex Windowing")

violence_escalation = df_crimes_enriched \
    .filter(col("is_violent") == True) \
    .withColumn("geo_zone", concat(
        round(col("latitude"), 1).cast(StringType()),
        lit("_"),
        round(col("longitude"), 1).cast(StringType())
    )) \
    .groupBy(
        window(col("event_time"), "10 minutes", "2 minutes"),
        col("geo_zone")
    ) \
    .agg(
        count("*").alias("violent_incidents"),
        approx_count_distinct("primary_type").alias("violence_types"),
        sum(when(col("primary_type") == "HOMICIDE", 1).otherwise(0)).alias("homicides"),
        sum(when(col("primary_type") == "ASSAULT", 1).otherwise(0)).alias("assaults"),
        sum(when(col("arrest") == True, 1).otherwise(0)).alias("arrests_made"),
        avg("latitude").alias("zone_lat"),
        avg("longitude").alias("zone_lon")
    ) \
    .withColumn("escalation_risk",
        when(col("violent_incidents") >= 4, "CRITICAL")
        .when(col("violent_incidents") >= 2, "HIGH")
        .otherwise("NORMAL")
    ) \
    .withColumn("arrest_effectiveness", 
        round((col("arrests_made") / col("violent_incidents")) * 100, 2)
    ) \
    .filter(col("violent_incidents") >= 1) \
    .select(
        col("window.start").alias("period_start"),
        col("window.end").alias("period_end"),
        "geo_zone", "zone_lat", "zone_lon", 
        "violent_incidents", "violence_types", "homicides", "assaults",
        "arrests_made", "arrest_effectiveness", "escalation_risk"
    )

# ===============================================================================================================
# TRANSFORMATION 4: STREAM-TO-STREAM JOIN - DOMESTIC VIOLENCE CORRELATION ANALYSIS
# ===============================================================================================================

print("\n TRANSFORMATION 4: Stream-to-Stream Join - Domestic Violence Correlation")

# Create two streams from the same source for self-join
domestic_crimes = df_crimes_enriched \
    .filter(col("domestic") == True) \
    .select(
        col("crime_id").alias("d_crime_id"),
        col("crime_date").alias("d_crime_date"),
        col("primary_type").alias("d_primary_type"),
        col("location_description").alias("d_location"),
        col("latitude").alias("d_latitude"),
        col("longitude").alias("d_longitude"),
        col("event_time").alias("d_event_time")
    )

non_domestic_crimes = df_crimes_enriched \
    .filter(col("domestic") == False) \
    .select(
        col("crime_id").alias("nd_crime_id"),
        col("crime_date").alias("nd_crime_date"),
        col("primary_type").alias("nd_primary_type"),
        col("location_description").alias("nd_location"),
        col("latitude").alias("nd_latitude"),
        col("longitude").alias("nd_longitude"),
        col("event_time").alias("nd_event_time")
    )

# FIXED: Stream-to-stream join with proper equality predicate and watermarks
# Create geographic grid zones for equality-based joining
domestic_crimes_with_zone = domestic_crimes \
    .withColumn("d_geo_grid", concat(
        round(col("d_latitude"), 2).cast(StringType()),
        lit("_"),
        round(col("d_longitude"), 2).cast(StringType())
    )) \
    .withWatermark("d_event_time", "10 seconds")

non_domestic_crimes_with_zone = non_domestic_crimes \
    .withColumn("nd_geo_grid", concat(
        round(col("nd_latitude"), 2).cast(StringType()),
        lit("_"),
        round(col("nd_longitude"), 2).cast(StringType())
    )) \
    .withWatermark("nd_event_time", "10 seconds")

# Stream-to-stream join using equality predicate on geographic grid and time window
domestic_correlation = domestic_crimes_with_zone \
    .join(
        non_domestic_crimes_with_zone,
        (col("d_geo_grid") == col("nd_geo_grid")) &
        (col("d_event_time") >= col("nd_event_time")) &
        (col("d_event_time") <= col("nd_event_time") + expr("INTERVAL 30 minutes")),
        "inner"
    ) \
    .withColumn("time_diff_minutes", 
        (unix_timestamp("d_event_time") - unix_timestamp("nd_event_time")) / 60
    ) \
    .withColumn("geographic_distance",
        sqrt(pow(col("d_latitude") - col("nd_latitude"), 2) + 
             pow(col("d_longitude") - col("nd_longitude"), 2)) * 111000
    ) \
    .select(
        "d_crime_id", "nd_crime_id", "d_primary_type", "nd_primary_type",
        "d_location", "nd_location", "time_diff_minutes", "geographic_distance",
        col("d_latitude").alias("correlation_lat"),
        col("d_longitude").alias("correlation_lon"), 
        col("d_geo_grid").alias("geo_grid")
    )

# ===============================================================================================================
# TRANSFORMATION 5: ADVANCED TEMPORAL PATTERN ANALYSIS WITH SLIDING WINDOWS
# ===============================================================================================================

print("\n⏰ TRANSFORMATION 5: Advanced Temporal Pattern Analysis")

temporal_patterns = df_crimes_enriched \
    .groupBy(
        window(col("event_time"), "15 minutes", "5 minutes"),
        col("primary_type"),
        col("hour_of_day"),
        col("is_weekend")
    ) \
    .agg(
        count("*").alias("incident_count"),
        avg(when(col("arrest") == True, 1).otherwise(0)).alias("arrest_rate"),
        approx_count_distinct("location_description").alias("location_diversity"),
        stddev("latitude").alias("lat_spread"),
        stddev("longitude").alias("lon_spread")
    ) \
    .withColumn("geographic_dispersion", 
        when(col("lat_spread").isNull() | col("lon_spread").isNull(), 0.0)
        .otherwise(sqrt(pow(col("lat_spread"), 2) + pow(col("lon_spread"), 2)))
    ) \
    .withColumn("crime_intensity",
        when(col("incident_count") >= 5, "VERY_HIGH")
        .when(col("incident_count") >= 3, "HIGH")
        .when(col("incident_count") >= 2, "MEDIUM")
        .otherwise("LOW")
    ) \
    .withColumn("time_category",
        when(col("hour_of_day").between(6, 12), "MORNING")
        .when(col("hour_of_day").between(12, 18), "AFTERNOON")  
        .when(col("hour_of_day").between(18, 24), "EVENING")
        .otherwise("NIGHT")
    ) \
    .filter(col("incident_count") >= 1) \
    .select(
        col("window.start").alias("analysis_start"),
        col("window.end").alias("analysis_end"),
        "primary_type", "hour_of_day", "time_category", "is_weekend",
        "incident_count", "arrest_rate", "location_diversity",
        "geographic_dispersion", "crime_intensity"
    )

# ===============================================================================================================
# OPTIMIZED OUTPUT STREAMS - SEQUENTIAL STARTUP TO PREVENT OVERLOAD
# ===============================================================================================================

print("\n💾 Starting streaming queries with optimized resource management...")
print(f"🔧 Session ID: {SESSION_ID}")

# Start queries with delays to prevent conflicts
print("\n Starting Crime Hotspots (Most Critical)...")
query1 = write_to_mongodb_streaming(
    crime_hotspots, 
    "stream_crime_hotspots",
    f"/tmp/checkpoint_hotspots_{SESSION_ID}"
)
time.sleep(3)  # Delay to prevent conflicts

# Console output for immediate monitoring
console_query1 = crime_hotspots.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .option("checkpointLocation", f"/tmp/checkpoint_console_{SESSION_ID}") \
    .trigger(processingTime='45 seconds') \
    .start()

print("✅ Crime Hotspots stream started successfully!")
time.sleep(2)

print("\n⚡ Starting Violence Escalation Detection...")
query3 = write_to_mongodb_streaming(
    violence_escalation,
    "stream_violence_escalation",
    f"/tmp/checkpoint_violence_{SESSION_ID}"
)
time.sleep(3)  # Prevent conflicts
print(" Violence Escalation stream started successfully!")

print("\n🏠 Starting Domestic Violence Correlation...")
query4 = write_to_mongodb_streaming(
    domestic_correlation,
    "stream_domestic_correlation", 
    f"/tmp/checkpoint_domestic_{SESSION_ID}"
)
time.sleep(3)  # Prevent conflicts
print(" Domestic Correlation stream started successfully!")

print("\n Starting Pattern Analysis...")  
query2 = write_to_mongodb_streaming(
    stream_pattern_analysis,
    "stream_pattern_analysis", 
    f"/tmp/checkpoint_patterns_{SESSION_ID}"
)
time.sleep(3)  # Prevent conflicts
print(" Pattern Analysis stream started successfully!")

print("\n Starting Temporal Patterns...")
query5 = write_to_mongodb_streaming(
    temporal_patterns,
    "stream_temporal_patterns",
    f"/tmp/checkpoint_temporal_{SESSION_ID}"
)
time.sleep(2)

print(" All streams started successfully!")

print("\n All 5 Advanced Stream Processing Transformations are running!")
print(" Data is being persisted to MongoDB collections:")
print("   • stream_crime_hotspots - Real-time hotspot detection")
print("   • stream_pattern_analysis - Historical pattern matching") 
print("   • stream_violence_escalation - Violence escalation monitoring")
print("   • stream_domestic_correlation - Domestic violence correlations")
print("   • stream_temporal_patterns - Advanced temporal analysis")
print("\n Monitoring console outputs...")

# Graceful termination handling
import signal
import sys

def signal_handler(sig, frame):
    print('\n Gracefully stopping all streams...')
    try:
        query1.stop()
        query2.stop()  
        query3.stop()
        query4.stop()
        query5.stop()
        console_query1.stop()
    except:
        pass
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Wait for streams with better error handling
try:
    query1.awaitTermination()
except Exception as e:
    print(f"Stream 1 terminated with error: {e}")
    
try:
    query2.awaitTermination()
except Exception as e:
    print(f"Stream 2 terminated with error: {e}")
    
try:
    query3.awaitTermination() 
except Exception as e:
    print(f"Stream 3 terminated with error: {e}")
    
try:
    query4.awaitTermination()
except Exception as e:
    print(f"Stream 4 terminated with error: {e}")
    
try:
    query5.awaitTermination()
except Exception as e:
    print(f"Stream 5 terminated with error: {e}")
