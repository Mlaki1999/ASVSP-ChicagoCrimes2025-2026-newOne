import json
import time
import uuid
import shutil
import os
import sys
import logging
import warnings
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pymongo import MongoClient

# Suppress Python warnings
warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.ERROR)
logging.getLogger('py4j').setLevel(logging.ERROR)

# Suppress Spark console progress bars
# os.environ['SPARK_SUBMIT_OPTS'] = '--conf spark.ui.showConsoleProgress=false'

# Custom stderr filter to block Stage progress lines
class StageProgressFilter:
    def __init__(self, original_stderr):
        self.original_stderr = original_stderr
        
    def write(self, message):
        # Block Stage progress lines and empty lines with just carriage returns
        if 'Stage ' not in message and message.strip() and not message.startswith('\r'):
            self.original_stderr.write(message)
    
    def flush(self):
        self.original_stderr.flush()

# Apply the filter
sys.stderr = StageProgressFilter(sys.stderr)

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
            print(f" Could not clean {dir_path}: {e}")

# Clean up before starting
print("🧹 Cleaning up old checkpoints...")
cleanup_checkpoints()
time.sleep(2)

def quiet_logs(sc):
    """Aggressively suppress ALL Spark and Kafka warnings for clean output"""
    logger = sc._jvm.org.apache.log4j
    
    # Set root logger to FATAL (only critical errors)
    logger.LogManager.getRootLogger().setLevel(logger.Level.FATAL)
    
    # Suppress all major loggers
    for log_name in [
        "org", "akka", "org.apache.kafka", "kafka",
        "org.apache.spark", "org.spark_project", 
        "org.apache.hadoop", "org.apache.parquet",
        "org.apache.spark.sql.kafka010",
        "org.apache.spark.sql.kafka010.KafkaDataConsumer",
        "org.apache.kafka.clients",
        "org.apache.kafka.clients.consumer",
        "org.apache.kafka.clients.admin",
        "org.apache.spark.scheduler",
        "org.apache.spark.executor",
        "org.apache.spark.sql.execution.streaming",
        "org.apache.spark.sql.execution.streaming.MicroBatchExecution",
        "org.apache.spark.sql.execution.streaming.ProcessingTimeExecutor"
    ]:
        logger.LogManager.getLogger(log_name).setLevel(logger.Level.FATAL)
    
    print("✓ Logging configured: ULTRA QUIET MODE (fatal errors only)")

# Force cleanup of any existing Spark contexts
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 pyspark-shell'

print("\n" + "="*80)
print(" SPARK SESSION INITIALIZATION")
print("="*80)

# Clean up any existing contexts
try:
    from pyspark import SparkContext
    if SparkContext._active_spark_context:
        print(" Cleaning up existing SparkContext...")
        SparkContext._active_spark_context.stop()
        SparkContext._active_spark_context = None
        time.sleep(2)
except:
    pass

print("\n Creating Spark session in LOCAL mode...")
print("   Master: local[2] (reduced for Docker memory constraints)")
print("   Driver Memory: 1g")
print("   Executor Memory: 1g")
print("    Note: Using local mode with memory optimization for Docker")

# Define all required Kafka JARs
kafka_jars = [
    "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.4.0.jar",
    "/opt/spark/jars/kafka-clients-3.4.0.jar",
    "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.4.0.jar",
    "/opt/spark/jars/commons-pool2-2.11.1.jar"
]
jars_path = ",".join(kafka_jars)
print(f"    Loading Kafka JARs: {len(kafka_jars)} files")

try:
    spark = SparkSession \
        .builder \
        .appName(f"ChicagoCrimesStreaming_{SESSION_ID}") \
        .master("local[2]") \
        .config("spark.jars", jars_path) \
        .config("spark.driver.memory", "1g") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.maxResultSize", "512m") \
        .config("spark.memory.fraction", "0.6") \
        .config("spark.memory.storageFraction", "0.5") \
        .config("spark.sql.streaming.checkpointLocation", f"/tmp/checkpoint_{SESSION_ID}") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "localhost") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/tmp/log4j.properties -XX:+UseG1GC -XX:+UnlockExperimentalVMOptions -XX:InitiatingHeapOccupancyPercent=35") \
        .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=file:/tmp/log4j.properties -XX:+UseG1GC") \
        .config("spark.ui.showConsoleProgress", "false") \
        .config("spark.ui.dagScheduler.showConsoleProgress", "false") \
        .getOrCreate()
    
    # Also set SparkContext configuration to suppress console progress
    spark.sparkContext.setLogLevel("FATAL")
    
    # Test the session (suppress output)
    print("    Testing Spark session...")
    import sys
    from io import StringIO
    old_stdout = sys.stdout
    sys.stdout = StringIO()  # Suppress test output
    test_df = spark.createDataFrame([("test",)], ["value"])
    count3 = test_df.count()
    sys.stdout = old_stdout  # Restore stdout
    
    print(f"    Test successful! DataFrame count: {count3}")
    print(f" Spark session created successfully!")
    print(f"   App ID: {spark.sparkContext.applicationId}")
    print(f"   Master: {spark.sparkContext.master}")
    
except Exception as e:
    print(f" Failed to create Spark session: {e}")
    exit(1)

print("="*80 + "\n")

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
            return len(result.inserted_ids)
        return 0
    except Exception as e:
        print(f"✗ Error saving to {collection_name}: {str(e)}")
        return 0

def create_visual_batch_writer(collection_name, query_name, emoji, trigger_interval):
    """Create a foreachBatch function with clean visual output"""
    def write_batch(batch_df, batch_id):
        # Redirect stdout temporarily to suppress Spark progress
        import sys
        from io import StringIO
        
        record_count = batch_df.count()
        timestamp = datetime.now().strftime("%H:%M:%S")
        
        # Use plural/singular correctly
        record_word = "record" if record_count == 1 else "records"
        
        print("\n" + "="*80)
        print(f"{emoji} {query_name} - Batch #{batch_id} [{timestamp}]")
        print(f"   Trigger Interval: {trigger_interval}")
        print("="*80)
        
        if record_count > 0:
            print(f"📊 Processing {record_count} {record_word}...")
            
            # Collect sample for clean display (avoid .show() which triggers warnings)
            try:
                # Capture Spark output during limit/collect
                old_stdout = sys.stdout
                sys.stdout = StringIO()
                
                sample_records = batch_df.limit(3).collect()
                
                # Restore stdout
                sys.stdout = old_stdout
                
                # Calculate sample size (avoid PySpark's min function)
                num_samples = len(sample_records)
                if num_samples > 3:
                    num_samples = 3
                
                print(f"\n📋 Sample Records (showing {num_samples} of {record_count}):")
                print("-" * 80)
                for i, record in enumerate(sample_records[:3], 1):  # Only take first 3
                    print(f"\n   Record {i}:")
                    # Show key fields based on schema
                    record_dict = record.asDict()
                    field_count = 0
                    for key, value in record_dict.items():
                        if field_count >= 6:  # Show first 6 fields
                            break
                        if value is not None:
                            # Format value based on type
                            if isinstance(value, datetime):
                                value_str = value.strftime("%Y-%m-%d %H:%M:%S")
                            elif isinstance(value, float):
                                value_str = f"{value:.4f}"
                            elif isinstance(value, list):
                                value_str = f"[{len(value)} items]"
                            else:
                                value_str = str(value)[:50]  # Truncate long strings
                            print(f"      {key}: {value_str}")
                            field_count += 1
                print("-" * 80)
            except Exception as e:
                print(f"   ⚠️  Could not display sample: {str(e)[:60]}")
            
            # Save to MongoDB
            saved_count = write_to_mongodb_batch(batch_df, collection_name)
            saved_word = "record" if saved_count == 1 else "records"
            print(f"\n✅ Successfully saved {saved_count} {saved_word} to: {collection_name}")
        else:
            print(f"⏳ No new records in this batch (waiting for streaming data...)")
        
        print("="*80 + "\n")
    
    return write_batch

def write_to_mongodb_streaming(df, collection_name, checkpoint_location, query_name, emoji, trigger_interval):
    """Write streaming DataFrame to MongoDB with visual feedback"""
    batch_writer = create_visual_batch_writer(collection_name, query_name, emoji, trigger_interval)
    
    return df.writeStream \
        .foreachBatch(batch_writer) \
        .option("checkpointLocation", checkpoint_location) \
        .trigger(processingTime=trigger_interval) \
        .start()

# -----------------------------
# UDF za parsiranje JSON-a
def parse_json_safe(s):
    try:
        return json.loads(s)
    except:
        return {}  # prazna mapa za loše JSON-e

# Create UDF with error handling
print(" Creating JSON parsing UDF...")
try:
    parse_udf = udf(lambda s: parse_json_safe(s), MapType(StringType(), StringType()))
    print(" JSON UDF created successfully!")
except Exception as e:
    print(f" Failed to create UDF: {e}")
    exit(1)

# -----------------------------
# Čitanje iz Kafka sa timestamp informacijama
print(" Setting up Kafka stream connection...")
try:
    df_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092") \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("includeHeaders", "true") \
        .option("kafka.request.timeout.ms", "120000") \
        .option("kafka.session.timeout.ms", "60000") \
        .option("kafka.max.poll.interval.ms", "300000") \
        .option("kafka.fetch.max.wait.ms", "10000") \
        .option("kafka.connections.max.idle.ms", "540000") \
        .option("kafka.metadata.max.age.ms", "30000") \
        .load()
    
    print(" Kafka stream connection configured successfully!")
    
except Exception as e:
    print(f" Failed to configure Kafka stream: {e}")
    exit(1)

# Parsiranje JSON sa timestamp informacijama
print(" Setting up JSON parsing and data extraction...")
try:
    df_parsed = df_stream.selectExpr("CAST(value AS STRING)", "timestamp") \
        .withColumn("parsed", parse_udf(col("value"))) \
        .filter(col("parsed")["ID"].isNotNull()) \
        .withColumn("event_time", col("timestamp")) \
        .withWatermark("event_time", "10 seconds")

    # Extract fields from parsed map with proper type conversions
    def extract(colname):
        return col("parsed")[colname]

    # Parse date with fallback for different formats
    # API format: 2024-12-31T23:58:00.000 (with milliseconds)
    # Some may be: 2024-12-31T23:58:00 (without milliseconds)
    date_col = when(
        extract("Date").contains("."),
        to_timestamp(extract("Date"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
    ).otherwise(
        to_timestamp(extract("Date"), "yyyy-MM-dd'T'HH:mm:ss")
    )

    df_crimes_enriched = df_parsed.select(
        extract("ID").cast(IntegerType()).alias("crime_id"),
        extract("Case Number").alias("case_number"),
        date_col.alias("crime_date"),
        extract("Primary Type").alias("primary_type"),
        extract("Description").alias("description"),
        extract("Location Description").alias("location_description"),
        # Handle string "true"/"false" from API
        when(lower(extract("Arrest")) == "true", True).otherwise(False).alias("arrest"),
        when(lower(extract("Domestic")) == "true", True).otherwise(False).alias("domestic"),
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

    print(" Data parsing and enrichment configured successfully!")
    
except Exception as e:
    print(f" Failed to configure data parsing: {e}")
    exit(1)

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

print("\n TRANSFORMATION 5: Advanced Temporal Pattern Analysis")

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
# OPTIMIZED OUTPUT STREAMS WITH STAGGERED TIMING AND VISUAL FEEDBACK
# ===============================================================================================================

print("\n" + "#"*80)
print("# STARTING STREAMING QUERIES WITH OPTIMIZED TIMING")
print("#"*80)
print(f"\n🔧 Session ID: {SESSION_ID}")
print("\n📊 Optimized Query Timing (prevents 'falling behind' warnings):")
print("   🔥 Hotspots:            Every 30 seconds (Fast - Critical real-time alerts)")
print("   📈 Temporal Patterns:   Every 45 seconds (Medium - Trend analysis)")
print("   🔍 Pattern Analysis:    Every 60 seconds (Standard - Anomaly detection)")
print("   ⚠️  Violence Escalation: Every 90 seconds (Slower - Complex aggregations)")
print("   🔗 Domestic Correlation: Every 120 seconds (SLOWEST - Stream-to-stream join)")
print("\n" + "#"*80 + "\n")

time.sleep(3)

# QUERY 1: Crime Hotspots - 30 seconds (FASTEST)
print("\n🔥 [1/5] Starting Crime Hotspots Detection...")
print("    ⏱️  Trigger: Every 30 seconds")
query1 = write_to_mongodb_streaming(
    crime_hotspots, 
    "stream_crime_hotspots",
    f"/tmp/checkpoint_hotspots_{SESSION_ID}",
    "Crime Hotspot Detection",
    "🔥",
    "30 seconds"
)
print("    ✅ Started successfully!")
time.sleep(2)

# QUERY 2: Temporal Patterns - 45 seconds
print("\n📈 [2/5] Starting Temporal Pattern Analysis...")
print("    ⏱️  Trigger: Every 45 seconds")
query5 = write_to_mongodb_streaming(
    temporal_patterns,
    "stream_temporal_patterns",
    f"/tmp/checkpoint_temporal_{SESSION_ID}",
    "Temporal Pattern Analysis",
    "📈",
    "45 seconds"
)
print("    ✅ Started successfully!")
time.sleep(2)

# QUERY 3: Pattern Analysis - 60 seconds
print("\n🔍 [3/5] Starting Historical Pattern Matching...")  
print("    ⏱️  Trigger: Every 60 seconds")
query2 = write_to_mongodb_streaming(
    stream_pattern_analysis,
    "stream_pattern_analysis", 
    f"/tmp/checkpoint_patterns_{SESSION_ID}",
    "Historical Pattern Matching",
    "🔍",
    "60 seconds"
)
print("    ✅ Started successfully!")
time.sleep(2)

# QUERY 4: Violence Escalation - 90 seconds
print("\n⚠️  [4/5] Starting Violence Escalation Detection...")
print("    ⏱️  Trigger: Every 90 seconds")
query3 = write_to_mongodb_streaming(
    violence_escalation,
    "stream_violence_escalation",
    f"/tmp/checkpoint_violence_{SESSION_ID}",
    "Violence Escalation Monitoring",
    "⚠️",
    "90 seconds"
)
print("    ✅ Started successfully!")
time.sleep(2)

# QUERY 5: Domestic Correlation - 120 seconds (SLOWEST - complex join)
print("\n🔗 [5/5] Starting Domestic Violence Correlation...")
print("    ⏱️  Trigger: Every 120 seconds (Complex stream-to-stream join)")
query4 = write_to_mongodb_streaming(
    domestic_correlation,
    "stream_domestic_correlation", 
    f"/tmp/checkpoint_domestic_{SESSION_ID}",
    "Domestic Violence Correlation",
    "🔗",
    "120 seconds"
)
print("    ✅ Started successfully!")

print("\n" + "#"*80)
print("# ALL 5 STREAMING QUERIES RUNNING!")
print("#"*80)

print("\n📦 MongoDB Collections:")
print("   • stream_crime_hotspots - Real-time hotspot detection (30s)")
print("   • stream_temporal_patterns - Advanced temporal analysis (45s)")
print("   • stream_pattern_analysis - Historical pattern matching (60s)")
print("   • stream_violence_escalation - Violence escalation monitoring (90s)")
print("   • stream_domestic_correlation - Domestic violence correlations (120s)")
print("\n🎯 Watch below for real-time processing updates...")
print("   (Different queries will trigger at staggered intervals)")
print("   All warnings suppressed - showing only actual processing events!")
print("\n" + "#"*80 + "\n")

# Graceful termination handling
import signal
import sys

def signal_handler(sig, frame):
    print('\n⏸️  Gracefully stopping all streams...')
    try:
        query1.stop()
        query2.stop()  
        query3.stop()
        query4.stop()
        query5.stop()
        print('✅ All streams stopped successfully!')
    except Exception as e:
        print(f'⚠️  Error stopping streams: {e}')
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