import os
import json
import logging
import warnings
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import StreamingQuery
from pymongo import MongoClient

# Suppress Python warnings
warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.ERROR)
logging.getLogger('py4j').setLevel(logging.ERROR)

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


# def quiet_logs(sc):
#     """Suppress Spark logging for clean output."""
#     logger = sc._jvm.org.apache.log4j
#     logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)
#     logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
#     logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)
#     logger.LogManager.getLogger("org.apache.spark.sql.kafka010.KafkaDataConsumer").setLevel(logger.Level.ERROR)
#     logger.LogManager.getLogger("org.apache.kafka").setLevel(logger.Level.ERROR)


# Initialize Spark
spark = (
    SparkSession.builder
    .appName("Chicago Crimes Stream Processing")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    # This line suppresses KafkaDataConsumer warnings even before Spark starts
    .config("spark.driver.extraJavaOptions", "-Dlog4j.logger.org.apache.spark.sql.kafka010.KafkaDataConsumer=ERROR")
    .getOrCreate()
)

quiet_logs(spark)

TOPIC = "chicagocrimes"

# MongoDB Configuration
MONGODB_HOST = os.environ.get("MONGODB_HOST", "mongodb")
MONGODB_PORT = int(os.environ.get("MONGODB_PORT", "27017"))
MONGODB_USERNAME = os.environ.get("MONGODB_USERNAME", "root")
MONGODB_PASSWORD = os.environ.get("MONGODB_PASSWORD", "mongodb123")
MONGODB_DATABASE = "chicago_crimes"
MONGODB_URI = f"mongodb://{MONGODB_USERNAME}:{MONGODB_PASSWORD}@{MONGODB_HOST}:{MONGODB_PORT}/{MONGODB_DATABASE}?authSource=admin"


def load_kafka_stream(topic: str = TOPIC, starting_offset: str = "latest") -> DataFrame:
    """Load the Kafka stream from the topic into a Spark DataFrame."""
    return spark.readStream \
        .format('kafka') \
        .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:29092") \
        .option("subscribe", topic) \
        .option("startingOffsets", starting_offset) \
        .option("failOnDataLoss", "false") \
        .load()


def preprocess_stream_data(df_stream_raw: DataFrame) -> DataFrame:
    """Parse and enrich the incoming Kafka stream."""
    
    # UDF to parse JSON safely
    def parse_json(s):
        try:
            return json.loads(s)
        except:
            return {}
    
    parse_udf = udf(parse_json, MapType(StringType(), StringType()))
    
    # Parse JSON
    df_parsed = df_stream_raw.selectExpr("CAST(value AS STRING)", "timestamp") \
        .withColumn("parsed", parse_udf(col("value"))) \
        .filter(col("parsed")["ID"].isNotNull()) \
        .withColumn("event_time", col("timestamp")) \
        .withWatermark("event_time", "10 seconds")
    
    # Extract fields
    def extract(colname):
        return col("parsed")[colname]
    
    # Parse date with fallback
    date_col = when(
        extract("Date").contains("."),
        to_timestamp(extract("Date"), "yyyy-MM-dd'T'HH:mm:ss.SSS")
    ).otherwise(
        to_timestamp(extract("Date"), "yyyy-MM-dd'T'HH:mm:ss")
    )
    
    df_crimes = df_parsed.select(
        extract("ID").cast(IntegerType()).alias("crime_id"),
        extract("Case Number").alias("case_number"),
        date_col.alias("crime_date"),
        extract("Primary Type").alias("primary_type"),
        extract("Description").alias("description"),
        extract("Location Description").alias("location_description"),
        when(lower(extract("Arrest")) == "true", True).otherwise(False).alias("arrest"),
        when(lower(extract("Domestic")) == "true", True).otherwise(False).alias("domestic"),
        extract("Latitude").cast(DoubleType()).alias("latitude"),
        extract("Longitude").cast(DoubleType()).alias("longitude"),
        col("event_time")
    ).filter(col("crime_id").isNotNull() & col("crime_date").isNotNull())
    
    # Add derived columns
    df_enriched = df_crimes \
        .withColumn("hour_of_day", hour("crime_date")) \
        .withColumn("day_of_week", date_format("crime_date", "EEEE")) \
        .withColumn("month", month("crime_date")) \
        .withColumn("year", year("crime_date")) \
        .withColumn("is_violent", when(col("primary_type").isin("HOMICIDE", "ASSAULT", "BATTERY", "ROBBERY"), True).otherwise(False)) \
        .withColumn("is_weekend", when(col("day_of_week").isin("Saturday", "Sunday"), True).otherwise(False))
    
    return df_enriched


def write_stream_to_mongodb(df_stream: DataFrame, collection_name: str) -> StreamingQuery:
    """Write the stream to MongoDB collection."""
    
    def write_to_mongo(batch_df, batch_id):
        if batch_df.count() > 0:
            try:
                # Convert to list of dicts
                records = batch_df.toPandas().to_dict('records')
                
                # Connect to MongoDB
                client = MongoClient(MONGODB_URI)
                db = client[MONGODB_DATABASE]
                collection = db[collection_name]
                
                # Insert records
                collection.insert_many(records)
                print(f">> Batch {batch_id}: Inserted {len(records)} records to {collection_name}")
                
            except Exception as e:
                print(f">> Error writing to MongoDB: {e}")
    
    return df_stream.writeStream \
        .foreachBatch(write_to_mongo) \
        .option("checkpointLocation", f"/tmp/checkpoint-{collection_name}/") \
        .trigger(processingTime="10 seconds") \
        .start()


def write_stream_to_console(df_stream: DataFrame) -> StreamingQuery:
    """Write the stream to the console for debugging."""
    return df_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()


def run_query_1(df_stream: DataFrame) -> DataFrame:
    """Crime hotspots by geographic grid in 1-minute windows."""
    
    df_result = df_stream \
        .filter(col("latitude").isNotNull() & col("longitude").isNotNull()) \
        .withColumn("geo_grid", concat(
            round(col("latitude"), 2).cast(StringType()),
            lit("_"),
            round(col("longitude"), 2).cast(StringType())
        )) \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("geo_grid")
        ) \
        .agg(
            count("*").alias("crime_count"),
            approx_count_distinct("primary_type").alias("crime_variety"),
            avg("latitude").alias("avg_lat"),
            avg("longitude").alias("avg_lon")
        ) \
        .withColumn("hotspot_severity",
            when(col("crime_count") >= 5, "HIGH")
            .when(col("crime_count") >= 3, "MEDIUM")
            .otherwise("LOW")
        )
    
    return df_result.select(
        col("window.start").cast("timestamp").alias("window_start"),
        col("window.end").cast("timestamp").alias("window_end"),
        "geo_grid",
        "avg_lat",
        "avg_lon",
        "crime_count",
        "crime_variety",
        "hotspot_severity"
    )


def run_query_2(df_stream: DataFrame) -> DataFrame:
    """Temporal crime patterns by hour and day of week in 1-minute windows."""
    
    df_result = df_stream \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("primary_type"),
            col("hour_of_day"),
            col("is_weekend")
        ) \
        .agg(
            count("*").alias("incident_count"),
            avg(when(col("arrest") == True, 1).otherwise(0)).alias("arrest_rate")
        ) \
        .withColumn("crime_intensity",
            when(col("incident_count") >= 5, "VERY_HIGH")
            .when(col("incident_count") >= 3, "HIGH")
            .otherwise("NORMAL")
        )
    
    return df_result.select(
        col("window.start").cast("timestamp").alias("window_start"),
        col("window.end").cast("timestamp").alias("window_end"),
        "primary_type",
        "hour_of_day",
        "is_weekend",
        "incident_count",
        "arrest_rate",
        "crime_intensity"
    )


def run_query_3(df_stream: DataFrame) -> DataFrame:
    """Most common crime types by location description in 1-minute windows."""
    
    df_result = df_stream \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("location_description"),
            col("primary_type")
        ) \
        .agg(
            count("*").alias("crime_count")
        )
    
    return df_result.select(
        col("window.start").cast("timestamp").alias("window_start"),
        col("window.end").cast("timestamp").alias("window_end"),
        "location_description",
        "primary_type",
        "crime_count"
    )


def run_query_4(df_stream: DataFrame) -> DataFrame:
    """Violence escalation monitoring by geographic zone in 1-minute windows."""
    
    df_result = df_stream \
        .filter(col("is_violent") == True) \
        .withColumn("geo_zone", concat(
            round(col("latitude"), 1).cast(StringType()),
            lit("_"),
            round(col("longitude"), 1).cast(StringType())
        )) \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("geo_zone")
        ) \
        .agg(
            count("*").alias("violent_incidents"),
            sum(when(col("primary_type") == "HOMICIDE", 1).otherwise(0)).alias("homicides"),
            sum(when(col("arrest") == True, 1).otherwise(0)).alias("arrests_made"),
            avg("latitude").alias("zone_lat"),
            avg("longitude").alias("zone_lon")
        ) \
        .withColumn("escalation_risk",
            when(col("violent_incidents") >= 4, "CRITICAL")
            .when(col("violent_incidents") >= 2, "HIGH")
            .otherwise("NORMAL")
        )
    
    return df_result.select(
        col("window.start").cast("timestamp").alias("window_start"),
        col("window.end").cast("timestamp").alias("window_end"),
        "geo_zone",
        "zone_lat",
        "zone_lon",
        "violent_incidents",
        "homicides",
        "arrests_made",
        "escalation_risk"
    )


def run_query_5(df_stream: DataFrame) -> DataFrame:
    """Domestic vs non-domestic crime correlation by area in 1-minute windows."""
    
    df_result = df_stream \
        .withColumn("geo_area", concat(
            round(col("latitude"), 2).cast(StringType()),
            lit("_"),
            round(col("longitude"), 2).cast(StringType())
        )) \
        .groupBy(
            window(col("event_time"), "1 minute"),
            col("geo_area")
        ) \
        .agg(
            count("*").alias("total_crimes"),
            sum(when(col("domestic") == True, 1).otherwise(0)).alias("domestic_crimes"),
            sum(when(col("domestic") == False, 1).otherwise(0)).alias("non_domestic_crimes")
        ) \
        .withColumn("domestic_ratio",
            round(col("domestic_crimes") / col("total_crimes"), 2)
        )
    
    return df_result.select(
        col("window.start").cast("timestamp").alias("window_start"),
        col("window.end").cast("timestamp").alias("window_end"),
        "geo_area",
        "total_crimes",
        "domestic_crimes",
        "non_domestic_crimes",
        "domestic_ratio"
    )


def main() -> None:
    """Main entrypoint for the consumer."""
    
    print("\n>> Loading Kafka stream...")
    df_stream_raw = load_kafka_stream(TOPIC)
    df_stream_raw.printSchema()
    
    print("\n>> Preprocessing the stream data...")
    df_stream = preprocess_stream_data(df_stream_raw)
    df_stream.printSchema()
    
    print("\n>> Running queries on the stream...")
    df_query_1 = run_query_1(df_stream)
    df_query_2 = run_query_2(df_stream)
    df_query_3 = run_query_3(df_stream)
    df_query_4 = run_query_4(df_stream)
    df_query_5 = run_query_5(df_stream)
    
    # Write results to MongoDB (uncomment to enable)
    query_1_mongo = write_stream_to_mongodb(df_query_1, "stream_crime_hotspots")
    query_2_mongo = write_stream_to_mongodb(df_query_2, "stream_temporal_patterns")
    query_3_mongo = write_stream_to_mongodb(df_query_3, "stream_location_crimes")
    query_4_mongo = write_stream_to_mongodb(df_query_4, "stream_violence_escalation")
    query_5_mongo = write_stream_to_mongodb(df_query_5, "stream_domestic_correlation")
    
    # Debug to console
    query_1_debug = write_stream_to_console(df_query_1)
    # query_2_debug = write_stream_to_console(df_query_2)
    # query_3_debug = write_stream_to_console(df_query_3)
    # query_4_debug = write_stream_to_console(df_query_4)
    # query_5_debug = write_stream_to_console(df_query_5)
    
    print("\n>> Streaming queries started. Waiting for data...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()