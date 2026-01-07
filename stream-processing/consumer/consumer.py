import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType, MapType

TOPIC = "chicagocrimes"

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

spark = SparkSession \
    .builder \
    .appName("ChicagoCrimesConsumer") \
    .getOrCreate()

quiet_logs(spark)

# -----------------------------
# UDF za parsiranje JSON-a
def parse_json_safe(s):
    try:
        return json.loads(s)
    except:
        return {}  # prazna mapa za loše JSON-e

parse_udf = udf(lambda s: parse_json_safe(s), MapType(StringType(), StringType()))

# -----------------------------
# Čitanje iz Kafka
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:19092,kafka2:19092") \
    .option("subscribe", TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parsiranje JSON
df_parsed = df_stream.selectExpr("CAST(value AS STRING)") \
    .withColumn("parsed", parse_udf(col("value"))) \
    .filter(col("parsed")["ID"].isNotNull())  # samo validni zapisi

# Ekstrakt polja iz mape
def extract(colname):
    return col("parsed")[colname]

df_parsed = df_parsed.select(
    extract("ID").alias("ID"),
    extract("Case Number").alias("CaseNumber"),
    extract("Date").alias("Date"),
    extract("Block").alias("Block"),
    extract("IUCR").alias("IUCR"),
    extract("Primary Type").alias("PrimaryType"),
    extract("Description").alias("Description"),
    extract("Location Description").alias("LocationDescription"),
    extract("Arrest").alias("Arrest"),
    extract("Domestic").alias("Domestic"),
    extract("Beat").alias("Beat"),
    extract("District").alias("District"),
    extract("Ward").alias("Ward"),
    extract("Community Area").alias("CommunityArea"),
    extract("FBI Code").alias("FBI_Code"),
    extract("X Coordinate").alias("X_Coordinate"),
    extract("Y Coordinate").alias("Y_Coordinate"),
    extract("Year").alias("Year"),
    extract("Updated On").alias("UpdatedOn"),
    extract("Latitude").alias("Latitude"),
    extract("Longitude").alias("Longitude"),
    extract("Location").alias("Location")
)

# -----------------------------
# Funkcija za ispis batch-a
def print_query_name(query_name):
    def f(batch_df, batch_id):
        print("\n==========", query_name, "Batch:", batch_id, "==========")
        batch_df.show(truncate=False)
    return f

# Stream za sve zločine
query_all = df_parsed.writeStream \
    .foreachBatch(print_query_name("ALL_CRIMES")) \
    .start()

# Stream za BATTERY
query_battery = df_parsed.filter(col("PrimaryType") == "BATTERY") \
    .writeStream \
    .foreachBatch(print_query_name("BATTERY_CRIMES")) \
    .start()

# Stream za THEFT
query_theft = df_parsed.filter(col("PrimaryType") == "THEFT") \
    .writeStream \
    .foreachBatch(print_query_name("THEFT_CRIMES")) \
    .start()

# Čeka da se svi završe
query_all.awaitTermination()
query_battery.awaitTermination()
query_theft.awaitTermination()
