import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

# Inicijalizacija Spark sesije
spark = SparkSession \
    .builder \
    .appName("Chicago Crimes Preprocessing") \
    .getOrCreate()

# Smanji logove
def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

quiet_logs(spark)

# HDFS Namenode
HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Učitavanje podataka iz HDFS-a
df_crimes = spark.read \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/raw/batch_data.csv")

# Selektovanje i preimenovanje kolona
df_crimes = df_crimes.select(
    col("ID").alias("crime_id"),
    col("Case Number").alias("case_number"),
    col("Date").alias("date"),
    col("Primary Type").alias("primary_type"),
    col("Description").alias("description"),
    col("Location Description").alias("location_description"),
    col("Arrest").alias("arrest"),
    col("Domestic").alias("domestic"),
    col("Latitude").alias("latitude"),
    col("Longitude").alias("longitude")
)

# Konverzija tipova podataka
df_crimes = df_crimes \
    .withColumn("date", to_timestamp("date", "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("arrest", col("arrest").cast(BooleanType())) \
    .withColumn("domestic", col("domestic").cast(BooleanType())) \
    .withColumn("latitude", col("latitude").cast(FloatType())) \
    .withColumn("longitude", col("longitude").cast(FloatType()))

# Dodavanje novih kolona (dan u nedelji, mesec, godina)
df_crimes = df_crimes \
    .withColumn("day_of_week", date_format("date", "EEEE")) \
    .withColumn("month", month("date")) \
    .withColumn("year", year("date"))

# Dodavanje kolone 'season'
df_crimes = df_crimes.withColumn(
    "season",
    when((col("month").isin(12, 1, 2)), "Winter")
    .when((col("month").isin(3, 4, 5)), "Spring")
    .when((col("month").isin(6, 7, 8)), "Summer")
    .when((col("month").isin(9, 10, 11)), "Fall")
    .otherwise("Unknown")
)

# Filtriranje nedostajućih vrednosti (npr. latitude/longitude)
df_crimes = df_crimes.filter(col("latitude").isNotNull() & col("longitude").isNotNull())

# Prikaz rezultata
df_crimes.show()

# Čuvanje obrađenih podataka u HDFS
df_crimes.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/transform/chicago_crimes.csv")
