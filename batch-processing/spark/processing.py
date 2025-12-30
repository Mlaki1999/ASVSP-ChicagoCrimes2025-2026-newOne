import os
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.window import Window


# Inicijalizacija Spark sesije
spark = SparkSession \
    .builder \
    .appName("Chicago Crimes Processing") \
    .getOrCreate()

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org").setLevel(logger.Level.ERROR)
    logger.LogManager.getLogger("akka").setLevel(logger.Level.ERROR)

quiet_logs(spark)

HDFS_NAMENODE = os.environ["CORE_CONF_fs_defaultFS"]

# Učitavanje transformisanog dataset-a
df_crimes = spark.read \
    .option("delimiter", ",") \
    .option("header", "true") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/transform/chicago_crimes.csv")

# ----------------------------------------------- Query 1 -----------------------------------------------
print("\n--- 1. Koji dani u nedelji imaju najviše zločina? ---")
df_query1 = df_crimes \
    .groupBy("day_of_week") \
    .agg(count("*").alias("total_crimes")) \
    .orderBy(desc("total_crimes"))

df_query1.show()

# ----------------------------------------------- Query 2 -----------------------------------------------
print("\n--- 2. Koji tip zločina je najčešći svake godine? ---")
df_query2 = df_crimes \
    .groupBy("year", "primary_type") \
    .agg(count("*").alias("total_crimes")) \
    .withColumn("rank", row_number().over(Window.partitionBy("year").orderBy(desc("total_crimes")))) \
    .filter(col("rank") == 1) \
    .orderBy("year")

df_query2.show()

# ----------------------------------------------- Query 3 -----------------------------------------------
print("\n--- 3. Koliko zločina se dešava po lokaciji? ---")
df_query3 = df_crimes \
    .groupBy("location_description") \
    .agg(count("*").alias("total_crimes")) \
    .orderBy(desc("total_crimes"))

df_query3.show()

# ----------------------------------------------- Query 4 -----------------------------------------------
print("\n--- 4. Da li postoji veza između zločina i učešća u hapšenjima? ---")
df_query4 = df_crimes \
    .groupBy("primary_type", "arrest") \
    .agg(count("*").alias("total_crimes")) \
    .orderBy("primary_type", "arrest")

df_query4.show()

# ----------------------------------------------- Query 5 -----------------------------------------------
print("\n--- 5. Kako se menja trend nasilnih zločina tokom godina? ---")
violent_crimes = ["HOMICIDE", "ASSAULT", "BATTERY", "ROBBERY"]
df_query5 = df_crimes.filter(col("primary_type").isin(violent_crimes)) \
    .groupBy("year") \
    .agg(count("*").alias("violent_crimes")) \
    .orderBy("year")

df_query5.show()

# ----------------------------------------------- Query 6 -----------------------------------------------
print("\n--- 6. Koje lokacije su najopasnije za specifične vrste zločina? ---")
df_query6 = df_crimes \
    .groupBy("location_description", "primary_type") \
    .agg(count("*").alias("total_crimes")) \
    .orderBy(desc("total_crimes"))

df_query6.show()

# ----------------------------------------------- Query 7 -----------------------------------------------
print("\n--- 7. Koliko zločina se dešava po mesecima tokom godine? ---")
df_query7 = df_crimes \
    .groupBy("month") \
    .agg(count("*").alias("total_crimes")) \
    .orderBy("month")

df_query7.show()

# ----------------------------------------------- Query 8 -----------------------------------------------
print("\n--- 8. Da li su domaći incidenti povezani sa većim brojem hapšenja? ---")
df_query8 = df_crimes.filter(col("domestic") == True) \
    .groupBy("arrest") \
    .agg(count("*").alias("total_crimes")) \
    .orderBy("arrest")

df_query8.show()

# ----------------------------------------------- Query 9 -----------------------------------------------
print("\n--- 9. Koji su najčešći tipovi zločina na osnovu sezone? ---")
df_query9 = df_crimes \
    .groupBy("season", "primary_type") \
    .agg(count("*").alias("total_crimes")) \
    .withColumn("rank", row_number().over(Window.partitionBy("season").orderBy(desc("total_crimes")))) \
    .filter(col("rank") == 1) \
    .orderBy("season")

df_query9.show()

# ----------------------------------------------- Query 10 -----------------------------------------------
print("\n--- 10. Šta je trend u geografskom rasporedu zločina tokom godina? ---")
df_query10 = df_crimes \
    .groupBy("year", "latitude", "longitude") \
    .agg(count("*").alias("total_crimes")) \
    .orderBy("year", desc("total_crimes"))

df_query10.show()

# Snimanje rezultata
df_query1.write \
    .mode("overwrite") \
    .csv(HDFS_NAMENODE + "/user/root/data-lake/results/query1.csv")
