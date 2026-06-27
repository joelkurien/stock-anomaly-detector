from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum, first, last, min, max
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField
# import os
from dotenv import load_dotenv

load_dotenv()

spark = SparkSession.builder \
        .appName("anomaly-detection") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
        .getOrCreate()

print(spark)

price_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:29092") \
    .option("subscribe", "crypto") \
    .option("startingOffsets", "earliest") \
    .load()

schema = StructType([
    StructField("s", StringType()),
    StructField("p", DoubleType()),
    StructField("t", LongType()),
    StructField("v", DoubleType()),
])

df = price_df.select(
    col("key").cast("string").alias("symbol"),
    from_json(col("value").cast("string"), schema).alias("data"),
    col("timestamp")
).select(
    col("symbol"),
    col("data.p").alias("price"),
    col("data.v").alias("volume"),
    col("data.t").alias("unix_timestamp"),
    col("data.s").alias("ticker"),
    col("timestamp")
)

ohlcv = df \
    .withWatermark("timestamp", "15 seconds") \
    .groupBy(col("symbol"), window(col("timestamp"), "15 seconds")) \
    .agg(
        first(col("price")).alias("open"),
        max(col("price")).alias("high"),
        min(col("price")).alias("low"),
        last(col("price")).alias("close"),
        sum(col("volume")).alias("volume")
    )


query = ohlcv.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()

