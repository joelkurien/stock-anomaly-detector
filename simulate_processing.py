import asyncio
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, expr, window, max, min, sum, struct, 
    stddev, avg, when, abs
)
from pyspark.sql.types import TimestampType, StructType, StructField, StringType, DoubleType
from pyspark.sql.utils import StreamingQueryException
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("EXTERNAL_KAKFA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC_NAME = "stock-price"

def process_init():
    try:
        spark = SparkSession.builder \
            .appName("StockAnomalyProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        schema = StructType([
            StructField("ticker", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("Price", StringType(), True),
            StructField("Volume", StringType(), True)
        ])
        
        stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", TOPIC_NAME) \
            .option("startingOffsets", "latest") \
            .load()
        
        parsed_df = stream_df \
            .selectExpr("CAST(value AS STRING) as price_info") \
            .select(from_json(col("price_info"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("event_time", col("timestamp").cast(TimestampType())) \
            .withColumn("price", col("Price").cast(DoubleType())) \
            .withColumn("volume", col("Volume").cast(DoubleType()))
        
        return spark, parsed_df
    except Exception as e:
        logger.error(f"Failed Spark Connection / Kafka streaming: {e}")
        raise e
    
def process_ohlc(df):
    ohlc_df = df \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(
            window(col("event_time"), "1 minute", "1 minute"),
            col("ticker")
        ) \
        .agg(
            min(struct(col("event_time"), col("price"))).getField("Price").alias("open"), 
            max(col("price")).alias("high"),
            min(col("price")).alias("low"),
            max(struct(col("event_time"), col("price"))).getField("Price").alias("close"),
            sum(col("volume")).alias("volume"),
            stddev("Price").alias("stddev_price"),
            avg("Price").alias("vwap")
        ) \
        .select(
            col("window.start").alias("bar_start"),
            col("window.end").alias("bar_end"),
            col("ticker"),
            col("open"),
            col("high"),
            col("low"),
            col("close"),
            col("volume"),
            col("vwap"),
            col("stddev_price")
        )
    
    return ohlc_df

def ohlc_feateng(ohlc_df):
    feateng_df = ohlc_df \
        .withColumn("body_range", abs(col("close") - col("open"))) \
        .withColumn("total_range", col("high") - col("low")) \
        .withColumn("return_1m", (col("close") - col("open"))/(col("open") + 1e-5)) \
        .withColumn("upper_wick", col("high") - expr("greatest(open, close)")) \
        .withColumn("lower_wick", expr("least(open, close)") - col("low")) \
        .withColumn("wick_body_ratio", (col("upper_wick") + col("lower_wick")) / (col("body_range") + 1e-5)) \
        .withColumn("zero_volatity", when(col("stddev_price") < 1e-4, 1).otherwise(0)) \
        .withColumn("intraday_volatility", col("stddev_price") / (col("vwap") + 1e-5))
    
    return feateng_df

def rolling_feateng(df):
    rolling_df = df \
        .withWatermark("event_time", "10 seconds") \
        .groupBy(
            window(col("event_time"), "5 minute", "1 minute"),
            col("ticker")
        ) \
        .agg(
            avg("Volume").alias("avg_volume_5m"),
            stddev("Volume").alias("std_volume_5m"),
            avg("Price").alias("avg_price_5m")
        ) \
        .select(
            col("window.end").alias("bar_end"),
            col("ticker").alias("ticker"),
            col("avg_volume_5m"),
            col("std_volume_5m"),
            col("avg_price_5m")
        )
        
    return rolling_df
        
def combine_dfs(df1, df2):
    combined_df = df1.join(
        df2,
        on=["bar_end", "ticker"],
        how="inner"
    ) \
    .withColumn("volume_shock_ratio", col("volume") / (col("avg_volume_5m") + 1e-5)) \
    .withColumn("price_deviation_5m", abs(col("close") - col("avg_price_5m")) / (col("avg_price_5m") + 1e-5))
    return combined_df

async def process_stream():
    spark = query = None
    logger.info("Initiated Data Processing")
    try:
        spark, df = process_init()
        ohlc_df = process_ohlc(df)
        processed_ohlc_df = ohlc_feateng(ohlc_df)
        rolling_df = rolling_feateng(df)
        combined_df = combine_dfs(processed_ohlc_df, rolling_df)
        query = combined_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .start()
        
        logger.info("Data Querying Successfully")
        await asyncio.to_thread(query.awaitTermination)
    except StreamingQueryException as e:
        logger.error(f"Spark Query Streaming Error: {e}")
    except Exception as e:
        logger.error(f"Failure in streaming execution: {e}")
    finally:
        if query and query.isActive:
            logger.info("Query Stopped")
            query.stop()
            
        if spark:
            logger.info("Spark Stopped")
            spark.stop()
            
if __name__ == "__main__":
    asyncio.run(process_stream())