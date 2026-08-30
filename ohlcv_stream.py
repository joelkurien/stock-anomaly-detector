import os
import asyncio

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, to_json, col, expr, window, max, min, sum, struct, 
    stddev, avg, when, abs, corr
)

from pyspark.sql.types import TimestampType, DoubleType
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.utils import StreamingQueryException

from schemas import LIVE_SCHEMA, PROCESSING_SCHEMA, STATE_SCHEMA

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("EXTERNAL_KAKFA_BOOTSTRAP_SERVERS", "localhost:29092")
STK_TOPIC_NAME = "stock-price"

def process_init():
    try:
        spark = SparkSession.builder \
            .appName("StockAnomalyProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        schema = LIVE_SCHEMA
        
        stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", STK_TOPIC_NAME) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
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
        .withWatermark("event_time", "5 seconds") \
        .groupBy(
            window(col("event_time"), "10 seconds", "10 seconds"),
            col("ticker")
        ) \
        .agg(
            min(struct(col("event_time"), col("price"))).getField("Price").alias("open"), 
            max(col("price")).alias("high"),
            min(col("price")).alias("low"),
            max(struct(col("event_time"), col("price"))).getField("Price").alias("close"),
            sum(col("volume")).alias("volume"),
            stddev("Price").alias("stddev_price"),
            avg("Price").alias("vwap"),
            corr(col("price"), col("volume")).alias("trend_vol_corr"),
            expr("count(*) as trade_count")
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
            col("stddev_price"),
            col("trend_vol_corr"),
            col("trade_count")
        )
    
    return ohlc_df

def ohlc_feateng(ohlc_df):
    feateng_df = ohlc_df \
        .withColumn("body_range", abs(col("close") - col("open"))) \
        .withColumn("total_range", col("high") - col("low")) \
        .withColumn("intra_bar_return", (col("close") - col("open"))/(col("open") + 1e-5)) \
        .withColumn("upper_wick", col("high") - expr("greatest(open, close)")) \
        .withColumn("lower_wick", expr("least(open, close)") - col("low")) \
        .withColumn("wick_body_ratio", (col("upper_wick") + col("lower_wick")) / (col("body_range") + 1e-5)) \
        .withColumn("is_dead_bar", when(
            col("stddev_price").isNull() | (col("stddev_price") < 1e-4) | 
            (col("volume")<1e-5), 1
        ).otherwise(0)) \
        .withColumn("intraday_volatility", col("stddev_price") / (col("vwap") + 1e-5)) \
        .withColumn("price_reversal_ratio", col("upper_wick") / (col("body_range") + 1e-5))
    
    return feateng_df

async def ohlc_stream():
    spark = query = None
    logger.info("Initiated Data Processing")
    try:
        spark, df = process_init()
        ohlc_df = process_ohlc(df)
        feateng_df = ohlc_feateng(ohlc_df)
        
        feature_cols = [column for column in feateng_df.columns]
        
        query = feateng_df.select(
            col("ticker").cast("string").alias("key"),
            to_json(struct(*feature_cols)).alias("value")    
        ).writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:29092") \
            .option("topic", "stock-ohlcv") \
            .option("checkpointLocation", "tmp/chk/stock_ohlcv") \
            .outputMode("append") \
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
    asyncio.run(ohlc_stream())