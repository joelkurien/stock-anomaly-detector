from pyspark.sql.types import (
    TimestampType, StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, IntegerType
)

LIVE_SCHEMA = StructType([
    StructField("ticker", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("Price", StringType(), True),
    StructField("Volume", StringType(), True)
])

OHLCV_SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("bar_start", TimestampType()),
    StructField("bar_end", TimestampType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("vwap", DoubleType()),
    StructField("stddev_price", DoubleType()), 
    StructField("trend_vol_corr", DoubleType()),
    StructField("trade_count", LongType()),
    StructField("body_range", DoubleType()), 
    StructField("total_range", DoubleType()),
    StructField("intra_bar_return", DoubleType()), 
    StructField("upper_wick", DoubleType()),
    StructField("lower_wick", DoubleType()), 
    StructField("wick_body_ratio", DoubleType()),
    StructField("is_dead_bar", BooleanType()), 
    StructField("intraday_volatility", DoubleType()),
    StructField("price_reversal_ratio", DoubleType())
])

PROCESSING_SCHEMA = StructType([
    StructField("ticker", StringType()),
    StructField("bar_start", TimestampType()),
    StructField("bar_end", TimestampType()),
    StructField("open", DoubleType()),
    StructField("high", DoubleType()),
    StructField("low", DoubleType()),
    StructField("close", DoubleType()),
    StructField("volume", DoubleType()),
    StructField("vwap", DoubleType()),
    StructField("trade_count", LongType()),
    StructField("dead_bar", IntegerType()),
    StructField("wick_body_ratio", DoubleType()),
    StructField("r_t", DoubleType()),
    StructField("gap_pct", DoubleType()),
    StructField("zscore", DoubleType()),
    StructField("vol_shock_ratio", DoubleType()),
    StructField("consec_dir_bars", IntegerType()),
    StructField("sign_flips_10bar", IntegerType()),
    StructField("halt_run", IntegerType())
])

STATE_SCHEMA = StructType([
    StructField("closes", StringType()),
    StructField("returns", StringType()),
    StructField("volumes", StringType()),
    StructField("dead_run", IntegerType())
])