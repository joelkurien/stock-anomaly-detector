import asyncio
import os

import numpy as np
import pandas as pd

from collections import deque

from ohlcv_stream import ohlc_stream
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.streaming.state import  GroupStateTimeout
from pyspark.sql.utils import StreamingQueryException
from pyspark.sql.types import (
    StructType, StructField, DoubleType, LongType
)


from operations import (
    debug_batches, write_batches, encode_state, decode_state, 
    log_return, gap_percentage, calc_zscore, 
    vol_shock_avg, no_reversed_dirs,
    to_pdtypes,
    rolling_stats, consec_bars_dir, 
    sign_flips, halt_run_check, get_returns
)

from schemas import LIVE_SCHEMA, PROCESSING_SCHEMA, STATE_SCHEMA, OHLCV_SCHEMA
from parameters import LOOKBACK_BARS
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logging.getLogger("py4j").setLevel(logging.WARNING)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("EXTERNAL_KAKFA_BOOTSTRAP_SERVERS", "localhost:29092")
OHLC_TOPIC_NAME = "stock-ohlcv"

class InterBarStateProcessor(StatefulProcessor):
    def init(self, handle: StatefulProcessorHandle):
        self.handle = handle
        self.close_state = handle.getValueState(
            "close_state", StructType([StructField("value", DoubleType(), True)])
        )
        self.dead_state = handle.getValueState(
            "dead_state", StructType([StructField("value", LongType(), True)])
        )
        self.returns_state = handle.getListState(
            "returns_state", StructType([StructField("value", DoubleType(), True)])
        )
        self.volumes_state = handle.getListState(
            "volumes_state", StructType([StructField("value", DoubleType(), True)])
        )
    
    def handleInputRows(self, key, rows, timerValues):
        (ticker, ) = key
        df = pd.concat(list(rows), ignore_index=True)
        
        if df.empty:
            yield pd.DataFrame(columns = [f.name for f in PROCESSING_SCHEMA.fields])
            return
        
        df = df.sort_values("bar_start").reset_index(drop=True)
        
        prev_close = self.close_state.get()[0] if self.close_state.exists() else 0
        returns = [rt[0] for rt in self.returns_state.get()] if self.returns_state.exists() else []
        volumes = [vol[0] for vol in self.volumes_state.get()] if self.volumes_state.exists() else []
        dead_run = self.dead_state.get()[0] if self.dead_state.exists() else 0
        
        close = df["close"].to_numpy(dtype="float64")
        open = df["open"].to_numpy(dtype="float64")
        volume = df["volume"].to_numpy(dtype="float64")
        dead = df["is_dead_bar"].to_numpy().astype(bool)

        rt, gap_pct = get_returns(close, open, prev_close)
        total_returns = returns + rt.tolist()
        total_volumes = volumes + volume.tolist()
        
        total_zscore, total_vol_shock = rolling_stats(total_returns, total_volumes)
        
        n_returns = len(returns)
        zscore = total_zscore[n_returns:]
        vol_shock = total_vol_shock[n_returns:]
        
        total_signs = np.sign(np.array(total_returns))
        consec = consec_bars_dir(total_signs)[n_returns:]
        flips = sign_flips(total_signs, window=9)[n_returns:]
        halt_run = halt_run_check(dead, dead_run)
        
        result_df = pd.DataFrame({
            "ticker": ticker,
            "bar_start": df["bar_start"],
            "bar_end": df["bar_end"],
            "open": df["open"],
            "high": df["high"],
            "low": df["low"],
            "close": df["close"],
            "volume": df["volume"],
            "vwap": df["vwap"],
            "trade_count": df["trade_count"].astype("int64"),
            "dead_bar": dead.astype("int64"),
            "wick_body_ratio": df["wick_body_ratio"],
            "r_t": rt,
            "gap_pct": gap_pct,
            "zscore": zscore, 
            "vol_shock_ratio": vol_shock,
            "consec_dir_bars": consec,
            "sign_flips_10bar": flips,
            "halt_run": halt_run
        })
        
        result_df = to_pdtypes(result_df, PROCESSING_SCHEMA)
        
        new_returns = total_returns[-LOOKBACK_BARS:]
        new_volumes = total_volumes[-LOOKBACK_BARS:]
        
        self.close_state.update((float(close[-1]),))
        self.dead_state.update((int(halt_run[-1]),))
        self.returns_state.put([(float(r),) for r in new_returns])
        self.volumes_state.put([(float(v),) for v in new_volumes])
        
        yield result_df
    
    def close(self):
        pass
        
def process_init():
    try:
        spark = SparkSession.builder \
            .appName("StockAnomalyProcessor") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.sql.streaming.stateStore.providerClass",
                    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        
        stream_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", OHLC_TOPIC_NAME) \
            .option("startingOffsets", "latest") \
            .load()
        
        parsed_df = stream_df.select(
            F.from_json(F.col("value").cast("string"), OHLCV_SCHEMA).alias("d")
        ).select("d.*")
        
        return spark, parsed_df
    except Exception as e:
        logger.error(f"Failed Spark Connection / Kafka streaming: {e}")
        raise e

def apply_inter_bar_states(df):
    return df.groupBy("ticker").transformWithStateInPandas(
        statefulProcessor = InterBarStateProcessor(),
        outputStructType = PROCESSING_SCHEMA,
        outputMode = "Append",
        timeMode = "None"
    )

async def process_stream():
    spark = query = None
    logger.info("Initiated Data Processing")
    try:
        spark, df = process_init()
        result_df = apply_inter_bar_states(df)
        
        query = result_df.writeStream \
            .outputMode("append") \
            .foreachBatch(write_batches) \
            .option("checkpointLocation", "tmp/chk/inter_bar") \
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