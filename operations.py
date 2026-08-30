import numpy as np
import sklearn 
import pandas as pd

from pyspark.sql.types import (
    TimestampType, StructType, StructField, StringType, DoubleType,
    LongType, BooleanType, IntegerType
)

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from parameters import MIN_ZSCORE, LOOKBACK_BARS

def decode_state(state):
    return [float(st) for st in state.split(",")] if state else []

def encode_state(state):
    return ",".join(f"{st:.10g}" for st in state)

def log_return(prev_close, close):
    r_t = 0
    if prev_close and prev_close > 0 and close > 0:
        r_t = float(np.log(close / prev_close))
    return r_t

def gap_percentage(prev_close, open): 
    gap_pct = 0
    if prev_close and prev_close > 0:
        gap_pct = (open - prev_close)/prev_close
    return gap_pct

def calc_zscore(returns, rt):
    z = 0
    if len(returns) >= MIN_ZSCORE:
        mu = float(np.mean(returns))
        sigma = float(np.std(returns)) + 1e-8
        z = (rt - mu)/sigma
    return z

def vol_shock_avg(volumes, volume):
    vol_shock = 1
    if(len(volumes) >= MIN_ZSCORE):
        avg_vol = float(np.mean(volumes)) + 1e-5
        vol_shock = volume / avg_vol
    return vol_shock

def no_reversed_dirs(rt, returns):
    direction = int(np.sign(rt))
    consec = 1
    for past_return in reversed(returns):
        if direction != 0 and int(np.sign(past_return)) == direction:
            consec += 1
        else:
            break
    if direction == 0:
        consec = 0
    return consec

def to_pdtypes(df, schema):
    for field in schema.fields:
        col_name = field.name
        if col_name in df.columns:
            if isinstance(field.dataType, (LongType, IntegerType)):
                df[col_name] = df[col_name].astype("int64")
            elif isinstance(field.dataType, DoubleType):
                df[col_name] = df[col_name].astype("float64")
            elif isinstance(field.dataType, BooleanType):
                df[col_name] = df[col_name].astype("bool")  
            elif isinstance(field.dataType, TimestampType):
                df[col_name] = pd.to_datetime(df[col_name])
            elif isinstance(field.dataType, StringType):
                df[col_name] = df[col_name].astype("str") 
    return df  

def get_returns(close, open, prev_close):
    prev = np.empty(len(close), dtype="float64")
    prev[0] = prev_close if prev_close is not None else np.nan
    prev[1:] = close[:-1]
    
    rt = np.zeros(len(close), dtype="float64")
    valid_return = ~np.isnan(prev) & (prev > 0) & (close > 0)
    rt[valid_return] = np.log(close[valid_return] / prev[valid_return])
    
    gap_pct = np.zeros(len(close), dtype="float64")
    valid_gap = ~np.isnan(prev) & (prev > 0)
    gap_pct[valid_gap] = (open[valid_gap] - prev[valid_gap]) / prev[valid_gap]
    
    return rt, gap_pct

def rolling_stats(total_returns, total_volumes):
    returns = pd.Series(total_returns, dtype="float64")
    volumes = pd.Series(total_volumes, dtype="float64")
    
    roll_ret_mean = returns.rolling(window=LOOKBACK_BARS, min_periods=1).mean().shift(1)
    roll_ret_std = returns.rolling(window=LOOKBACK_BARS, min_periods=1).std(ddof=0).shift(1)
    
    zscore = ((returns - roll_ret_mean)/(roll_ret_std + 1e-8)).fillna(0)
    
    roll_vol_mean = volumes.rolling(window=LOOKBACK_BARS, min_periods=1).mean().shift(1)
    
    vol_shock = (volumes / (roll_vol_mean + 1e-8)).fillna(0)
    
    return zscore.to_numpy(), vol_shock.to_numpy()
    
    
def consec_bars_dir(total_signs):
    non_zero = total_signs != 0
    change = np.empty(len(total_signs), dtype=bool)
    change[0] = True
    change[1:] = (total_signs[1:] != total_signs[:-1]) | (~non_zero[1:])
    change_id = np.cumsum(change)
    
    streak = pd.Series(np.arange(len(total_signs))).groupby(change_id).cumcount().to_numpy()+1
    streak[~non_zero] = 0
    return streak

def sign_flips(total_signs, window=10):
    diff_flip = np.zeros(len(total_signs), dtype="int64")
    if len(total_signs) > 1:
        same_sign = (total_signs[1:] != 0) & (total_signs[:-1] != 0)
        changed = total_signs[1:] != total_signs[:-1]
        diff_flip[1:] = (same_sign & changed).astype("int64")
    return pd.Series(diff_flip).rolling(window=window, min_periods=1).sum().to_numpy().astype("int64")

def halt_run_check(dead, dead_run):
    is_dead = dead.astype("int64")
    reset_id = (is_dead == 0).cumsum()
    halt = pd.Series(is_dead).groupby(reset_id).cumsum().to_numpy().copy()
    
    if dead_run > 0 and dead[0] == 1:
        halt[reset_id == reset_id[0]] += dead_run
    return halt

def write_batches(batch, batch_id):
    if batch.count() == 0: return
    logger.info(f"Batch {batch_id} being writting to parquet")
    batch.write \
        .mode("append") \
        .partitionBy("bar_end") \
        .parquet("stock_data/minute_streaming")

def debug_batches(batch, batch_id):
    logger.info(f"\n--- DATA FOR BATCH ID: {batch_id} ---: {batch.count()}")
    if not batch.isEmpty():
        # Prints formatted table directly to process stdout
        batch.show(20, truncate=False)
    else:
        print("Batch is empty.")