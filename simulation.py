import numpy as np
import pandas as pd
import sklearn 
from math import sqrt
from scipy.stats import norm

def simulate_price(start_price, n_days, mu, sigma, base_vol, inject_anomalies = None, seed = 42):
    rng = np.random.default_rng(seed)
    no_minutes = 390
    total_steps = n_days * no_minutes * 60
    delta = 1/(252 * no_minutes * 60)
    
    #brownian
    increments = norm.rvs(size=total_steps, scale=sqrt(delta,))
    log_returns = (mu - 0.5*sigma**2) * delta + sigma * increments
    raw_price = start_price * np.exp(np.cumsum(log_returns))
    
    #volume sim
    sigma_vol = 0.2
    raw_vol = np.random.lognormal(mean = np.log(base_vol), sigma = sigma_vol)
    mod_returns = np.abs(log_returns)
    vol_multiplier = mod_returns / np.mean(mod_returns)
    raw_volume = raw_vol * (0.4 + 0.6 * vol_multiplier)
    
    no_bars = len(raw_price) // 60
    trim_price = no_bars * 60
    price = raw_price[:trim_price].reshape(no_bars, 60)
    volume = raw_volume[:trim_price].reshape(no_bars, 60)
    
    raw_df = pd.DataFrame({
        "Price": raw_price, 
        "Volume": raw_volume
    })
    
    # df = pd.DataFrame({
    #     "Open": price[:, 0],
    #     "High": price.max(axis=1),
    #     "Low": price.min(axis=1),
    #     "Close": price[:, -1],
    #     "Volume": volume.sum(axis=1),
    #     "Day": np.repeat(np.arange(n_days), no_minutes)
    # })
    
    return raw_df

def shift_price(price, start, level):
    if start >= len(price):
        return
    prev = price[start]
    if prev == 0:
        return 
    price[start:] *= (level / prev)

def gap_anomaly(price, volume, start, pct=0.5):
    base = price[start]
    jump = base * (1+pct)
    shift_price(price, start, jump)
    volume[start] *= 8
    return dict(type="gap_spike", start_idx=start, end_idx=start+1)

def liquidity_halt(price, volume, start, duration=15):
    p = price
    end = start + duration
    p[start:end] = p[start]
    volume[start:end]*=0.02
    return dict(type="liquidity_halt", start_idx = start, end_idx= end)
    
def momentum_ignition(price, volume, start, duration=10, magnitude=0.05, reversal_fac=0.75):
    end = start + duration
    base = price[start]
    peak = base * (1+magnitude)
    price[start: end] = np.linspace(base, peak, duration)
    reversal_end = min(end + duration, len(price))
    target = peak - (peak-base)*reversal_fac
    price[end:reversal_end] = np.linspace(peak, target, reversal_end - end)
    shift_price(price, reversal_end, target)
    volume[start:end]*=4
    return dict(type="momentum_ignition", start_idx=start, end_idx=reversal_end)

def spoofing_oscillation(price, volume, start, duration=10, amplitude_pct=0.05, vol_mult=5):
    end = min(start+duration, len(price))
    base = price[start]
    for idx, trade in enumerate(range(start, end)):
        sign = 1 if idx % 2 else -1
        price[trade] = base * (1+sign*amplitude_pct)
    shift_price(price, end, base)
    volume[start: end] *= vol_mult
    return dict(type="spoofing_oscillation", start_idx = start, end_idx = end)

def closing_imbalance(price, volume, day_end, duration=10, magnitude=0.05, vol_mult=5):
    start = max(day_end-duration, 0)
    base = price[start]
    target = base * (1+magnitude)
    price[start: day_end] = np.linspace(base, target, day_end-start)
    shift_price(price, day_end, target)
    volume[start: day_end] *= vol_mult
    return dict(type="closing_balance", start_idx=start, end_idx=day_end)

def flash_crashes(price, volume, start, duration = 50, drop_pct =0.45, recover_by = 0.75):
    '''
    Price falls and recovers quickly within a few minutes
    '''
    p = price
    end = start + duration
    base = p[start]
    trough = base * (1-drop_pct)
    p[start:end] = np.linspace(base, trough, duration)
    recover_len = max(duration, 3)
    recover_end = min(end + recover_len, len(p))
    target = trough + (base - trough) * recover_by
    p[end:recover_end] = np.linspace(trough, target, recover_end - end)
    shift_price(p, recover_end, target)
    volume[start: recover_end] *= 6
    return dict(type="flash_crash", start_idx = start, end_idx = recover_end)

def flash_spike(price, volume, start, duration = 50, spike_pct =0.45, recover_by = 0.75):
    '''
    Price shoots up and recovers quickly within a few minutes
    '''
    p = price
    end = start + duration
    base = p[start]
    rise = base * (1+spike_pct)
    p[start:end] = np.linspace(base, rise, duration)
    recover_len = max(duration, 3)
    recover_end = min(end + recover_len, len(p))
    target = rise - (rise - base) * recover_by
    p[end:recover_end] = np.linspace(rise, target, recover_end - end)
    shift_price(p, recover_end, target)
    volume[start: recover_end] *= 6
    return dict(type="flash_spike", start_idx = start, end_idx = recover_end)


def fat_finger_trades(price, volume, start, pct=0.15):
    base = price[start]
    price[start] = base * (1 - pct) if start % 2 == 0 else base * (1+pct)
    if start + 1 < len(price):
        shift_price(price, start + 1, base)
    volume[start] *= 15
    return dict(type="fat finger", start_idx = start, end_idx = start+1)

INJECTORS = {
    "flash_crash": flash_crashes,
    "flash_spike": flash_spike,
    "gap": gap_anomaly,
    "liquidity_halt": liquidity_halt,
    "momentum_ignition": momentum_ignition,
    "spoofing_oscillation": spoofing_oscillation,
    "fat_finger": fat_finger_trades,
    "closing_imbalance": closing_imbalance,
}

def random_anomaly_injection(df, 
                             no_anomalies, 
                             anomaly_types,
                             min_gap, 
                             seed = 42):
    rng = np.random.default_rng(42)
    n = len(df)
    labels = []
    anomaly_present = np.zeros(n, dtype=bool)
    
    attempts = 0
    out = pd.DataFrame()
    price = df["Price"].to_numpy().copy()
    volume = df["Volume"].to_numpy().copy()
    while len(labels) < no_anomalies and attempts < no_anomalies * 30:
        attempts += 1
        anomaly_type = rng.choice(anomaly_types)
        start = int(rng.integers(20, n-60))
        window = slice(max(0, start-min_gap), min(n, start+min_gap))
        if anomaly_present[window].any():
            continue
        
        if anomaly_type == 'closing_imbalance':
            day = df["Day"][start]
            day_end = int(np.nonzero(df["Day"] == day)[0][-1])
            day_end = min(day_end, n)
            label = INJECTORS[anomaly_type](price, volume, day_end)
        else:
            label = INJECTORS[anomaly_type](price, volume, start)
        
        anomaly_present[max(0, label["start_idx"] - min_gap): min(n, label["end_idx"] + min_gap )] = True
        labels.append(label)
    labels.sort(key = lambda l: l["start_idx"])
    out = df.copy()
    out["Price"] = price
    out["Volume"] = volume

    return out, labels

def targeted_anomaly_injection(df, anomalies, seed = 42):
    price = df["Price"].to_numpy().copy()
    volume = df["Volume"].to_numpy().copy()
    
    labels = []
    
    for anomaly in anomalies:
        for func in anomalies[anomaly]:
            label = func(price, volume)
            labels.append(label)
            
    out = df.copy()
    out["Price"] = price
    out["Volume"] = volume
    return out, labels

import plotly.graph_objects as go
from plotly.subplots import make_subplots

ANOMALY_COLORS = {
    "flash_crashes": "red",
    "flash_spike": "green",
    "gap_anomaly": "purple",
    "liquidity_halt": "yellow",
    "momentum_ignition": "blue",
    "spoofing_oscillation": "magenta",
    "fat_finger_trades": "black",
    "closing_imbalance": "brown",
}
def plot_minute_series(df, labels, out_path="anomaly_chart_minute.html"):
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.7, 0.3]
    )
    fig.add_trace(
        go.Scatter(x=df.index, y=df["Price"], mode="lines", name="Price", line=dict(width=1)),
        row=1, col=1,
    )
    fig.add_trace(
        go.Bar(x=df.index, y=df["Volume"], name="Volume", marker_color="lightgray"),
        row=2, col=1,
    )

    for l in labels:
        color = ANOMALY_COLORS.get(l["type"], "black")
        fig.add_vrect(
            x0=l["start_idx"], x1=max(l["end_idx"], l["start_idx"] + 1),
            fillcolor=color, opacity=0.25, line_width=0,
            row=1, col=1,
        )
        fig.add_annotation(
            x=(l["start_idx"] + l["end_idx"]) / 2, y=1.02, yref="y domain",
            text=l["type"], showarrow=False, textangle=-90, font=dict(size=8, color=color),
            row=1, col=1,
        )

    fig.update_layout(title="Simulated Minute-Level Price with Injected Anomalies",
                       showlegend=False, height=650)
    fig.update_xaxes(title_text="Minute index", row=2, col=1)
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)
    fig.show()


def plot_ohlcv(df_bars, bar_labels, out_path="anomaly_chart_daily.html"):
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, row_heights=[0.7, 0.3]
    )
    fig.add_trace(
        go.Ohlc(x=df_bars.index, open=df_bars["Open"], high=df_bars["High"],
                low=df_bars["Low"], close=df_bars["Close"], name="Price"),
        row=1, col=1,
    )
    fig.add_trace(
        go.Bar(x=df_bars.index, y=df_bars["Volume"], name="Volume", marker_color="gray"),
        row=2, col=1,
    )
    print(bar_labels)
    for l in bar_labels:
        color = ANOMALY_COLORS.get(l["type"], "black")
        fig.add_vrect(
            x0=l["start_idx"] - 0.5, x1=l["end_idx"] + 0.5,
            fillcolor=color, opacity=0.25, line_width=0, row=1, col=1,
        )
    fig.update_layout(title="Daily OHLCV with Injected Anomaly Windows", showlegend=False, height=650)
    fig.update_xaxes(rangeslider_visible=False, row=1, col=1)
    fig.show()

raw_df, train_series = simulate_price(start_price=150, n_days=10, mu=0.08,
                                           sigma=0.35, base_vol=5000)

anomaly_lambdas = {
    "flash_crash": [
        lambda p, v: flash_crashes(p, v, start=15, duration=30, drop_pct=0.30),
        lambda p, v: flash_crashes(p, v, start=30, duration=60, drop_pct=0.55, recover_by=0.90)
    ],
    "flash_spike": [
        lambda p, v: flash_spike(p, v, start=1520, duration=10, spike_pct=0.20),
        lambda p, v: flash_spike(p, v, start=6942, duration=15, spike_pct=0.50, recover_by=0.60)
    ],
    "gap": [
        lambda p, v: gap_anomaly(p, v, start=69000, pct=0.10),
        lambda p, v: gap_anomaly(p, v, start=12457, pct=-0.15)  # Negative gap
    ],
    "liquidity_halt": [
        lambda p, v: liquidity_halt(p, v, start=142578, duration=15),
        lambda p, v: liquidity_halt(p, v, start=53, duration=45)
    ],
    "momentum_ignition": [
        lambda p, v: momentum_ignition(p, v, start=15000, duration=15, magnitude=0.08),
        lambda p, v: momentum_ignition(p, v, start=20000, duration=40, magnitude=0.12, reversal_fac=0.50)
    ],
    "spoofing_oscillation": [
        lambda p, v: spoofing_oscillation(p, v, start=100, duration=12, amplitude_pct=0.03),
        lambda p, v: spoofing_oscillation(p, v, start=23450, duration=30, amplitude_pct=0.07, vol_mult=8)
    ],
    "fat_finger": [
        lambda p, v: fat_finger_trades(p, v, start=5690, pct=0.05),
        lambda p, v: fat_finger_trades(p, v, start=45203, pct=0.25)
    ],
    "closing_imbalance": [
        # Note: closing_imbalance expects 'day_end' instead of 'start'
        lambda p, v: closing_imbalance(p, v, day_end=25130, duration=15, magnitude=0.04),
        lambda p, v: closing_imbalance(p, v, day_end=30000, duration=30, magnitude=0.09, vol_mult=10)
    ]
}

print(raw_df.head())
df, train_labels = random_anomaly_injection(raw_df, 8, list(INJECTORS.keys()), 30)
#df, train_labels = targeted_anomaly_injection(raw_df, anomaly_lambdas)
# df.to_csv("train_data1.csv")
plot_ohlcv(train_series, train_labels)