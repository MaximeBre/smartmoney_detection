"""
Fetch and cache OHLCV candle data from Hyperliquid.
Used by smart money analysis to compute forward returns.
"""
import os
import time
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from data.hl_client import _post
from config import CANDLES_DIR, CANDLE_INTERVAL

logger = logging.getLogger(__name__)


def fetch_candles(coin: str,
                  interval: str = "1h",
                  start_ms: int = None,
                  end_ms: int = None) -> pd.DataFrame:
    """
    Fetch OHLCV candles from Hyperliquid.

    Hyperliquid candle response fields:
      t = open time ms
      o, h, l, c = OHLC prices (strings)
      v = volume (string)
    """
    if start_ms is None:
        start_ms = int((datetime.utcnow() - timedelta(days=90)).timestamp() * 1000)
    if end_ms is None:
        end_ms = int(datetime.utcnow().timestamp() * 1000)

    payload = {
        "type": "candleSnapshot",
        "req": {
            "coin":      coin,
            "interval":  interval,
            "startTime": start_ms,
            "endTime":   end_ms,
        }
    }

    try:
        candles = _post(payload)
    except Exception as e:
        logger.warning(f"Failed to fetch candles for {coin}: {e}")
        return pd.DataFrame()

    if not candles:
        return pd.DataFrame()

    records = []
    for c in candles:
        try:
            records.append({
                "coin":      coin,
                "time_ms":   int(c.get("t", 0)),
                "open":      float(c.get("o", 0)),
                "high":      float(c.get("h", 0)),
                "low":       float(c.get("l", 0)),
                "close":     float(c.get("c", 0)),
                "volume":    float(c.get("v", 0)),
            })
        except Exception:
            continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
    df = df.sort_values("time_ms").reset_index(drop=True)
    return df


def load_candles_cached(coin: str,
                         interval: str = "1h",
                         days: int = 90) -> pd.DataFrame:
    """
    Load candles with file-based cache.
    Cache is per coin+interval, refreshed if >1h old.
    """
    os.makedirs(CANDLES_DIR, exist_ok=True)
    # Sanitize coin name for filename (replace : with _)
    safe_coin = coin.replace(":", "_")
    cache_path = f"{CANDLES_DIR}/{safe_coin}_{interval}.csv"

    # Check cache freshness
    if os.path.exists(cache_path):
        age_s = time.time() - os.path.getmtime(cache_path)
        if age_s < 3600:  # 1h cache
            df = pd.read_csv(cache_path, parse_dates=["time"])
            if not df.empty:
                return df

    start_ms = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)
    df = fetch_candles(coin, interval, start_ms)

    if not df.empty:
        df.to_csv(cache_path, index=False)

    return df


def get_price_at(coin: str,
                  target_ms: int,
                  candles_df: pd.DataFrame) -> float:
    """
    Get the closing price of a coin nearest to target_ms.
    Uses pre-loaded candles DataFrame for speed.
    """
    if candles_df.empty:
        return np.nan

    idx = (candles_df["time_ms"] - target_ms).abs().idxmin()
    diff_ms = abs(candles_df.loc[idx, "time_ms"] - target_ms)

    # Only use if within 2 candle periods
    candle_ms = 3600_000  # 1h in ms
    if diff_ms > 2 * candle_ms:
        return np.nan

    return float(candles_df.loc[idx, "close"])


def get_forward_price(coin: str,
                       entry_ms: int,
                       horizon_hours: int,
                       candles_df: pd.DataFrame) -> float:
    """
    Get closing price horizon_hours after entry_ms.
    """
    target_ms = entry_ms + horizon_hours * 3600_000
    return get_price_at(coin, target_ms, candles_df)


def prefetch_candles_for_coins(coins: list,
                                interval: str = "1h",
                                days: int = 90) -> dict:
    """
    Pre-fetch candles for all coins needed.
    Returns {coin: candles_df}
    """
    result = {}
    for coin in coins:
        df = load_candles_cached(coin, interval, days)
        result[coin] = df
        time.sleep(0.05)
    return result
