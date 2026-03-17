"""
Fetch complete trade history for each top wallet.
"""
import os
import time
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
from data.hl_client import (
    get_user_fills_by_time,
    get_user_funding_history,
    get_user_non_funding_ledger,
)
from config import HISTORY_DAYS, RAW_DIR, FILLS_DIR

logger = logging.getLogger(__name__)


def ms_ago(days: int) -> int:
    """Return Unix timestamp in ms for N days ago."""
    return int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)


def parse_fills(fills: list, address: str) -> pd.DataFrame:
    """
    Parse raw fills into a clean DataFrame.

    Fill structure:
    {
      "coin": "BTC",
      "px": "67500.0",
      "sz": "0.01",
      "side": "B" | "A",   (B=buy/long, A=ask/sell)
      "time": 1710000000000,
      "startPosition": "0.0",
      "dir": "Open Long" | "Close Long" | "Open Short" | "Close Short",
      "closedPnl": "0.0",
      "hash": "0x...",
      "oid": 123456,
      "crossed": true,
      "fee": "0.135",
      "liquidation": null | {...},
      "feeToken": "USDC",
      "builderFee": "0.0"
    }
    """
    if not fills:
        return pd.DataFrame()

    records = []
    for f in fills:
        try:
            records.append({
                "address":     address,
                "coin":        f.get("coin", ""),
                "side":        f.get("side", ""),           # B or A
                "direction":   f.get("dir", ""),            # Open Long / Close Short / etc.
                "price":       float(f.get("px", 0)),
                "size":        float(f.get("sz", 0)),
                "value_usd":   float(f.get("px", 0)) * float(f.get("sz", 0)),
                "closed_pnl":  float(f.get("closedPnl", 0)),
                "fee_usd":     float(f.get("fee", 0)),
                "time_ms":     int(f.get("time", 0)),
                "is_liquidation": f.get("liquidation") is not None,
                "crossed":     f.get("crossed", False),
            })
        except Exception as e:
            logger.debug(f"Error parsing fill: {e}")
            continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
    df["is_open"]  = df["direction"].str.contains("Open",  case=False, na=False)
    df["is_close"] = df["direction"].str.contains("Close", case=False, na=False)
    df["is_long"]  = df["direction"].str.contains("Long",  case=False, na=False)
    df["is_short"] = df["direction"].str.contains("Short", case=False, na=False)
    df = df.sort_values("time_ms").reset_index(drop=True)
    return df


def parse_funding_history(funding_data: list, address: str) -> pd.DataFrame:
    """Parse funding payment history."""
    if not funding_data:
        return pd.DataFrame()

    records = []
    for entry in funding_data:
        try:
            delta = entry.get("delta", {})
            records.append({
                "address":      address,
                "time_ms":      int(entry.get("time", 0)),
                "coin":         delta.get("coin", ""),
                "funding_usd":  float(delta.get("usdc", 0)),
                "side":         delta.get("type", ""),
            })
        except Exception:
            continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
    return df


def fetch_wallet_fills(address: str, days: int = HISTORY_DAYS) -> pd.DataFrame:
    """Fetch all fills for a wallet over the last N days."""
    start_ms = ms_ago(days)
    try:
        fills = get_user_fills_by_time(address, start_ms)
        df = parse_fills(fills, address)
        return df
    except Exception as e:
        logger.warning(f"Failed to fetch fills for {address[:8]}…: {e}")
        return pd.DataFrame()


def fetch_wallet_funding(address: str, days: int = HISTORY_DAYS) -> pd.DataFrame:
    """Fetch funding payment history for a wallet."""
    start_ms = ms_ago(days)
    try:
        data = get_user_funding_history(address, start_ms)
        return parse_funding_history(data, address)
    except Exception as e:
        logger.warning(f"Failed to fetch funding for {address[:8]}…: {e}")
        return pd.DataFrame()


def fetch_all_wallets(addresses: list) -> dict:
    """
    Fetch fills and funding for all wallets.
    Returns: {address: {"fills": df, "funding": df}}
    """
    os.makedirs(FILLS_DIR, exist_ok=True)
    results = {}

    for addr in tqdm(addresses, desc="Fetching wallet histories"):
        fills_path   = f"{FILLS_DIR}/{addr[:10]}_fills.csv"
        funding_path = f"{FILLS_DIR}/{addr[:10]}_funding.csv"

        # Load from cache if exists
        if os.path.exists(fills_path):
            fills_df   = pd.read_csv(fills_path, parse_dates=["time"])
            funding_df = pd.read_csv(funding_path, parse_dates=["time"]) if os.path.exists(funding_path) else pd.DataFrame()
        else:
            fills_df   = fetch_wallet_fills(addr)
            funding_df = fetch_wallet_funding(addr)

            if not fills_df.empty:
                fills_df.to_csv(fills_path, index=False)
            if not funding_df.empty:
                funding_df.to_csv(funding_path, index=False)

            time.sleep(0.1)  # rate limit

        results[addr] = {
            "fills":   fills_df,
            "funding": funding_df,
        }

    return results
