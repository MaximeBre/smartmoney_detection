"""
Comprehensive real-time market overview for Hyperliquid.
Funding rates, OI, recent large trades, market sentiment.
"""
import json
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from data.hl_client import (
    get_meta_and_asset_ctxs,
    get_all_mids,
    get_recent_trades,
    get_funding_history,
)
from config import TRACKED_ASSETS, MARKET_JSON

logger = logging.getLogger(__name__)


def get_market_snapshot() -> pd.DataFrame:
    """
    Full market snapshot: price, funding rate, OI, mark price for all assets.
    """
    try:
        result = get_meta_and_asset_ctxs()
        meta, asset_ctxs = result[0], result[1]
    except Exception as e:
        logger.error(f"Failed to get market data: {e}")
        return pd.DataFrame()

    mids = get_all_mids()

    universe = meta.get("universe", [])
    records  = []

    for i, asset in enumerate(universe):
        if i >= len(asset_ctxs):
            break

        ctx  = asset_ctxs[i]
        name = asset.get("name", "")

        try:
            funding_rate = float(ctx.get("funding",    0))
            open_int     = float(ctx.get("openInterest", 0))
            mark_px      = float(ctx.get("markPx",    0))
            mid_px       = float(mids.get(name, 0))

            # OI in USD
            oi_usd = open_int * mark_px if mark_px > 0 else 0

            # Annualised funding rate (3 payments/day × 365)
            funding_annual = funding_rate * 3 * 365

            records.append({
                "coin":             name,
                "mid_price":        mid_px,
                "mark_price":       mark_px,
                "funding_rate_8h":  funding_rate,
                "funding_annual":   funding_annual,
                "open_interest":    open_int,
                "oi_usd":           oi_usd,
                "funding_sentiment": (
                    "VERY_BULLISH" if funding_rate >  0.001 else
                    "BULLISH"      if funding_rate >  0.0002 else
                    "NEUTRAL"      if abs(funding_rate) <= 0.0002 else
                    "BEARISH"      if funding_rate > -0.001 else
                    "VERY_BEARISH"
                ),
            })
        except Exception as e:
            logger.debug(f"Error parsing asset {name}: {e}")
            continue

    df = pd.DataFrame(records)
    if not df.empty:
        df = df.sort_values("oi_usd", ascending=False).reset_index(drop=True)
    return df


def get_large_trades(coin: str, min_value_usd: float = 50_000) -> list:
    """
    Get recent large trades for a coin.
    Returns list of trades above min_value_usd.
    """
    try:
        trades = get_recent_trades(coin)
        large  = []
        for t in trades:
            try:
                px    = float(t.get("px", 0))
                sz    = float(t.get("sz", 0))
                value = px * sz
                if value >= min_value_usd:
                    large.append({
                        "coin":       coin,
                        "side":       t.get("side", ""),
                        "price":      px,
                        "size":       sz,
                        "value_usd":  value,
                        "time_ms":    t.get("time", 0),
                    })
            except Exception:
                continue
        return large
    except Exception as e:
        logger.debug(f"Failed to get trades for {coin}: {e}")
        return []


def get_funding_history_df(coin: str, days: int = 7) -> pd.DataFrame:
    """Get funding rate history for a coin as DataFrame."""
    start_ms = int((datetime.utcnow() - timedelta(days=days)).timestamp() * 1000)
    try:
        history = get_funding_history(coin, start_ms)
        records = []
        for entry in history:
            records.append({
                "coin":         coin,
                "time_ms":      int(entry.get("time",        0)),
                "funding_rate": float(entry.get("fundingRate", 0)),
            })
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df["time"] = pd.to_datetime(df["time_ms"], unit="ms", utc=True)
        return df.sort_values("time")
    except Exception as e:
        logger.debug(f"Funding history failed for {coin}: {e}")
        return pd.DataFrame()


def compute_market_sentiment(snapshot_df: pd.DataFrame) -> dict:
    """
    Compute overall market sentiment from funding rates.
    """
    if snapshot_df.empty:
        return {}

    tracked = snapshot_df[snapshot_df["coin"].isin(TRACKED_ASSETS)].copy()
    if tracked.empty:
        tracked = snapshot_df.copy()

    positive_funding = (tracked["funding_rate_8h"] > 0).sum()
    negative_funding = (tracked["funding_rate_8h"] < 0).sum()
    total            = len(tracked)

    avg_funding = float(tracked["funding_rate_8h"].mean())

    # OI-weighted funding rate
    if tracked["oi_usd"].sum() > 0:
        oi_weighted_funding = float(
            (tracked["funding_rate_8h"] * tracked["oi_usd"]).sum() /
            tracked["oi_usd"].sum()
        )
    else:
        oi_weighted_funding = avg_funding

    top_positive = tracked.nlargest(3, "funding_rate_8h")[["coin", "funding_rate_8h"]].to_dict("records")
    top_negative = tracked.nsmallest(3, "funding_rate_8h")[["coin", "funding_rate_8h"]].to_dict("records")

    return {
        "timestamp":             datetime.utcnow().isoformat(),
        "n_assets":              total,
        "n_positive_funding":    int(positive_funding),
        "n_negative_funding":    int(negative_funding),
        "pct_positive_funding":  float(positive_funding / max(total, 1)),
        "avg_funding_8h":        avg_funding,
        "oi_weighted_funding":   oi_weighted_funding,
        "funding_annual_pct":    oi_weighted_funding * 3 * 365 * 100,
        "market_regime":         (
            "RISK_ON"     if oi_weighted_funding >  0.001  else
            "BULLISH"     if oi_weighted_funding >  0.0002 else
            "NEUTRAL"     if abs(oi_weighted_funding) < 0.0002 else
            "BEARISH"     if oi_weighted_funding > -0.001  else
            "RISK_OFF"
        ),
        "top_positive_funding":  top_positive,
        "top_negative_funding":  top_negative,
        "total_oi_usd":          float(tracked["oi_usd"].sum()),
    }


def run_market_overview() -> dict:
    """
    Full market overview pipeline.
    """
    logger.info("Computing market overview…")

    snapshot  = get_market_snapshot()
    sentiment = compute_market_sentiment(snapshot)

    # Large trades for top assets
    large_trades = []
    top_assets   = list(snapshot.head(10)["coin"]) if not snapshot.empty else TRACKED_ASSETS[:5]
    for coin in top_assets:
        large_trades.extend(get_large_trades(coin))

    large_trades.sort(key=lambda x: x.get("value_usd", 0), reverse=True)

    result = {
        "sentiment":    sentiment,
        "snapshot":     snapshot.to_dict("records") if not snapshot.empty else [],
        "large_trades": large_trades[:50],
    }

    with open(MARKET_JSON, "w") as f:
        json.dump(result, f, indent=2, default=str)
    logger.info(f"  Saved market overview to {MARKET_JSON}")

    return result
