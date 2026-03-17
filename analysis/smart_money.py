"""
Smart Money Detection via Information Coefficient (IC) Analysis.

For each wallet:
1. Take all fills (entry trades)
2. Measure: what happened to price N hours after entry?
3. IC = mean(signed_forward_return) -> positive = systematically right
4. Rolling IC over time -> is the edge recent or historical?
5. Smart Money Score = weighted combination of IC metrics
"""
import os
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from scipy import stats
from data.price_data import prefetch_candles_for_coins, get_forward_price
from config import (
    IC_HORIZONS, IC_MIN_TRADES, IC_ROLLING_DAYS,
    IC_WEIGHT_RECENT, IC_WEIGHT_ALLTIME, IC_WEIGHT_ICIR, IC_WEIGHT_TREND,
    SMART_MONEY_CSV, CANDLE_INTERVAL, CANDLES_DIR,
)

logger = logging.getLogger(__name__)


def compute_forward_returns(fills_df: pd.DataFrame,
                             candles_dict: dict) -> pd.DataFrame:
    """
    For each OPENING fill, compute the forward return at each IC horizon.

    Returns fills_df enriched with columns:
      fwd_ret_1h, fwd_ret_4h, fwd_ret_8h, fwd_ret_24h
      signed_ret_1h, signed_ret_4h, ... (return x direction)
    """
    if fills_df.empty:
        return fills_df

    opens = fills_df[fills_df["is_open"]].copy()
    if opens.empty:
        return fills_df

    # Direction: +1 for Long, -1 for Short
    opens["direction"] = opens["is_long"].map({True: 1.0, False: -1.0})

    for h in IC_HORIZONS:
        fwd_col    = f"fwd_ret_{h}h"
        signed_col = f"signed_ret_{h}h"
        opens[fwd_col]    = np.nan
        opens[signed_col] = np.nan

    for idx, row in opens.iterrows():
        coin   = row["coin"]
        t_ms   = int(row["time_ms"])

        candles = candles_dict.get(coin, pd.DataFrame())
        if candles.empty:
            continue

        # Entry price: use fill price directly (more accurate than candle close)
        p_entry = row["price"]
        if p_entry <= 0:
            continue

        for h in IC_HORIZONS:
            p_fwd = get_forward_price(coin, t_ms, h, candles)
            if np.isnan(p_fwd) or p_entry == 0:
                continue

            fwd_ret    = (p_fwd - p_entry) / p_entry
            signed_ret = fwd_ret * row["direction"]

            opens.loc[idx, f"fwd_ret_{h}h"]    = fwd_ret
            opens.loc[idx, f"signed_ret_{h}h"] = signed_ret

    return opens


def compute_ic(returns_df: pd.DataFrame, horizon: int) -> float:
    """
    IC for a given horizon: mean of signed returns (t-stat normalised).
    Returns value between -1 and 1 roughly.
    """
    col = f"signed_ret_{horizon}h"
    if col not in returns_df.columns:
        return 0.0

    sr = returns_df[col].dropna()
    if len(sr) < IC_MIN_TRADES:
        return 0.0

    # Mean signed return (positive = systematically right)
    # Normalised by std for comparability across assets
    mean = sr.mean()
    std  = sr.std()
    if std == 0:
        return 0.0

    # Return Sharpe-style IC (mean/std -> unbounded, but intuitive)
    return float(mean / std)


def compute_rolling_ic(returns_df: pd.DataFrame,
                        horizon: int,
                        window_days: int = IC_ROLLING_DAYS) -> pd.Series:
    """
    Rolling IC over time windows.
    Returns a Series indexed by date with IC values.
    """
    col = f"signed_ret_{horizon}h"
    if col not in returns_df.columns or "time" not in returns_df.columns:
        return pd.Series(dtype=float)

    df = returns_df[["time", col]].dropna().copy()
    if len(df) < IC_MIN_TRADES:
        return pd.Series(dtype=float)

    df = df.set_index("time").sort_index()
    df.index = pd.to_datetime(df.index, utc=True)

    # Resample to daily, compute rolling mean IC
    window = f"{window_days}D"
    rolling_mean = df[col].rolling(window, min_periods=IC_MIN_TRADES // 2).mean()
    rolling_std  = df[col].rolling(window, min_periods=IC_MIN_TRADES // 2).std().replace(0, np.nan)
    rolling_ic   = rolling_mean / rolling_std

    return rolling_ic.dropna()


def compute_ic_trend(rolling_ic: pd.Series) -> float:
    """
    OLS slope of rolling IC over last 60 days -> is IC improving?
    Positive = improving edge, Negative = deteriorating.
    """
    if len(rolling_ic) < 5:
        return 0.0

    recent = rolling_ic.iloc[-min(60, len(rolling_ic)):]
    if len(recent) < 3:
        return 0.0

    x = np.arange(len(recent))
    slope, _, _, _, _ = stats.linregress(x, recent.values)
    return float(slope)


def compute_smart_money_score(ic_alltime: float,
                               ic_recent: float,
                               icir: float,
                               ic_trend: float) -> float:
    """
    Weighted smart money score.
    All inputs normalised to roughly [-1, 1].
    """
    # Clip extreme values
    ic_alltime = np.clip(ic_alltime, -2, 2)
    ic_recent  = np.clip(ic_recent,  -2, 2)
    icir       = np.clip(icir,       -3, 3)
    ic_trend   = np.clip(ic_trend * 100, -1, 1)  # scale slope

    score = (
        IC_WEIGHT_ALLTIME * ic_alltime +
        IC_WEIGHT_RECENT  * ic_recent  +
        IC_WEIGHT_ICIR    * (icir / 3) +  # normalise ICIR to ~[-1,1]
        IC_WEIGHT_TREND   * ic_trend
    )
    return float(score)


def score_wallet(address: str,
                 fills_df: pd.DataFrame,
                 candles_dict: dict) -> dict:
    """
    Full smart money scoring for a single wallet.
    """
    result = {
        "address":         address,
        "n_scored_trades": 0,
        "smart_money_score": 0.0,
        "grade": "UNKNOWN",
    }

    if fills_df.empty:
        return result

    # Compute forward returns
    enriched = compute_forward_returns(fills_df, candles_dict)
    if enriched.empty:
        return result

    n_trades = enriched[
        [f"signed_ret_{h}h" for h in IC_HORIZONS if f"signed_ret_{h}h" in enriched.columns]
    ].notna().any(axis=1).sum()
    result["n_scored_trades"] = int(n_trades)

    if n_trades < IC_MIN_TRADES:
        result["grade"] = "INSUFFICIENT_DATA"
        return result

    # IC per horizon
    primary_h = 8  # 8h is primary for HL funding-aligned

    ic_scores = {}
    for h in IC_HORIZONS:
        ic_scores[f"ic_{h}h"] = compute_ic(enriched, h)

    # Rolling IC for primary horizon
    rolling = compute_rolling_ic(enriched, primary_h)

    # Recent IC (last 30 days)
    if not rolling.empty:
        recent_cutoff = pd.Timestamp.utcnow() - pd.Timedelta(days=30)
        recent_ic_series = rolling[rolling.index >= recent_cutoff]
        ic_recent = float(recent_ic_series.mean()) if len(recent_ic_series) > 0 else ic_scores[f"ic_{primary_h}h"]
        icir = float(rolling.mean() / rolling.std()) if rolling.std() > 0 else 0.0
        ic_trend = compute_ic_trend(rolling)
    else:
        ic_recent = ic_scores[f"ic_{primary_h}h"]
        icir      = 0.0
        ic_trend  = 0.0

    # Final score
    score = compute_smart_money_score(
        ic_alltime = ic_scores[f"ic_{primary_h}h"],
        ic_recent  = ic_recent,
        icir       = icir,
        ic_trend   = ic_trend,
    )

    # Grade
    grade = (
        "TIER_1"   if score > 0.40 else
        "TIER_2"   if score > 0.25 else
        "TIER_3"   if score > 0.15 else
        "MARGINAL" if score > 0.05 else
        "NO_EDGE"
    )

    result.update({
        "smart_money_score":  round(score, 4),
        "grade":              grade,
        "ic_1h":              round(ic_scores.get("ic_1h",  0), 4),
        "ic_4h":              round(ic_scores.get("ic_4h",  0), 4),
        "ic_8h":              round(ic_scores.get("ic_8h",  0), 4),
        "ic_24h":             round(ic_scores.get("ic_24h", 0), 4),
        "ic_recent_8h":       round(ic_recent, 4),
        "icir_8h":            round(icir,      4),
        "ic_trend":           round(ic_trend,  6),
        "ic_trend_direction": "IMPROVING" if ic_trend > 0 else "DETERIORATING",
        "rolling_ic_values":  rolling.tail(10).tolist() if not rolling.empty else [],
    })

    return result


def run_smart_money_scoring(leaderboard_df: pd.DataFrame,
                             wallet_data: dict) -> pd.DataFrame:
    """
    Score all wallets for smart money characteristics.
    Returns DataFrame sorted by smart_money_score descending.
    """
    logger.info("Running Smart Money IC Analysis...")

    # Collect coins by trade frequency — only fetch top 40 most-traded
    coin_counts: dict = {}
    for addr, data in wallet_data.items():
        fills = data.get("fills", pd.DataFrame())
        if not fills.empty and "coin" in fills.columns:
            for coin in fills["coin"].unique():
                coin_counts[coin] = coin_counts.get(coin, 0) + len(fills[fills["coin"] == coin])

    top_coins = sorted(coin_counts, key=coin_counts.get, reverse=True)[:40]
    logger.info(f"  Pre-fetching candles for top {len(top_coins)} coins…")
    os.makedirs(CANDLES_DIR, exist_ok=True)
    candles_dict = prefetch_candles_for_coins(top_coins)

    # Score each wallet
    scores = []
    for addr, data in wallet_data.items():
        fills = data.get("fills", pd.DataFrame())
        score = score_wallet(addr, fills, candles_dict)

        # Merge with leaderboard data
        lb_row = leaderboard_df[leaderboard_df["address"] == addr]
        if not lb_row.empty:
            score["display_name"] = lb_row.iloc[0].get("display_name", "")
            score["pnl_alltime"]  = lb_row.iloc[0].get("pnl_alltime",  0)
            score["pnl_month"]    = lb_row.iloc[0].get("pnl_month",    0)
            score["rank"]         = lb_row.iloc[0].get("rank",         0)

        scores.append(score)

    df = pd.DataFrame(scores)
    df = df.sort_values("smart_money_score", ascending=False).reset_index(drop=True)
    df["smart_money_rank"] = df.index + 1

    os.makedirs("outputs/raw", exist_ok=True)
    df.to_csv(SMART_MONEY_CSV, index=False)
    logger.info(f"  Saved smart money scores to {SMART_MONEY_CSV}")

    return df
