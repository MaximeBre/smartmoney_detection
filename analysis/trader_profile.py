"""
Build comprehensive profiles for each tracked trader.
"""
import logging
import numpy as np
import pandas as pd
from scipy import stats
from config import MIN_TRADES, PROFILES_CSV

logger = logging.getLogger(__name__)


def _sharpe(returns: pd.Series, periods_per_year: int = 365) -> float:
    """Annualised Sharpe (daily returns assumed)."""
    if len(returns) < 5 or returns.std() == 0:
        return 0.0
    return (returns.mean() / returns.std()) * np.sqrt(periods_per_year)


def _max_drawdown(cum_returns: pd.Series) -> float:
    """Maximum drawdown from a cumulative return series."""
    peak = cum_returns.cummax()
    dd   = (cum_returns - peak) / peak.replace(0, np.nan)
    return float(dd.min()) if len(dd) > 0 else 0.0


def _avg_hold_time(fills_df: pd.DataFrame) -> float:
    """
    Estimate average hold time in hours by pairing opens with closes
    for each (address, coin) combination.
    """
    if fills_df.empty:
        return 0.0

    hold_times = []

    for coin, coin_fills in fills_df.groupby("coin"):
        coin_fills = coin_fills.sort_values("time_ms").copy()
        opens  = coin_fills[coin_fills["is_open"]].copy()
        closes = coin_fills[coin_fills["is_close"]].copy()

        # Simple FIFO matching
        open_queue = list(opens["time_ms"].values)
        for _, close_row in closes.iterrows():
            if open_queue:
                open_ms  = open_queue.pop(0)
                close_ms = close_row["time_ms"]
                hold_h   = (close_ms - open_ms) / (1000 * 3600)
                if 0 < hold_h < 24 * 90:  # sanity: 0h–90d
                    hold_times.append(hold_h)

    return float(np.median(hold_times)) if hold_times else 0.0


def build_trader_profile(address: str,
                          fills_df: pd.DataFrame,
                          funding_df: pd.DataFrame,
                          leaderboard_row: pd.Series = None) -> dict:
    """
    Build a comprehensive profile dict for a single trader.
    """
    profile = {
        "address":              address,
        "address_short":        address[:6] + "…" + address[-4:],
        # — will be filled below —
    }

    # Leaderboard data
    if leaderboard_row is not None:
        profile["rank"]             = int(leaderboard_row.get("rank", 0))
        profile["account_value"]    = float(leaderboard_row.get("account_value_usd", 0))
        profile["pnl_alltime"]      = float(leaderboard_row.get("pnl_alltime", 0))
        profile["pnl_month"]        = float(leaderboard_row.get("pnl_month", 0))
        profile["pnl_week"]         = float(leaderboard_row.get("pnl_week", 0))
        profile["roi_alltime"]      = float(leaderboard_row.get("roi_alltime", 0))
        profile["roi_month"]        = float(leaderboard_row.get("roi_month", 0))
        profile["volume_month"]     = float(leaderboard_row.get("volume_month", 0))
        profile["n_open_positions"] = int(leaderboard_row.get("n_open_positions", 0) or 0)
        profile["open_assets"]      = str(leaderboard_row.get("open_assets", ""))
        profile["max_leverage"]     = float(leaderboard_row.get("max_leverage_open", 0) or 0)

    # ── Fill-based Metrics ───────────────────────────────────────────────────

    if fills_df.empty or len(fills_df) < MIN_TRADES:
        profile["sufficient_data"] = False
        return profile

    profile["sufficient_data"] = True
    profile["total_trades"]    = len(fills_df)
    profile["n_assets_traded"] = int(fills_df["coin"].nunique())

    # Closed PnL
    closed_trades = fills_df[fills_df["is_close"]].copy()
    profile["total_closed_pnl"]   = float(fills_df["closed_pnl"].sum())
    profile["total_fees_paid"]    = float(fills_df["fee_usd"].sum())
    profile["net_pnl"]            = profile["total_closed_pnl"] - profile["total_fees_paid"]

    # Win Rate (on closing trades)
    if len(closed_trades) > 0:
        wins              = (closed_trades["closed_pnl"] > 0).sum()
        profile["win_rate"]   = float(wins / len(closed_trades))
        profile["n_wins"]     = int(wins)
        profile["n_losses"]   = int(len(closed_trades) - wins)

        profile["avg_win"]    = float(closed_trades.loc[closed_trades["closed_pnl"] > 0, "closed_pnl"].mean() or 0)
        profile["avg_loss"]   = float(closed_trades.loc[closed_trades["closed_pnl"] < 0, "closed_pnl"].mean() or 0)
        profile["profit_factor"] = (
            abs(profile["avg_win"] * profile["n_wins"]) /
            max(abs(profile["avg_loss"] * profile["n_losses"]), 1e-9)
        )
    else:
        profile["win_rate"] = 0.0
        profile["profit_factor"] = 0.0

    # Liquidations
    profile["n_liquidations"]   = int(fills_df["is_liquidation"].sum())
    profile["liquidation_rate"] = float(profile["n_liquidations"] / max(len(closed_trades), 1))

    # Favourite Assets
    asset_pnl = (
        closed_trades.groupby("coin")["closed_pnl"]
        .agg(["sum", "count"])
        .sort_values("sum", ascending=False)
    )
    profile["top_asset_1"]   = asset_pnl.index[0]  if len(asset_pnl) > 0 else ""
    profile["top_asset_2"]   = asset_pnl.index[1]  if len(asset_pnl) > 1 else ""
    profile["top_asset_3"]   = asset_pnl.index[2]  if len(asset_pnl) > 2 else ""

    # Asset Concentration (HHI)
    asset_counts  = fills_df["coin"].value_counts(normalize=True)
    profile["asset_hhi"]      = float((asset_counts ** 2).sum())   # 1.0 = only one asset
    profile["is_concentrated"] = profile["asset_hhi"] > 0.5

    # Long vs Short Bias
    long_trades  = fills_df[fills_df["is_long"]]
    short_trades = fills_df[fills_df["is_short"]]
    total        = len(fills_df)
    profile["long_pct"]  = float(len(long_trades)  / max(total, 1))
    profile["short_pct"] = float(len(short_trades) / max(total, 1))
    profile["bias"]      = "LONG" if profile["long_pct"] > 0.6 else "SHORT" if profile["short_pct"] > 0.6 else "NEUTRAL"

    # Average Trade Size
    profile["avg_trade_value"]  = float(fills_df["value_usd"].mean())
    profile["median_trade_value"] = float(fills_df["value_usd"].median())
    profile["max_trade_value"]  = float(fills_df["value_usd"].max())

    # Hold Time
    profile["avg_hold_hours"]    = _avg_hold_time(fills_df)
    profile["hold_style"]        = (
        "SCALPER"   if profile["avg_hold_hours"] < 2    else
        "INTRADAY"  if profile["avg_hold_hours"] < 12   else
        "SWING"     if profile["avg_hold_hours"] < 72   else
        "POSITION"
    )

    # Ensure time column is datetime
    if "time" in fills_df.columns:
        fills_df["time"] = pd.to_datetime(fills_df["time"], utc=True, errors="coerce")

    # Trading Frequency
    if "time" in fills_df.columns and len(fills_df) > 1:
        days_active = (fills_df["time"].max() - fills_df["time"].min()).days + 1
        profile["trades_per_day"] = float(len(fills_df) / max(days_active, 1))
        profile["days_active"]    = int(days_active)
    else:
        profile["trades_per_day"] = 0.0
        profile["days_active"]    = 0

    # Daily PnL Series → Sharpe, Drawdown
    if "time" in fills_df.columns and not closed_trades.empty:
        ct = closed_trades.copy()
        ct["time"] = pd.to_datetime(ct["time"], utc=True, errors="coerce")
        ct = ct.dropna(subset=["time"])
        daily_pnl = (
            ct.set_index("time")["closed_pnl"]
            .resample("1D")
            .sum()
        )
        if len(daily_pnl) > 5:
            profile["sharpe"]       = _sharpe(daily_pnl)
            cum = daily_pnl.cumsum()
            profile["max_drawdown"] = _max_drawdown(cum)
        else:
            profile["sharpe"]       = 0.0
            profile["max_drawdown"] = 0.0
    else:
        profile["sharpe"]       = 0.0
        profile["max_drawdown"] = 0.0

    # Time-of-day pattern: when does this trader trade?
    if "time" in fills_df.columns:
        fills_df["hour"] = fills_df["time"].dt.hour
        hour_counts = fills_df["hour"].value_counts().sort_index()
        peak_hour   = int(hour_counts.idxmax()) if not hour_counts.empty else -1
        profile["peak_hour_utc"] = peak_hour

    # Funding PnL
    if not funding_df.empty:
        profile["total_funding_pnl"] = float(funding_df["funding_usd"].sum())
        profile["funding_pnl_pct"]   = (
            profile["total_funding_pnl"] /
            max(abs(profile["net_pnl"]), 1e-9)
        )
    else:
        profile["total_funding_pnl"] = 0.0
        profile["funding_pnl_pct"]   = 0.0

    return profile


def build_all_profiles(leaderboard_df: pd.DataFrame,
                        wallet_data: dict) -> pd.DataFrame:
    """
    Build profiles for all tracked wallets.
    Returns DataFrame with one row per wallet.
    """
    profiles = []

    for _, row in leaderboard_df.iterrows():
        addr = row["address"]
        data = wallet_data.get(addr, {})

        profile = build_trader_profile(
            address        = addr,
            fills_df       = data.get("fills",   pd.DataFrame()),
            funding_df     = data.get("funding", pd.DataFrame()),
            leaderboard_row = row,
        )
        profiles.append(profile)

    df = pd.DataFrame(profiles)
    df.to_csv(PROFILES_CSV, index=False)
    logger.info(f"Saved {len(df)} trader profiles to {PROFILES_CSV}")
    return df
