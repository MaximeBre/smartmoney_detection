"""
What separates the best traders from the rest?
Cross-sectional pattern analysis across all profiles.
"""
import logging
import numpy as np
import pandas as pd
from scipy import stats
from config import PATTERNS_CSV

logger = logging.getLogger(__name__)


def segment_traders(profiles_df: pd.DataFrame) -> pd.DataFrame:
    """
    Segment traders into tiers based on all-time PnL.
    """
    df = profiles_df[profiles_df["sufficient_data"] == True].copy()
    if df.empty:
        return df

    pnl_col = "pnl_alltime" if "pnl_alltime" in df.columns else "net_pnl"

    p33 = df[pnl_col].quantile(0.33)
    p66 = df[pnl_col].quantile(0.66)

    df["tier"] = pd.cut(
        df[pnl_col],
        bins   = [-np.inf, p33, p66, np.inf],
        labels = ["bottom_tier", "mid_tier", "top_tier"]
    )
    return df


def compare_tiers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compare key metrics between top tier vs bottom tier.
    Returns a DataFrame with metric comparisons.
    """
    numeric_cols = [
        "win_rate", "profit_factor", "sharpe", "max_drawdown",
        "avg_hold_hours", "trades_per_day", "n_liquidations",
        "liquidation_rate", "long_pct", "asset_hhi",
        "avg_trade_value", "total_fees_paid", "n_assets_traded",
    ]

    existing_cols = [c for c in numeric_cols if c in df.columns]

    records = []
    for col in existing_cols:
        top    = df[df["tier"] == "top_tier"][col].dropna()
        bottom = df[df["tier"] == "bottom_tier"][col].dropna()

        if len(top) < 2 or len(bottom) < 2:
            continue

        t_stat, p_val = stats.ttest_ind(top, bottom)

        records.append({
            "metric":           col,
            "top_tier_mean":    round(top.mean(),    4),
            "top_tier_median":  round(top.median(),  4),
            "bottom_tier_mean": round(bottom.mean(), 4),
            "bottom_tier_median": round(bottom.median(), 4),
            "difference":       round(top.mean() - bottom.mean(), 4),
            "pct_difference":   round((top.mean() - bottom.mean()) / max(abs(bottom.mean()), 1e-9) * 100, 1),
            "p_value":          round(p_val, 4),
            "significant":      p_val < 0.05,
        })

    comparison_df = pd.DataFrame(records)
    if not comparison_df.empty:
        comparison_df = comparison_df.sort_values("p_value")
    return comparison_df


def asset_heatmap(wallet_data: dict,
                  profiles_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a matrix: wallet × asset → closed PnL.
    Shows what each trader specialises in.
    """
    records = []

    for addr, data in wallet_data.items():
        fills = data.get("fills", pd.DataFrame())
        if fills.empty:
            continue

        closed = fills[fills["is_close"]]
        if closed.empty:
            continue

        asset_pnl = closed.groupby("coin")["closed_pnl"].sum()
        for coin, pnl in asset_pnl.items():
            records.append({"address": addr, "coin": coin, "pnl": pnl})

    if not records:
        return pd.DataFrame()

    long_df = pd.DataFrame(records)
    matrix  = long_df.pivot_table(
        index="address", columns="coin", values="pnl", aggfunc="sum", fill_value=0
    )
    return matrix


def time_of_day_analysis(wallet_data: dict) -> pd.DataFrame:
    """
    When are the most profitable trades placed?
    Returns hour × metric DataFrame.
    """
    all_fills = []
    for addr, data in wallet_data.items():
        fills = data.get("fills", pd.DataFrame())
        if not fills.empty and "time" in fills.columns:
            all_fills.append(fills)

    if not all_fills:
        return pd.DataFrame()

    df = pd.concat(all_fills, ignore_index=True)
    df = df[df["is_close"]].copy()
    if df.empty:
        return pd.DataFrame()

    df["hour"] = df["time"].dt.hour

    hourly = df.groupby("hour").agg(
        n_trades        = ("closed_pnl", "count"),
        avg_pnl         = ("closed_pnl", "mean"),
        total_pnl       = ("closed_pnl", "sum"),
        win_rate        = ("closed_pnl", lambda x: (x > 0).mean()),
    ).reset_index()

    return hourly


def holding_time_analysis(wallet_data: dict) -> pd.DataFrame:
    """Distribution of holding times among top traders."""
    all_fills = []
    for addr, data in wallet_data.items():
        fills = data.get("fills", pd.DataFrame())
        if not fills.empty:
            all_fills.append(fills)

    if not all_fills:
        return pd.DataFrame()

    df = pd.concat(all_fills, ignore_index=True)

    hold_times = []
    for (addr, coin), group in df.groupby(["address", "coin"]):
        group = group.sort_values("time_ms")
        opens  = group[group["is_open"]].copy()
        closes = group[group["is_close"]].copy()

        open_queue = list(opens[["time_ms", "closed_pnl"]].itertuples(index=False))
        for _, row in closes.iterrows():
            if open_queue:
                open_item = open_queue.pop(0)
                hold_h    = (row["time_ms"] - open_item[0]) / (1000 * 3600)
                if 0 < hold_h < 24 * 90:
                    hold_times.append({
                        "hold_hours": hold_h,
                        "closed_pnl": row["closed_pnl"],
                        "coin": coin,
                    })

    return pd.DataFrame(hold_times) if hold_times else pd.DataFrame()


def run_pattern_analysis(profiles_df: pd.DataFrame,
                          wallet_data: dict) -> dict:
    """
    Master pattern analysis pipeline.
    Returns dict with all analysis results.
    """
    logger.info("Running pattern analysis…")

    df_segmented  = segment_traders(profiles_df)
    tier_compare  = compare_tiers(df_segmented) if not df_segmented.empty else pd.DataFrame()
    asset_matrix  = asset_heatmap(wallet_data, profiles_df)
    hourly        = time_of_day_analysis(wallet_data)
    hold_dist     = holding_time_analysis(wallet_data)

    # Save patterns summary
    if not tier_compare.empty:
        tier_compare.to_csv(PATTERNS_CSV, index=False)
        logger.info(f"  Saved patterns to {PATTERNS_CSV}")

    # Key findings
    findings = []
    if not tier_compare.empty:
        sig = tier_compare[tier_compare["significant"]]
        for _, row in sig.iterrows():
            direction = "higher" if row["difference"] > 0 else "lower"
            findings.append(
                f"Top traders have {direction} {row['metric']} "
                f"(top: {row['top_tier_mean']:.3f} vs bottom: {row['bottom_tier_mean']:.3f}, "
                f"p={row['p_value']:.3f})"
            )

    return {
        "segmented_profiles": df_segmented,
        "tier_comparison":    tier_compare,
        "asset_matrix":       asset_matrix,
        "hourly_pnl":         hourly,
        "hold_distribution":  hold_dist,
        "key_findings":       findings,
    }
