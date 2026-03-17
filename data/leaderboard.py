"""
Fetch Hyperliquid leaderboard and identify top traders.
"""
import os
import time
import logging
import pandas as pd
from tqdm import tqdm
from data.hl_client import get_leaderboard, get_clearinghouse_state
from config import TOP_N_WALLETS, MIN_PNL_USD, RAW_DIR, LEADERBOARD_CSV

logger = logging.getLogger(__name__)


def fetch_leaderboard_raw() -> list:
    """
    Fetch raw leaderboard data from Hyperliquid.
    Returns list of leaderboard entries.
    """
    logger.info("Fetching leaderboard from Hyperliquid…")
    data = get_leaderboard()

    # The leaderboard endpoint returns a dict with 'leaderboardRows'
    if isinstance(data, dict) and "leaderboardRows" in data:
        rows = data["leaderboardRows"]
    elif isinstance(data, list):
        rows = data
    else:
        logger.warning(f"Unexpected leaderboard format: {type(data)}")
        rows = []

    logger.info(f"  Got {len(rows)} leaderboard entries")
    return rows


def parse_leaderboard(rows: list) -> pd.DataFrame:
    """
    Parse raw leaderboard rows into a clean DataFrame.

    Hyperliquid leaderboard row structure:
    {
      "ethAddress": "0x...",
      "accountValue": "123456.78",
      "windowPerformances": [
        ["day", {"pnl": "...", "roi": "...", "vlm": "..."}],
        ["week", {...}],
        ["month", {...}],
        ["allTime", {...}]
      ],
      "prize": 0
    }
    """
    records = []
    for row in rows:
        try:
            address = row.get("ethAddress", "")
            if not address:
                continue

            account_value = float(row.get("accountValue", 0))

            # Parse windowPerformances
            perf = {}
            for window_entry in row.get("windowPerformances", []):
                if len(window_entry) == 2:
                    window_name, window_data = window_entry
                    perf[window_name] = {
                        "pnl": float(window_data.get("pnl", 0)),
                        "roi": float(window_data.get("roi", 0)),
                        "vlm": float(window_data.get("vlm", 0)),
                    }

            records.append({
                "address":           address,
                "display_name":      row.get("displayName", ""),
                "account_value_usd": account_value,
                "pnl_day":           perf.get("day",     {}).get("pnl", 0),
                "pnl_week":          perf.get("week",    {}).get("pnl", 0),
                "pnl_month":         perf.get("month",   {}).get("pnl", 0),
                "pnl_alltime":       perf.get("allTime", {}).get("pnl", 0),
                "roi_day":           perf.get("day",     {}).get("roi", 0),
                "roi_week":          perf.get("week",    {}).get("roi", 0),
                "roi_month":         perf.get("month",   {}).get("roi", 0),
                "roi_alltime":       perf.get("allTime", {}).get("roi", 0),
                "volume_day":        perf.get("day",     {}).get("vlm", 0),
                "volume_week":       perf.get("week",    {}).get("vlm", 0),
                "volume_month":      perf.get("month",   {}).get("vlm", 0),
            })
        except Exception as e:
            logger.debug(f"Skipping row due to parse error: {e}")
            continue

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df = df.sort_values("pnl_alltime", ascending=False).reset_index(drop=True)
    df["rank"] = df.index + 1
    return df


def filter_top_wallets(df: pd.DataFrame, n: int = TOP_N_WALLETS) -> pd.DataFrame:
    """
    Filter to top N wallets by all-time PnL with quality filters.
    """
    # Filter: minimum PnL
    df = df[df["pnl_alltime"] >= MIN_PNL_USD].copy()

    # Take top N
    df = df.head(n).copy()
    df["rank"] = range(1, len(df) + 1)

    logger.info(f"  Filtered to {len(df)} wallets (min PnL: ${MIN_PNL_USD:,.0f})")
    return df


def enrich_with_account_state(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich leaderboard with live account state (current positions, margin, leverage).
    """
    logger.info(f"  Enriching {len(df)} wallets with live account state…")

    extra_cols = {
        "current_margin_usd":     [],
        "current_unrealized_pnl": [],
        "n_open_positions":       [],
        "open_assets":            [],
        "max_leverage_open":      [],
    }

    for addr in tqdm(df["address"], desc="  Account states"):
        try:
            state = get_clearinghouse_state(addr)
            margin = float(state.get("marginSummary", {}).get("accountValue", 0))
            unrealized = float(state.get("marginSummary", {}).get("totalUnrealizedPnl", 0))
            positions = state.get("assetPositions", [])

            open_pos = [
                p for p in positions
                if float(p.get("position", {}).get("szi", 0)) != 0
            ]

            open_assets = [p["position"]["coin"] for p in open_pos]
            leverages   = [
                abs(float(p["position"].get("leverage", {}).get("value", 1)))
                for p in open_pos
            ]

            extra_cols["current_margin_usd"].append(margin)
            extra_cols["current_unrealized_pnl"].append(unrealized)
            extra_cols["n_open_positions"].append(len(open_pos))
            extra_cols["open_assets"].append(",".join(open_assets) if open_assets else "")
            extra_cols["max_leverage_open"].append(max(leverages) if leverages else 0)

            time.sleep(0.05)  # gentle rate limiting

        except Exception as e:
            logger.debug(f"Failed to get state for {addr}: {e}")
            for col in extra_cols:
                extra_cols[col].append(None)

    for col, values in extra_cols.items():
        df[col] = values

    return df


def run_leaderboard_fetch() -> pd.DataFrame:
    """
    Full pipeline: fetch → parse → filter → enrich → save.
    Returns enriched leaderboard DataFrame.
    """
    os.makedirs(RAW_DIR, exist_ok=True)

    rows = fetch_leaderboard_raw()
    df   = parse_leaderboard(rows)

    if df.empty:
        logger.error("Leaderboard empty — check API")
        return df

    df = filter_top_wallets(df)
    df = enrich_with_account_state(df)

    df.to_csv(LEADERBOARD_CSV, index=False)
    logger.info(f"  Saved leaderboard to {LEADERBOARD_CSV}")

    return df
