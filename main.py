"""
Hyperliquid Smart Money Tracker — Main Pipeline

Steps:
  1. Fetch leaderboard + top wallets
  2. Fetch trade histories (cached)
  3. Build trader profiles
  4. Smart Money IC Scoring
  5. Live position monitor + signal generation
  6. Pattern analysis
  7. Market overview
  8. Generate dashboard
"""
import os
import sys
import logging
import pandas as pd
from config import OUTPUT_DIR, RAW_DIR, FILLS_DIR, STATE_DIR, CANDLES_DIR

logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s %(message)s",
    datefmt = "%H:%M:%S",
)
logger = logging.getLogger(__name__)


def print_section(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def main():
    for d in [OUTPUT_DIR, RAW_DIR, FILLS_DIR, STATE_DIR, CANDLES_DIR]:
        os.makedirs(d, exist_ok=True)

    # ── STEP 1: Leaderboard ──────────────────────────────────────────────────
    print_section("STEP 1 — Fetching Leaderboard")
    from data.leaderboard import run_leaderboard_fetch
    leaderboard_df = run_leaderboard_fetch()

    if leaderboard_df.empty:
        logger.error("Leaderboard empty. Aborting.")
        sys.exit(1)

    if "address_short" not in leaderboard_df.columns:
        leaderboard_df["address_short"] = leaderboard_df["address"].str[:8] + "…"

    print(f"\n  Top 10 Wallets by All-Time PnL:")
    display = leaderboard_df.head(10)[["rank", "address_short", "pnl_alltime", "pnl_month", "roi_alltime"]].copy()
    display["pnl_alltime"] = display["pnl_alltime"].map("${:,.0f}".format)
    display["pnl_month"]   = display["pnl_month"].map("${:,.0f}".format)
    display["roi_alltime"] = display["roi_alltime"].map("{:.1%}".format)
    print(display.to_string(index=False))

    # ── STEP 2: Wallet Histories ─────────────────────────────────────────────
    print_section("STEP 2 — Fetching Wallet Trade Histories")
    from data.wallet_history import fetch_all_wallets
    addresses   = leaderboard_df["address"].tolist()
    wallet_data = fetch_all_wallets(addresses)
    n_with_data = sum(1 for d in wallet_data.values() if not d["fills"].empty)
    print(f"\n  {n_with_data}/{len(addresses)} wallets had trade data")

    # ── STEP 3: Trader Profiles ──────────────────────────────────────────────
    print_section("STEP 3 — Building Trader Profiles")
    from analysis.trader_profile import build_all_profiles
    profiles_df = build_all_profiles(leaderboard_df, wallet_data)
    valid = profiles_df[profiles_df.get("sufficient_data", pd.Series([False]*len(profiles_df))) == True]
    print(f"\n  {len(valid)}/{len(profiles_df)} wallets had sufficient data")

    # ── STEP 4: Smart Money Scoring ──────────────────────────────────────────
    print_section("STEP 4 — Smart Money IC Analysis")
    from analysis.smart_money import run_smart_money_scoring
    smart_money_df = run_smart_money_scoring(leaderboard_df, wallet_data)

    tier1 = smart_money_df[smart_money_df["grade"] == "TIER_1"]
    tier2 = smart_money_df[smart_money_df["grade"] == "TIER_2"]
    tier3 = smart_money_df[smart_money_df["grade"] == "TIER_3"]

    print(f"\n  Smart Money Tiers:")
    print(f"    TIER_1 (score >0.40):  {len(tier1)} wallets")
    print(f"    TIER_2 (score >0.25):  {len(tier2)} wallets")
    print(f"    TIER_3 (score >0.15):  {len(tier3)} wallets")

    if not smart_money_df.empty:
        print(f"\n  Top 10 Smart Money Wallets:")
        cols = ["smart_money_rank", "address", "display_name", "grade",
                "smart_money_score", "ic_8h", "ic_recent_8h", "icir_8h",
                "ic_trend_direction", "pnl_alltime"]
        show = [c for c in cols if c in smart_money_df.columns]
        top10 = smart_money_df.head(10)[show].copy()
        top10["address"] = top10["address"].str[:8]
        if "pnl_alltime" in top10:
            top10["pnl_alltime"] = top10["pnl_alltime"].map("${:,.0f}".format)
        print(top10.to_string(index=False))

    # ── STEP 5: Live Monitor + Signals ───────────────────────────────────────
    print_section("STEP 5 — Live Position Monitor + Signal Generation")
    from analysis.live_monitor import run_live_monitor
    from analysis.signals import run_signal_pipeline

    events, current_state = run_live_monitor(smart_money_df)
    signal_output = run_signal_pipeline(events, current_state)

    signals   = signal_output["signals"]
    consensus = signal_output["consensus"]

    if signals:
        print(f"\n  {len(signals)} NEW SIGNALS:")
        for s in signals:
            print(f"    [{s['signal_strength']}] {s['direction']} {s['coin']} "
                  f"@ ${s['entry_price']:,.2f} "
                  f"— {s['consensus_wallets']} wallets, confidence {s['consensus_confidence']:.2f}")
    else:
        print(f"\n  No new signals this run")

    strong_consensus = [c for c in consensus if c["is_consensus"]]
    if strong_consensus:
        print(f"\n  Current Smart Money Consensus ({len(strong_consensus)} positions):")
        for c in strong_consensus[:10]:
            wallets_str = ", ".join(c["wallets"][:3])
            print(f"    [{c['consensus_strength']}] {c['direction']} {c['coin']} "
                  f"— {c['n_wallets']} wallets ({wallets_str})")

    # ── STEP 6: Pattern Analysis ─────────────────────────────────────────────
    print_section("STEP 6 — Pattern Analysis")
    from analysis.pattern_analysis import run_pattern_analysis
    patterns = run_pattern_analysis(profiles_df, wallet_data)

    if patterns["key_findings"]:
        print("\n  Key Findings:")
        for f in patterns["key_findings"]:
            print(f"  → {f}")

    # ── STEP 7: Market Overview ──────────────────────────────────────────────
    print_section("STEP 7 — Market Overview")
    from analysis.market_overview import run_market_overview
    market = run_market_overview()

    sentiment = market.get("sentiment", {})
    if sentiment:
        print(f"""
  Regime:               {sentiment.get('market_regime', 'N/A')}
  OI-Weighted Funding:  {sentiment.get('oi_weighted_funding', 0):.4%}/8h
  Annual Equivalent:    {sentiment.get('funding_annual_pct', 0):.1f}%
  Total HL OI:         ${sentiment.get('total_oi_usd', 0):,.0f}""")

    # ── STEP 8: Dashboard ────────────────────────────────────────────────────
    print_section("STEP 8 — Generating Dashboard")
    from generate_dashboard import generate
    generate(
        leaderboard_df = leaderboard_df,
        profiles_df    = profiles_df,
        smart_money_df = smart_money_df,
        signals        = signal_output,
        current_state  = current_state,
        patterns       = patterns,
        market         = market,
    )
    print(f"\n  Dashboard: outputs/dashboard.html")
    print(f"\n{'═'*60}")
    print(f"  Done.")
    print(f"{'═'*60}\n")


if __name__ == "__main__":
    main()
