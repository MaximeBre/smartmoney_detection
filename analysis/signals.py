"""
Signal aggregation from smart money position changes.
Generates copy-trading signals with confidence scores.
"""
import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from config import (
    SIGNAL_MIN_SCORE, SIGNAL_CONSENSUS_MIN,
    SIGNALS_CSV, SIGNALS_JSON,
)

logger = logging.getLogger(__name__)


def events_to_signals(events: list,
                       current_state: dict) -> list:
    """
    Convert raw position-change events into actionable copy-trading signals.

    Only generate signals for POSITION_OPENED events from wallets
    above the minimum smart money score threshold.
    """
    # Filter to actionable events (new opens only)
    opens = [
        e for e in events
        if e["event"] == "POSITION_OPENED"
        and e.get("smart_money_score", 0) >= SIGNAL_MIN_SCORE
    ]

    if not opens:
        return []

    signals = []
    for ev in opens:
        addr  = ev["address"]
        coin  = ev["coin"]
        side  = ev["side"]
        score = ev["smart_money_score"]

        # How many other smart money wallets hold same direction?
        consensus_count  = 0
        consensus_scores = []
        for other_addr, state in current_state.items():
            if other_addr == addr:
                continue
            other_score = state.get("smart_money_score", 0)
            if other_score < SIGNAL_MIN_SCORE:
                continue
            other_pos = state.get("positions", {}).get(coin, {})
            if other_pos.get("side") == side:
                consensus_count += 1
                consensus_scores.append(other_score)

        # Consensus confidence: average score of agreeing wallets
        if consensus_scores:
            consensus_confidence = np.mean(consensus_scores + [score])
        else:
            consensus_confidence = score

        # Signal strength
        if consensus_count >= 2 and consensus_confidence > 0.3:
            strength = "STRONG"
        elif consensus_count >= 1 or score > 0.3:
            strength = "MODERATE"
        else:
            strength = "WEAK"

        signals.append({
            "timestamp":            datetime.now(timezone.utc).isoformat(),
            "coin":                 coin,
            "direction":            side,
            "primary_wallet":       addr,
            "primary_wallet_name":  ev.get("display_name", addr[:8]),
            "primary_score":        round(score, 4),
            "consensus_wallets":    consensus_count + 1,
            "consensus_confidence": round(float(consensus_confidence), 4),
            "signal_strength":      strength,
            "entry_price":          ev.get("entry_price", 0),
            "leverage":             ev.get("leverage", 1),
            "grade":                ev.get("grade", ""),
            "action":               f"{side} {coin}",
            "note": (
                f"{consensus_count + 1} smart money wallets {side} {coin} "
                f"(avg score: {consensus_confidence:.2f})"
            ),
        })

    # Sort by consensus_confidence descending
    signals.sort(key=lambda x: x["consensus_confidence"], reverse=True)
    return signals


def aggregate_current_consensus(current_state: dict) -> list:
    """
    Even without new events, show what smart money is currently holding.
    Grouped by (coin, direction) with consensus count.
    """
    coin_sides = {}  # {(coin, side): [wallet_dicts]}

    for addr, state in current_state.items():
        score = state.get("smart_money_score", 0)
        if score < SIGNAL_MIN_SCORE:
            continue

        for coin, pos in state.get("positions", {}).items():
            key = (coin, pos["side"])
            if key not in coin_sides:
                coin_sides[key] = []
            coin_sides[key].append({
                "address": addr,
                "score":   score,
                "grade":   state.get("grade", ""),
                "name":    state.get("display_name", addr[:8]),
            })

    # Build consensus table
    consensus = []
    for (coin, side), wallets in coin_sides.items():
        if len(wallets) < 1:
            continue

        avg_score = np.mean([w["score"] for w in wallets])
        max_score = max(w["score"] for w in wallets)

        consensus.append({
            "coin":              coin,
            "direction":         side,
            "n_wallets":         len(wallets),
            "avg_score":         round(float(avg_score), 4),
            "max_score":         round(float(max_score), 4),
            "wallets":           [w["name"] for w in wallets],
            "is_consensus":      len(wallets) >= SIGNAL_CONSENSUS_MIN,
            "consensus_strength": (
                "STRONG"   if len(wallets) >= 3 and avg_score > 0.3 else
                "MODERATE" if len(wallets) >= 2 or avg_score > 0.2 else
                "WEAK"
            ),
        })

    consensus.sort(key=lambda x: (x["n_wallets"], x["avg_score"]), reverse=True)
    return consensus


def save_signals(signals: list, consensus: list) -> None:
    """Save signals and consensus to disk."""
    os.makedirs("outputs/raw", exist_ok=True)

    output = {
        "generated_at":      datetime.now(timezone.utc).isoformat(),
        "new_signals":       signals,
        "current_consensus": consensus,
        "n_new_signals":     len(signals),
        "n_consensus":       len([c for c in consensus if c["is_consensus"]]),
    }

    with open(SIGNALS_JSON, "w") as f:
        json.dump(output, f, indent=2, default=str)

    if signals:
        pd.DataFrame(signals).to_csv(SIGNALS_CSV, index=False)

    logger.info(f"  {len(signals)} new signals, {len(consensus)} consensus positions")


def run_signal_pipeline(events: list, current_state: dict) -> dict:
    """Full signal pipeline."""
    signals   = events_to_signals(events, current_state)
    consensus = aggregate_current_consensus(current_state)
    save_signals(signals, consensus)

    return {
        "signals":   signals,
        "consensus": consensus,
    }
