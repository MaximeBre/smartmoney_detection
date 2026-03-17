"""
Live position monitor for smart money wallets.
Detects new position opens/closes and generates alerts.
"""
import os
import json
import logging
import time
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from data.hl_client import get_clearinghouse_state
from config import POSITIONS_STATE, MONITOR_TOP_N, STATE_DIR, SMART_MONEY_THRESHOLD

logger = logging.getLogger(__name__)


def load_last_state() -> dict:
    """Load the last known position state from disk."""
    if os.path.exists(POSITIONS_STATE):
        try:
            with open(POSITIONS_STATE) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state: dict) -> None:
    """Save current position state to disk."""
    os.makedirs(STATE_DIR, exist_ok=True)
    with open(POSITIONS_STATE, "w") as f:
        json.dump(state, f, indent=2, default=str)


def get_current_positions(address: str) -> dict:
    """
    Get all current open positions for a wallet.
    Returns {coin: {size, side, entry_price, unrealized_pnl, leverage}}
    """
    try:
        state = get_clearinghouse_state(address)
        positions = {}

        for pos_entry in state.get("assetPositions", []):
            pos  = pos_entry.get("position", {})
            size = float(pos.get("szi", 0))
            if size == 0:
                continue

            coin     = pos.get("coin", "")
            entry_px = float(pos.get("entryPx", 0) or 0)
            upnl     = float(pos.get("unrealizedPnl", 0) or 0)
            lev_info = pos.get("leverage", {}) or {}
            leverage = float(lev_info.get("value", 1) or 1)

            positions[coin] = {
                "size":           size,
                "side":           "LONG" if size > 0 else "SHORT",
                "entry_price":    entry_px,
                "unrealized_pnl": upnl,
                "leverage":       leverage,
                "abs_size":       abs(size),
            }

        return positions

    except Exception as e:
        logger.debug(f"Failed to get positions for {address[:8]}: {e}")
        return {}


def detect_changes(address: str,
                   old_positions: dict,
                   new_positions: dict) -> list:
    """
    Compare old vs new positions and return list of change events.
    """
    events = []
    now    = datetime.now(timezone.utc).isoformat()

    old_coins = set(old_positions.keys())
    new_coins = set(new_positions.keys())

    # New positions opened
    for coin in new_coins - old_coins:
        pos = new_positions[coin]
        events.append({
            "event":       "POSITION_OPENED",
            "timestamp":   now,
            "address":     address,
            "coin":        coin,
            "side":        pos["side"],
            "size":        pos["abs_size"],
            "entry_price": pos["entry_price"],
            "leverage":    pos["leverage"],
        })

    # Positions closed
    for coin in old_coins - new_coins:
        old = old_positions[coin]
        events.append({
            "event":     "POSITION_CLOSED",
            "timestamp": now,
            "address":   address,
            "coin":      coin,
            "side":      old["side"],
        })

    # Size changes (adds or reduces)
    for coin in old_coins & new_coins:
        old_sz = old_positions[coin]["size"]
        new_sz = new_positions[coin]["size"]
        delta  = new_sz - old_sz

        if abs(delta) / max(abs(old_sz), 1e-9) > 0.1:  # >10% size change
            events.append({
                "event":       "POSITION_INCREASED" if abs(new_sz) > abs(old_sz) else "POSITION_REDUCED",
                "timestamp":   now,
                "address":     address,
                "coin":        coin,
                "side":        new_positions[coin]["side"],
                "old_size":    abs(old_sz),
                "new_size":    abs(new_sz),
                "delta":       delta,
                "entry_price": new_positions[coin]["entry_price"],
            })

    return events


def snapshot_smart_money_positions(smart_money_df: pd.DataFrame) -> dict:
    """
    Snapshot current positions for all top smart money wallets.
    Returns {address: {positions, smart_money_score, grade, ...}}
    """
    os.makedirs(STATE_DIR, exist_ok=True)

    top_wallets = smart_money_df[
        smart_money_df["smart_money_score"] >= SMART_MONEY_THRESHOLD
    ].head(MONITOR_TOP_N)

    if top_wallets.empty:
        # Fall back to top N regardless of threshold
        top_wallets = smart_money_df.head(min(MONITOR_TOP_N, len(smart_money_df)))

    logger.info(f"  Snapshotting positions for {len(top_wallets)} smart money wallets...")

    current_state = {}
    for _, row in top_wallets.iterrows():
        addr      = row["address"]
        positions = get_current_positions(addr)

        current_state[addr] = {
            "positions":         positions,
            "smart_money_score": float(row.get("smart_money_score", 0)),
            "grade":             str(row.get("grade", "")),
            "display_name":      str(row.get("display_name", "")),
            "ic_8h":             float(row.get("ic_8h", 0)),
            "ic_recent_8h":      float(row.get("ic_recent_8h", 0)),
            "snapshot_time":     datetime.now(timezone.utc).isoformat(),
        }
        time.sleep(0.05)

    return current_state


def run_live_monitor(smart_money_df: pd.DataFrame) -> tuple:
    """
    Compare current vs last known positions.
    Returns (list of change events, current_state dict) for smart money wallets.
    """
    logger.info("Running live position monitor...")

    last_state    = load_last_state()
    current_state = snapshot_smart_money_positions(smart_money_df)

    all_events = []
    for addr, current in current_state.items():
        old_positions = last_state.get(addr, {}).get("positions", {})
        new_positions = current.get("positions", {})

        events = detect_changes(addr, old_positions, new_positions)

        # Annotate events with smart money metadata
        for ev in events:
            ev["smart_money_score"] = current["smart_money_score"]
            ev["grade"]             = current["grade"]
            ev["display_name"]      = current["display_name"]

        all_events.extend(events)

    save_state(current_state)

    if all_events:
        logger.info(f"  Detected {len(all_events)} position changes")
        for ev in all_events:
            name = ev.get("display_name") or ev["address"][:8]
            logger.info(f"    [{ev['grade']}] {name}: {ev['event']} {ev.get('coin','')} {ev.get('side','')}")
    else:
        logger.info("  No position changes detected")

    return all_events, current_state
