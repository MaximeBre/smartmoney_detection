"""
Low-level Hyperliquid API client.
All reads — no signing required.
"""
import time
import requests
import logging
from typing import Any

logger = logging.getLogger(__name__)

INFO_URL  = "https://api.hyperliquid.xyz/info"
STATS_URL = "https://stats-data.hyperliquid.xyz/Mainnet"

HEADERS = {"Content-Type": "application/json"}


def _post(payload: dict, retries: int = 4, backoff: float = 1.5) -> Any:
    """POST to Hyperliquid Info API with exponential backoff."""
    for attempt in range(retries):
        try:
            resp = requests.post(INFO_URL, json=payload, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429:
                wait = backoff ** (attempt + 1)
                logger.warning(f"Rate limited. Waiting {wait:.1f}s…")
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.RequestException as e:
            if attempt == retries - 1:
                raise
            wait = backoff ** (attempt + 1)
            logger.warning(f"Request failed ({e}). Retry in {wait:.1f}s…")
            time.sleep(wait)
    raise RuntimeError("Max retries exceeded")


def _get(path: str, retries: int = 4, backoff: float = 1.5) -> Any:
    """GET from Hyperliquid Stats API with exponential backoff."""
    url = f"{STATS_URL}/{path}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429:
                wait = backoff ** (attempt + 1)
                logger.warning(f"Rate limited. Waiting {wait:.1f}s…")
                time.sleep(wait)
            else:
                raise
        except requests.exceptions.RequestException as e:
            if attempt == retries - 1:
                raise
            wait = backoff ** (attempt + 1)
            logger.warning(f"Request failed ({e}). Retry in {wait:.1f}s…")
            time.sleep(wait)
    raise RuntimeError("Max retries exceeded")


# ── Market Data ────────────────────────────────────────────────────────────────

def get_all_mids() -> dict:
    """Current mid prices for all assets. Returns {coin: price_str}."""
    return _post({"type": "allMids"})


def get_meta_and_asset_ctxs() -> list:
    """
    Returns [meta, asset_ctxs].
    meta: universe info (name, szDecimals, etc.)
    asset_ctxs: funding, OI, mark price, etc. per asset
    """
    return _post({"type": "metaAndAssetCtxs"})


def get_l2_book(coin: str) -> dict:
    """Top-of-book for a coin. Returns {coin, levels: [[bids], [asks]]}."""
    return _post({"type": "l2Book", "coin": coin})


def get_recent_trades(coin: str) -> list:
    """Most recent trades for a coin."""
    return _post({"type": "recentTrades", "coin": coin})


def get_funding_history(coin: str, start_ms: int, end_ms: int = None) -> list:
    """Funding rate history for a coin."""
    payload = {"type": "fundingHistory", "coin": coin, "startTime": start_ms}
    if end_ms:
        payload["endTime"] = end_ms
    return _post(payload)


# ── User / Wallet Data ─────────────────────────────────────────────────────────

def get_clearinghouse_state(address: str) -> dict:
    """
    Full account state for a wallet.
    Includes: marginSummary, assetPositions, crossMaintenanceMarginUsed, etc.
    """
    return _post({"type": "clearinghouseState", "user": address})


def get_open_orders(address: str) -> list:
    """All open orders for a wallet."""
    return _post({"type": "openOrders", "user": address})


def get_user_fills(address: str, start_ms: int = None) -> list:
    """
    Complete fill (trade) history for a wallet.
    If start_ms given, fetches from that timestamp.
    Returns list of fills sorted newest first.
    """
    payload = {"type": "userFills", "user": address}
    if start_ms:
        payload["startTime"] = start_ms
    return _post(payload)


def get_user_fills_by_time(address: str, start_ms: int, end_ms: int = None) -> list:
    """Fill history with time range."""
    payload = {"type": "userFillsByTime", "user": address, "startTime": start_ms}
    if end_ms:
        payload["endTime"] = end_ms
    return _post(payload)


def get_user_funding_history(address: str, start_ms: int) -> list:
    """Funding payments received/paid by a wallet."""
    return _post({"type": "userFunding", "user": address, "startTime": start_ms})


def get_user_non_funding_ledger(address: str, start_ms: int) -> list:
    """Non-funding ledger updates (deposits, withdrawals, liquidations)."""
    return _post({"type": "userNonFundingLedgerUpdates", "user": address, "startTime": start_ms})


# ── Leaderboard ────────────────────────────────────────────────────────────────

def get_leaderboard() -> list:
    """
    Fetch leaderboard from Hyperliquid stats API.
    Returns list of {ethAddress, accountValue, pnl data, ...}
    """
    return _get("leaderboard")


# ── Liquidations ───────────────────────────────────────────────────────────────

def get_user_rate_limit(address: str) -> dict:
    """Rate limit info for a user."""
    return _post({"type": "userRateLimit", "user": address})
