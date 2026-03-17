# ─────────────────────────────────────────
# Hyperliquid Wallet Tracker — Config
# ─────────────────────────────────────────

# API Endpoints
HL_INFO_URL   = "https://api.hyperliquid.xyz/info"
HL_STATS_URL  = "https://stats-data.hyperliquid.xyz/Mainnet"

# How many top wallets to pull from leaderboard
TOP_N_WALLETS = 200

# How many days of trade history to fetch per wallet
HISTORY_DAYS = 90

# Minimum trades to include a wallet in analysis
MIN_TRADES = 20

# Minimum absolute PnL (USD) to include wallet
MIN_PNL_USD = 50_000

# Wallet quality filter: exclude market makers
# volume_month / pnl_alltime > this → likely market maker (earns from spreads, not direction)
MAX_VOL_PNL_RATIO = 1000

# ROI cap for quality score (avoid tiny-account survivor bias)
ROI_CAP_FOR_SCORING = 5.0  # = 500% all-time

# Rate limiting between API calls (seconds)
API_SLEEP_BETWEEN    = 0.12   # between individual wallet requests
API_SLEEP_BATCH      = 2.0    # extra pause every N wallets
API_SLEEP_BATCH_SIZE = 50     # pause every 50 wallets

# Output paths
OUTPUT_DIR        = "outputs"
RAW_DIR           = "outputs/raw"
LEADERBOARD_CSV   = "outputs/raw/leaderboard.csv"
FILLS_DIR         = "outputs/raw/fills"
PROFILES_CSV      = "outputs/raw/trader_profiles.csv"
PATTERNS_CSV      = "outputs/raw/patterns.csv"
MARKET_JSON       = "outputs/raw/market_overview.json"
DASHBOARD_HTML    = "outputs/dashboard.html"

# Analysis
TIMEFRAMES = {
    "short":  7,    # days
    "medium": 30,
    "long":   90,
}

# Assets tracked on Hyperliquid (top by OI)
TRACKED_ASSETS = [
    "BTC", "ETH", "SOL", "DOGE", "XRP",
    "AVAX", "LINK", "ARB", "OP", "SUI",
    "WIF", "PEPE", "BONK", "JUP", "TIA",
    "INJ", "BLUR", "APT", "SEI", "STRK",
]

# ── Smart Money Detection ──────────────────────────────────
# IC Analysis horizons (in hours)
IC_HORIZONS = [1, 4, 8, 24]

# Minimum trades needed to compute IC
IC_MIN_TRADES = 15

# Rolling window for IC (days)
IC_ROLLING_DAYS = 30

# Smart Money Score threshold to be considered "smart"
SMART_MONEY_THRESHOLD = 0.15

# Score weights
IC_WEIGHT_RECENT   = 0.40   # IC last 30 days
IC_WEIGHT_ALLTIME  = 0.30   # IC all-time
IC_WEIGHT_ICIR     = 0.20   # IC consistency (ICIR)
IC_WEIGHT_TREND    = 0.10   # IC trend (improving?)

# Live Monitor
MONITOR_TOP_N      = 20     # Monitor top N smart money wallets
POSITIONS_STATE    = "outputs/state/positions.json"

# Signal thresholds
SIGNAL_MIN_SCORE       = 0.15   # Minimum smart money score to generate signal
SIGNAL_CONSENSUS_MIN   = 2      # Minimum wallets agreeing for consensus signal

# Price data cache
CANDLES_DIR  = "outputs/raw/candles"
CANDLE_INTERVAL = "1h"

# GitHub Pages
GITHUB_PAGES_URL = ""

# Output paths (add to existing)
SMART_MONEY_CSV  = "outputs/raw/smart_money_scores.csv"
SIGNALS_CSV      = "outputs/raw/signals.csv"
SIGNALS_JSON     = "outputs/raw/signals.json"
STATE_DIR        = "outputs/state"
