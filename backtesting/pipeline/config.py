"""
Pipeline Configuration
======================
Set your credentials and database URL here.
Never commit this file with real values to version control.
Use environment variables in production.
"""

import os

# ── Delta Exchange Credentials ────────────────────────────────────────────────
DELTA_API_KEY    = os.getenv("DELTA_API_KEY")
DELTA_API_SECRET = os.getenv("DELTA_API_SECRET")

# WebSocket endpoint (India production)
DELTA_WS_URL     = "wss://socket.india.delta.exchange"

# ── PostgreSQL Connection ─────────────────────────────────────────────────────
# Format: postgresql://user:password@host:port/dbname
POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://postgres:password@localhost:5432/delta_data"
)

# ── Symbols to Subscribe ──────────────────────────────────────────────────────
# Add/remove symbols as needed.
# Perpetual futures format: BTCUSDT, ETHUSDT
# Options format: C-BTC-95200-200225, P-BTC-95200-200225
TICKER_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
TRADE_SYMBOLS  = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]

# ── Pipeline Tuning ───────────────────────────────────────────────────────────
# Number of rows to buffer before flushing to DB in a single batch INSERT
BATCH_SIZE = 1

# Seconds between heartbeat pings to keep the WS connection alive
HEARTBEAT_INTERVAL = 30

# Seconds to wait before reconnecting after a drop
RECONNECT_DELAY = 5