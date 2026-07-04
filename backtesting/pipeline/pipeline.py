"""
Delta Exchange → PostgreSQL Real-Time Pipeline
===============================================
Streams market data (tickers, trades, candles) and account data
(margins, positions, orders, user trades) via WebSocket and writes
them to PostgreSQL in batches.

Install dependencies:
    pip install websocket-client psycopg2-binary

Run:
    python pipeline.py

Stop:
    Ctrl+C  (clean shutdown, flushes pending buffers)
"""

import hashlib
import hmac
import json
import logging
import signal
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import psycopg2
import psycopg2.extras
import websocket
import requests   
from config import (
    DELTA_API_KEY,
    DELTA_API_SECRET,
    DELTA_WS_URL,
    POSTGRES_DSN,
    TICKER_SYMBOLS,
    TRADE_SYMBOLS,
    BATCH_SIZE,
    HEARTBEAT_INTERVAL,
    RECONNECT_DELAY,
)

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("delta_pipeline")


# ── Helpers ───────────────────────────────────────────────────────────────────

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def safe_decimal(value):
    """Convert string/float/None to float, return None on failure."""
    if value is None:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_ts(value) -> datetime | None:
    """Parse ISO 8601 timestamp string to datetime. Returns None on failure."""
    if not value:
        return None
    try:
        # Handle microsecond precision with or without Z suffix
        value = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(value)
    except Exception:
        return None


def generate_ws_signature(secret: str) -> tuple[str, str]:
    try:
        resp = requests.get(
            "https://api.india.delta.exchange/v2/time",
            timeout=5
        )
        server_time = resp.json()["result"]["server_time"]
        timestamp = str(int(server_time))
    except Exception as e:
        log.error("Server time fetch failed: %s", e)  # add this line
        timestamp = str(int(time.time()))

    message   = "GET" + timestamp + "/live"
    signature = hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return timestamp, signature

# ── Buffer + DB Writer ────────────────────────────────────────────────────────

class BufferedWriter:
    """
    Thread-safe buffer. Accumulates rows per table and flushes to
    PostgreSQL when buffer hits BATCH_SIZE or flush() is called explicitly.
    """

    INSERT_SQL = {
        "tickers": """
            INSERT INTO tickers
                (symbol, mark_price, spot_price, close, open, high, low,
                 volume, turnover, open_interest, funding_rate,
                 oi_change_usd_6h, iv, delta, gamma, theta, vega, rho, received_at)
            VALUES %s
        """,
        "trades": """
            INSERT INTO trades
                (symbol, trade_id, price, size, side, traded_at, received_at)
            VALUES %s
        """,
        "candles_1m": """
            INSERT INTO candles_1m
                (symbol, open, high, low, close, volume, candle_start_at, received_at)
            VALUES %s
            ON CONFLICT (symbol, candle_start_at) DO UPDATE SET
                open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
                close=EXCLUDED.close, volume=EXCLUDED.volume,
                received_at=EXCLUDED.received_at
        """,
        "margins": """
            INSERT INTO margins
                (asset_symbol, balance, available_balance, order_margin,
                 position_margin, unrealised_pnl, received_at)
            VALUES %s
        """,
        "positions": """
            INSERT INTO positions
                (product_id, product_symbol, size, entry_price, mark_price,
                 liquidation_price, unrealised_pnl, realised_pnl,
                 margin_mode, auto_topup, received_at)
            VALUES %s
        """,
        "orders": """
            INSERT INTO orders
                (order_id, product_id, product_symbol, side, size,
                 unfilled_size, order_type, limit_price, avg_fill_price,
                 state, created_at, updated_at, received_at)
            VALUES %s
        """,
        "user_trades": """
            INSERT INTO user_trades
                (fill_id, product_id, product_symbol, side, size,
                 price, role, commission, pnl, traded_at, received_at)
            VALUES %s
        """,
    }

    def __init__(self, dsn: str, batch_size: int):
        self.dsn        = dsn
        self.batch_size = batch_size
        self.lock       = threading.Lock()
        self.buffers    = defaultdict(list)
        self._connect()

    def _connect(self):
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = False
        log.info("PostgreSQL connected.")

    def _ensure_connection(self):
        try:
            self.conn.cursor().execute("SELECT 1")
        except Exception:
            log.warning("DB connection lost — reconnecting…")
            try:
                self.conn.close()
            except Exception:
                pass
            self._connect()

    def add(self, table: str, row: tuple):
        with self.lock:
            self.buffers[table].append(row)
            if len(self.buffers[table]) >= self.batch_size:
                self._flush_table(table)

    def flush(self):
        with self.lock:
            for table in list(self.buffers.keys()):
                self._flush_table(table)

    def _flush_table(self, table: str):
        rows = self.buffers[table]
        if not rows:
            return
        self._ensure_connection()
        try:
            with self.conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, self.INSERT_SQL[table], rows)
            self.conn.commit()
            log.debug("Flushed %d rows → %s", len(rows), table)
        except Exception as e:
            self.conn.rollback()
            log.error("DB write failed for %s: %s", table, e)
        finally:
            self.buffers[table] = []

    def close(self):
        self.flush()
        try:
            self.conn.close()
        except Exception:
            pass
        log.info("DB connection closed.")


# ── Message Parsers ───────────────────────────────────────────────────────────

def parse_ticker(msg: dict, writer: BufferedWriter):
    """v2/ticker channel — one message per symbol per update."""
    data = msg.get("result", {})
    if not data:
        return
    writer.add("tickers", (
        data.get("symbol"),
        safe_decimal(data.get("mark_price")),
        safe_decimal(data.get("spot_price")),
        safe_decimal(data.get("close")),
        safe_decimal(data.get("open")),
        safe_decimal(data.get("high")),
        safe_decimal(data.get("low")),
        safe_decimal(data.get("volume")),
        safe_decimal(data.get("turnover")),
        safe_decimal(data.get("open_interest")),
        safe_decimal(data.get("funding_rate")),
        safe_decimal(data.get("oi_change_usd_6h")),
        # Greeks
        safe_decimal(data.get("greeks", {}).get("iv")),
        safe_decimal(data.get("greeks", {}).get("delta")),
        safe_decimal(data.get("greeks", {}).get("gamma")),
        safe_decimal(data.get("greeks", {}).get("theta")),
        safe_decimal(data.get("greeks", {}).get("vega")),
        safe_decimal(data.get("greeks", {}).get("rho")),
        now_utc(),
    ))


def parse_trade(msg: dict, writer: BufferedWriter):
    """trades channel — may contain a list of trades."""
    trades = msg.get("result", [])
    if isinstance(trades, dict):
        trades = [trades]
    for t in trades:
        writer.add("trades", (
            t.get("symbol") or msg.get("symbol"),
            t.get("id"),
            safe_decimal(t.get("price")),
            safe_decimal(t.get("size") or t.get("quantity")),
            t.get("buyer_role") and ("buy" if t.get("buyer_role") == "taker" else "sell"),
            parse_ts(t.get("timestamp") or t.get("created_at")),
            now_utc(),
        ))


def parse_candle(msg: dict, writer: BufferedWriter):
    """candlestick_1m channel."""
    data = msg.get("result", {})
    if not data:
        return
    writer.add("candles_1m", (
        msg.get("symbol") or data.get("symbol"),
        safe_decimal(data.get("open")),
        safe_decimal(data.get("high")),
        safe_decimal(data.get("low")),
        safe_decimal(data.get("close")),
        safe_decimal(data.get("volume")),
        parse_ts(data.get("start")) or parse_ts(data.get("time")),
        now_utc(),
    ))


def parse_margins(msg: dict, writer: BufferedWriter):
    """Private: Margins channel — wallet balances."""
    data = msg.get("result", {})
    if not data:
        return
    # Can be a single object or list
    items = data if isinstance(data, list) else [data]
    for item in items:
        writer.add("margins", (
            item.get("asset_symbol") or item.get("asset"),
            safe_decimal(item.get("balance")),
            safe_decimal(item.get("available_balance")),
            safe_decimal(item.get("order_margin")),
            safe_decimal(item.get("position_margin")),
            safe_decimal(item.get("unrealised_cashflow") or item.get("unrealised_pnl")),
            now_utc(),
        ))


def parse_positions(msg: dict, writer: BufferedWriter):
    """Private: Positions channel."""
    data = msg.get("result", {})
    if not data:
        return
    items = data if isinstance(data, list) else [data]
    for item in items:
        writer.add("positions", (
            item.get("product_id"),
            item.get("product_symbol"),
            safe_decimal(item.get("size")),
            safe_decimal(item.get("entry_price")),
            safe_decimal(item.get("mark_price")),
            safe_decimal(item.get("liquidation_price")),
            safe_decimal(item.get("unrealised_pnl")),
            safe_decimal(item.get("realised_pnl") or item.get("realized_pnl")),
            item.get("margin_mode"),
            item.get("auto_topup"),
            now_utc(),
        ))


def parse_orders(msg: dict, writer: BufferedWriter):
    """Private: Orders channel."""
    data = msg.get("result", {})
    if not data:
        return
    items = data if isinstance(data, list) else [data]
    for item in items:
        writer.add("orders", (
            item.get("id"),
            item.get("product_id"),
            item.get("product_symbol"),
            item.get("side"),
            safe_decimal(item.get("size")),
            safe_decimal(item.get("unfilled_size")),
            item.get("order_type"),
            safe_decimal(item.get("limit_price")),
            safe_decimal(item.get("average_fill_price") or item.get("avg_fill_price")),
            item.get("state"),
            parse_ts(item.get("created_at")),
            parse_ts(item.get("updated_at")),
            now_utc(),
        ))


def parse_user_trades(msg: dict, writer: BufferedWriter):
    """Private: UserTrades / v2/user_trades channel."""
    data = msg.get("result", {})
    if not data:
        return
    items = data if isinstance(data, list) else [data]
    for item in items:
        writer.add("user_trades", (
            item.get("id") or item.get("fill_id"),
            item.get("product_id"),
            item.get("product_symbol"),
            item.get("side"),
            safe_decimal(item.get("size")),
            safe_decimal(item.get("price") or item.get("fill_price")),
            item.get("role"),
            safe_decimal(item.get("commission")),
            safe_decimal(item.get("pnl") or item.get("realized_pnl")),
            parse_ts(item.get("created_at") or item.get("timestamp")),
            now_utc(),
        ))


# ── Channel Router ────────────────────────────────────────────────────────────

CHANNEL_PARSERS = {
    "v2/ticker":       parse_ticker,
    "ticker":          parse_ticker,
    "trades":          parse_trade,
    "candlestick_1m":  parse_candle,
    "candlestick":     parse_candle,
    # Private
    "margins":         parse_margins,
    "Margins":         parse_margins,
    "positions":       parse_positions,
    "Positions":       parse_positions,
    "orders":          parse_orders,
    "Orders":          parse_orders,
    "user_trades":     parse_user_trades,
    "v2/user_trades":  parse_user_trades,
    "UserTrades":      parse_user_trades,
}


# ── WebSocket Pipeline ────────────────────────────────────────────────────────

class DeltaPipeline:

    def __init__(self):
        self.writer    = BufferedWriter(POSTGRES_DSN, BATCH_SIZE)
        self.ws        = None
        self._stop     = threading.Event()
        self._hb_timer = None

    # ── Subscription payloads ─────────────────────────────────────────────────

    def _subscribe_public(self, ws):
        """Subscribe to public market data channels."""
        channels = [
            {"name": "v2/ticker",      "symbols": TICKER_SYMBOLS},
            {"name": "candlestick_1m", "symbols": TICKER_SYMBOLS},
        ]
        payload = {"type": "subscribe", "payload": {"channels": channels}}
        ws.send(json.dumps(payload))
        log.info("Subscribed public channels: %s", [c["name"] for c in channels])

    def _subscribe_private(self, ws):
        channels = [
            {"name": "margins"},
            {"name": "positions",  "symbols": TICKER_SYMBOLS},
            {"name": "orders",     "symbols": TICKER_SYMBOLS},
            {"name": "user_trades","symbols": TICKER_SYMBOLS},
        ]
        payload = {"type": "subscribe", "payload": {"channels": channels}}
        ws.send(json.dumps(payload))
        log.info("Subscribed private channels: %s", [c["name"] for c in channels])

    def _authenticate(self, ws):
        """Send key-auth frame (current method as of Oct 2025 changelog)."""
        timestamp, signature = generate_ws_signature(DELTA_API_SECRET)
        auth_msg = {
            "type": "key-auth",
            "payload": {
                "api-key":   DELTA_API_KEY,
                "signature": signature,
                "timestamp": timestamp,
            },
        }
        ws.send(json.dumps(auth_msg))
        log.info("Auth frame sent.")

    # ── Heartbeat ─────────────────────────────────────────────────────────────

    def _start_heartbeat(self, ws):
        if self._hb_timer:
            self._hb_timer.cancel()

        def ping():
            if not self._stop.is_set() and ws:
                try:
                    ws.send(json.dumps({"type": "heartbeat"}))
                    log.debug("Heartbeat sent.")
                except Exception as e:
                    log.warning("Heartbeat failed: %s", e)
            self._start_heartbeat(ws)  # reschedule

        self._hb_timer = threading.Timer(HEARTBEAT_INTERVAL, ping)
        self._hb_timer.daemon = True
        self._hb_timer.start()

    def _stop_heartbeat(self):
        if self._hb_timer:
            self._hb_timer.cancel()
            self._hb_timer = None

    # ── WebSocket callbacks ───────────────────────────────────────────────────

    def on_open(self, ws):
        log.info("WebSocket connected.")
        self._authenticate(ws)
        self._subscribe_public(ws)
        self._subscribe_private(ws)
        self._start_heartbeat(ws)

    def on_message(self, ws, raw: str):
        log.info("RAW: %s", raw[:300])
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("Non-JSON message: %s", raw[:200])
            return

        msg_type = msg.get("type", "")

        # Auth confirmation
        if msg_type in ("key-auth", "auth"):
            success = msg.get("success") or msg.get("payload", {}).get("authenticated")
            if success:
                log.info("Authenticated successfully.")
            else:
                log.error("Authentication FAILED: %s", msg)
            return

        # Subscription confirmations
        if msg_type in ("subscriptions", "subscription"):
            log.info("Subscription confirmed: %s", msg)
            return

        # Heartbeat echo
        if msg_type == "heartbeat":
            return

        # Error frames
        if msg_type == "error":
            log.error("WS error frame: %s", msg)
            return

        # Data messages — route by channel name
        channel = (
            msg.get("channel")
            or msg.get("type")
            or msg.get("name", "")
        )
        parser = CHANNEL_PARSERS.get(channel)
        if parser:
            try:
                parser(msg, self.writer)
            except Exception as e:
                log.error("Parse error on channel '%s': %s | msg=%s", channel, e, raw[:300])
        else:
            log.debug("Unhandled channel '%s': %s", channel, raw[:200])

    def on_error(self, ws, error):
        log.error("WebSocket error: %s", error)

    def on_close(self, ws, code, msg):
        self._stop_heartbeat()
        self.writer.flush()
        if not self._stop.is_set():
            log.warning("WebSocket closed (code=%s, msg=%s) — reconnecting in %ds…",
                        code, msg, RECONNECT_DELAY)

    # ── Run loop ──────────────────────────────────────────────────────────────

    def run(self):
        """Run forever with automatic reconnection."""
        log.info("Starting Delta Exchange → PostgreSQL pipeline…")
        log.info("Symbols: %s", TICKER_SYMBOLS)

        while not self._stop.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    DELTA_WS_URL,
                    on_open    = self.on_open,
                    on_message = self.on_message,
                    on_error   = self.on_error,
                    on_close   = self.on_close,
                )
                # run_forever blocks until the connection closes
                self.ws.run_forever(ping_interval=0)  # we handle heartbeat ourselves
            except Exception as e:
                log.error("Unexpected error in run loop: %s", e)

            if not self._stop.is_set():
                log.info("Reconnecting in %ds…", RECONNECT_DELAY)
                time.sleep(RECONNECT_DELAY)

        log.info("Pipeline stopped.")

    def stop(self):
        log.info("Shutdown requested — flushing buffers…")
        self._stop.set()
        self._stop_heartbeat()
        if self.ws:
            self.ws.close()
        self.writer.close()


# ── Entry Point ───────────────────────────────────────────────────────────────

def main():
    pipeline = DeltaPipeline()

    # Graceful shutdown on Ctrl+C / SIGTERM
    def handle_signal(sig, frame):
        log.info("Signal %s received — shutting down…", sig)
        pipeline.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT,  handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    pipeline.run()


if __name__ == "__main__":
    main()