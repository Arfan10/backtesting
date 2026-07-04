"""
Delta Exchange India - Python API Client
========================================
Base URL (Production India): https://api.india.delta.exchange

Covers:
  - Public endpoints (no auth needed): products, tickers, orderbook, candles
  - Private endpoints (auth required): wallet balance, positions, open orders, order history, fills

Authentication:
  Signature = HMAC-SHA256 of (METHOD + TIMESTAMP + PATH + QUERY_STRING + BODY)
  Pass in headers: api-key, timestamp, signature

Dependencies:
  pip install requests

Optional official client:
  pip install delta-rest-client
"""

import hashlib
import hmac
import time
import json
import requests
from typing import Optional

# ─── CONFIG ──────────────────────────────────────────────────────────────────

API_KEY    = "YOUR_API_KEY"       # Replace with your key
API_SECRET = "YOUR_API_SECRET"    # Replace with your secret
BASE_URL   = "https://api.india.delta.exchange"


# ─── AUTH HELPER ─────────────────────────────────────────────────────────────

def generate_signature(secret: str, method: str, path: str,
                       query_string: str = "", body: str = "") -> tuple[str, str]:
    """
    Returns (timestamp, signature).
    Signature = HMAC-SHA256( METHOD + TIMESTAMP + PATH + QUERY_STRING + BODY )
    """
    timestamp = str(int(time.time()))
    message   = method.upper() + timestamp + path + query_string + body
    signature = hmac.new(
        secret.encode("utf-8"),
        message.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return timestamp, signature


def auth_headers(method: str, path: str,
                 query_string: str = "", body: str = "") -> dict:
    """Build authenticated request headers."""
    timestamp, signature = generate_signature(
        API_SECRET, method, path, query_string, body
    )
    return {
        "api-key":      API_KEY,
        "timestamp":    timestamp,
        "signature":    signature,
        "Content-Type": "application/json",
        "Accept":       "application/json",
    }


# ─── BASE REQUEST ─────────────────────────────────────────────────────────────

def _get(path: str, params: Optional[dict] = None, authenticated: bool = False):
    url = BASE_URL + path

    # Build query string for signature (must match exactly what requests sends)
    query_string = ""
    if params:
        query_string = "&".join(f"{k}={v}" for k, v in params.items())

    headers = (
        auth_headers("GET", path, query_string)
        if authenticated
        else {"Accept": "application/json"}
    )

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()


def _post(path: str, body: dict) -> dict:
    body_str = json.dumps(body)
    headers  = auth_headers("POST", path, body=body_str)
    response = requests.post(BASE_URL + path, data=body_str, headers=headers)
    response.raise_for_status()
    return response.json()


def _delete(path: str, body: dict = None) -> dict:
    body_str = json.dumps(body) if body else ""
    headers  = auth_headers("DELETE", path, body=body_str)
    response = requests.delete(BASE_URL + path, data=body_str, headers=headers)
    response.raise_for_status()
    return response.json()


# ─── PUBLIC ENDPOINTS (no auth) ──────────────────────────────────────────────

def get_products(contract_type: Optional[str] = None):
    """
    All tradeable products.
    contract_type: e.g. 'perpetual_futures', 'futures', 'call_options', 'put_options'
    """
    params = {}
    if contract_type:
        params["contract_types"] = contract_type
    return _get("/v2/products", params)


def get_product(symbol: str):
    """Product details for a specific symbol, e.g. 'BTCUSDT'."""
    return _get(f"/v2/products/{symbol}")


def get_tickers(contract_type: Optional[str] = None):
    """Live tickers for all (or filtered) products."""
    params = {}
    if contract_type:
        params["contract_types"] = contract_type
    return _get("/v2/tickers", params)


def get_ticker(symbol: str):
    """Live ticker for a specific symbol."""
    return _get(f"/v2/tickers/{symbol}")


def get_orderbook(symbol: str, depth: int = 10):
    """Level-2 orderbook for a symbol. depth: number of price levels each side."""
    return _get(f"/v2/l2orderbook/{symbol}", params={"depth": depth})


def get_candles(symbol: str, resolution: str = "1h",
                start: Optional[int] = None, end: Optional[int] = None):
    """
    OHLCV candlestick data.
    resolution options: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 1d, 7d, 30d
    start/end: Unix timestamps (seconds). Defaults to last 100 candles.
    """
    params = {"symbol": symbol, "resolution": resolution}
    if start:
        params["start"] = start
    if end:
        params["end"] = end
    return _get("/v2/history/candles", params)


def get_assets():
    """All supported assets (BTC, ETH, USDT, etc.)."""
    return _get("/v2/assets")


# ─── PRIVATE ENDPOINTS (auth required) ───────────────────────────────────────

def get_wallet_balance():
    """
    Your wallet balances across all assets.
    Returns list of {asset_symbol, balance, available_balance, ...}
    """
    return _get("/v2/wallet/balances", authenticated=True)


def get_positions():
    """All open positions."""
    return _get("/v2/positions/margined", authenticated=True)


def get_position(product_id: int):
    """Open position for a specific product."""
    return _get("/v2/positions/margined", params={"product_id": product_id}, authenticated=True)


def get_open_orders(product_id: Optional[int] = None):
    """Fetch open (live) orders. Optionally filter by product_id."""
    params = {}
    if product_id:
        params["product_id"] = product_id
    return _get("/v2/orders", params=params, authenticated=True)


def get_order_history(product_id: Optional[int] = None, page_size: int = 100):
    """Paginated order history."""
    params = {"page_size": page_size}
    if product_id:
        params["product_id"] = product_id
    return _get("/v2/orders/history", params=params, authenticated=True)


def get_fills(product_id: Optional[int] = None, page_size: int = 100):
    """Trade fill history."""
    params = {"page_size": page_size}
    if product_id:
        params["product_id"] = product_id
    return _get("/v2/fills", params=params, authenticated=True)


def place_order(product_id: int, side: str, size: int,
                order_type: str = "limit_order", limit_price: Optional[str] = None):
    """
    Place a new order.
    side: 'buy' or 'sell'
    order_type: 'limit_order' or 'market_order'
    limit_price: required for limit orders (pass as string)
    """
    body = {
        "product_id": product_id,
        "side":       side,
        "size":       size,
        "order_type": order_type,
    }
    if limit_price:
        body["limit_price"] = limit_price
    return _post("/v2/orders", body)


def cancel_order(product_id: int, order_id: int):
    """Cancel a specific open order."""
    body = {"product_id": product_id, "id": order_id}
    return _delete("/v2/orders", body)


# ─── QUICK DEMO ───────────────────────────────────────────────────────────────

if __name__ == "__main__":

    print("=" * 60)
    print("1. BTC Perpetual Ticker")
    print("=" * 60)
    ticker = get_ticker("BTCUSDT")
    if ticker.get("success"):
        t = ticker["result"]
        print(f"  Symbol      : {t.get('symbol')}")
        print(f"  Mark Price  : {t.get('mark_price')}")
        print(f"  Spot Price  : {t.get('spot_price')}")
        print(f"  Volume (24h): {t.get('volume', {}).get('buy', 'N/A')}")
    else:
        print("  Error:", ticker)

    print()
    print("=" * 60)
    print("2. Top 5 Products (Perpetual Futures)")
    print("=" * 60)
    products = get_products(contract_type="perpetual_futures")
    if products.get("success"):
        for p in products["result"][:5]:
            print(f"  {p['symbol']:<20} id={p['id']}")
    else:
        print("  Error:", products)

    print()
    print("=" * 60)
    print("3. Wallet Balances  [requires valid API key/secret]")
    print("=" * 60)
    if API_KEY != "YOUR_API_KEY":
        balances = get_wallet_balance()
        if balances.get("success"):
            for b in balances["result"]:
                if float(b.get("balance", 0)) > 0:
                    print(f"  {b['asset_symbol']:<8} balance={b['balance']:>16}  available={b['available_balance']:>16}")
        else:
            print("  Error:", balances)
    else:
        print("  Skipped — set API_KEY and API_SECRET at the top of this file.")

    print()
    print("=" * 60)
    print("4. Open Positions   [requires valid API key/secret]")
    print("=" * 60)
    if API_KEY != "YOUR_API_KEY":
        positions = get_positions()
        if positions.get("success"):
            open_pos = [p for p in positions["result"] if p.get("size", 0) != 0]
            if open_pos:
                for p in open_pos:
                    print(f"  {p['product_symbol']:<20} size={p['size']}  entry={p['entry_price']}")
            else:
                print("  No open positions.")
        else:
            print("  Error:", positions)
    else:
        print("  Skipped — set API_KEY and API_SECRET at the top of this file.")