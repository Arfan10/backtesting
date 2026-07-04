-- =============================================================================
-- Delta Exchange → PostgreSQL Schema
-- Run once to create all tables before starting the pipeline.
--
--   psql -d delta_data -f schema.sql
-- =============================================================================

-- ── Market Data ───────────────────────────────────────────────────────────────

-- Live ticker snapshots (price, OI, mark price, etc.)
CREATE TABLE IF NOT EXISTS tickers (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    mark_price      NUMERIC,
    spot_price      NUMERIC,
    close           NUMERIC,          -- last traded price
    open            NUMERIC,
    high            NUMERIC,
    low             NUMERIC,
    volume          NUMERIC,
    turnover        NUMERIC,
    open_interest   NUMERIC,
    funding_rate    NUMERIC,
    oi_change_usd_6h NUMERIC,
    -- Options Greeks (null for non-options)
    iv              NUMERIC,
    delta           NUMERIC,
    gamma           NUMERIC,
    theta           NUMERIC,
    vega            NUMERIC,
    rho             NUMERIC,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tickers_symbol_time ON tickers (symbol, received_at DESC);

-- Individual trades (every fill on the exchange)
CREATE TABLE IF NOT EXISTS trades (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    trade_id        BIGINT,           -- exchange trade ID
    price           NUMERIC     NOT NULL,
    size            NUMERIC     NOT NULL,
    side            TEXT,             -- 'buy' or 'sell'
    traded_at       TIMESTAMPTZ,      -- exchange timestamp
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trades_symbol_time ON trades (symbol, traded_at DESC);

-- 1-minute OHLCV candles (streamed in real-time)
CREATE TABLE IF NOT EXISTS candles_1m (
    id              BIGSERIAL PRIMARY KEY,
    symbol          TEXT        NOT NULL,
    open            NUMERIC,
    high            NUMERIC,
    low             NUMERIC,
    close           NUMERIC,
    volume          NUMERIC,
    candle_start_at TIMESTAMPTZ NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, candle_start_at)   -- upsert-safe
);

CREATE INDEX IF NOT EXISTS idx_candles_symbol_time ON candles_1m (symbol, candle_start_at DESC);

-- ── Account Data (Private Channels) ──────────────────────────────────────────

-- Wallet margin / balance snapshots
CREATE TABLE IF NOT EXISTS margins (
    id              BIGSERIAL PRIMARY KEY,
    asset_symbol    TEXT        NOT NULL,
    balance         NUMERIC,
    available_balance NUMERIC,
    order_margin    NUMERIC,
    position_margin NUMERIC,
    unrealised_pnl  NUMERIC,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_margins_asset_time ON margins (asset_symbol, received_at DESC);

-- Open/closed positions
CREATE TABLE IF NOT EXISTS positions (
    id                  BIGSERIAL PRIMARY KEY,
    product_id          BIGINT,
    product_symbol      TEXT        NOT NULL,
    size                NUMERIC,
    entry_price         NUMERIC,
    mark_price          NUMERIC,
    liquidation_price   NUMERIC,
    unrealised_pnl      NUMERIC,
    realised_pnl        NUMERIC,
    margin_mode         TEXT,
    auto_topup          BOOLEAN,
    received_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_positions_symbol_time ON positions (product_symbol, received_at DESC);

-- Order state changes (new, filled, cancelled, etc.)
CREATE TABLE IF NOT EXISTS orders (
    id              BIGSERIAL PRIMARY KEY,
    order_id        BIGINT,
    product_id      BIGINT,
    product_symbol  TEXT,
    side            TEXT,
    size            NUMERIC,
    unfilled_size   NUMERIC,
    order_type      TEXT,
    limit_price     NUMERIC,
    avg_fill_price  NUMERIC,
    state           TEXT,             -- 'open', 'filled', 'cancelled'
    created_at      TIMESTAMPTZ,
    updated_at      TIMESTAMPTZ,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_orders_symbol_time ON orders (product_symbol, received_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_order_id    ON orders (order_id);

-- User fills (every personal trade execution)
CREATE TABLE IF NOT EXISTS user_trades (
    id              BIGSERIAL PRIMARY KEY,
    fill_id         BIGINT,
    product_id      BIGINT,
    product_symbol  TEXT,
    side            TEXT,
    size            NUMERIC,
    price           NUMERIC,
    role            TEXT,             -- 'maker' or 'taker'
    commission      NUMERIC,
    pnl             NUMERIC,
    traded_at       TIMESTAMPTZ,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_trades_symbol ON user_trades (product_symbol, traded_at DESC);
CREATE INDEX IF NOT EXISTS idx_user_trades_fill_id ON user_trades (fill_id);