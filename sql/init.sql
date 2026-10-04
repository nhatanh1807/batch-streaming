CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- =========================================================
-- BRONZE
-- Raw Binance data loaded by micro-batch
-- =========================================================

CREATE TABLE IF NOT EXISTS bronze.trades (
    trade_id BIGINT PRIMARY KEY,
    event_time_ms BIGINT NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    price VARCHAR(50) NOT NULL,
    quantity VARCHAR(50) NOT NULL,
    buyer_maker BOOLEAN,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_trades_event_time
ON bronze.trades(event_time_ms);

CREATE INDEX IF NOT EXISTS idx_bronze_trades_symbol
ON bronze.trades(symbol);


-- =========================================================
-- SILVER
-- =========================================================

CREATE TABLE IF NOT EXISTS silver.trades (
    trade_id BIGINT PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    quantity NUMERIC(20, 8) NOT NULL,
    buyer_maker BOOLEAN,
    trade_value NUMERIC(30, 8) NOT NULL,
    trade_date DATE NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);