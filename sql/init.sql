CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;


CREATE TABLE IF NOT EXISTS bronze.trades (
    trade_id BIGINT PRIMARY KEY,
    event_time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    quantity NUMERIC(20, 8) NOT NULL,
    buyer_maker BOOLEAN,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


CREATE INDEX IF NOT EXISTS idx_bronze_trades_event_time
ON bronze.trades(event_time);


CREATE INDEX IF NOT EXISTS idx_bronze_trades_symbol
ON bronze.trades(symbol);

CREATE TABLE IF NOT EXISTS silver.trades (
    trade_id BIGINT PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    price NUMERIC(20, 8) NOT NULL,
    quantity NUMERIC(20, 8) NOT NULL,
    trade_datetime TIMESTAMPTZ NOT NULL,
    buyer_maker BOOLEAN,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_silver_trades_trade_datetime
ON silver.trades(trade_datetime);

CREATE INDEX IF NOT EXISTS idx_silver_trades_symbol
ON silver.trades(symbol);