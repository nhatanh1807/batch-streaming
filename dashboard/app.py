
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from streamlit_autorefresh import st_autorefresh
st_autorefresh(
    interval=10_000,
    limit=None,
    key="dashboard_autorefresh",
)
st.set_page_config(
    page_title="Binance Trading Dashboard",
    page_icon="📈",
    layout="wide",
)

st.title("📈 Binance Trading Dashboard")
st.caption("BTCUSDT · PostgreSQL · Spark ETL")

# ---------- Database connection ----------
@st.cache_resource
def get_engine():
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD", "postgres"),
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        database=os.getenv("POSTGRES_DB", "batch_streaming"),
    )
    return create_engine(url, pool_pre_ping=True)


engine = get_engine()


@st.cache_data(ttl=10)
def load_trades(symbol: str, hours: int) -> pd.DataFrame:
    query = text("""
        SELECT
            trade_id,
            event_time,
            symbol,
            price,
            quantity,
            buyer_maker
        FROM silver.trades
        WHERE symbol = :symbol
        AND event_time >= NOW() - (:hours * INTERVAL '1 hour')
        ORDER BY event_time DESC
    """)

    with engine.connect() as conn:
        df = pd.read_sql(
            query,
            conn,
            params={"symbol": symbol, "hours": hours},
        )

    if not df.empty:
        df["event_time"] = pd.to_datetime(df["event_time"], utc=True)
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
        df["trade_value"] = df["price"] * df["quantity"]

    return df


# ---------- Filters ----------
with st.sidebar:
    st.header("Filters")
    symbol = st.selectbox("Trading pair", ["BTCUSDT"])
    hours = st.selectbox(
        "Time range",
        options=[1, 3, 6, 12, 24, 48],
        index=3,
        format_func=lambda x: f"Last {x} hours",
    )
    if st.button("Refresh data", use_container_width=True):
        st.cache_data.clear()

try:
    df = load_trades(symbol, hours)

except Exception as exc:
    st.error(f"Cannot load trades from PostgreSQL: {exc}")
    st.info(
        "Check that silver.trades exists, PostgreSQL is running, "
        "and the dashboard container can connect to it."
    )
    st.stop()

if df.empty:
    st.warning(
        "No trades found for the selected period. "
        "Check whether silver.trades contains data."
    )
    st.stop()

# ---------- KPI cards ----------
latest = df.iloc[0]
total_volume = df["quantity"].sum()
total_value = df["trade_value"].sum()
trade_count = len(df)

c1, c2, c3, c4 = st.columns(4)

c1.metric("Latest price", f"${latest['price']:,.2f}")
c2.metric("Trades", f"{trade_count:,}")
c3.metric("Volume (BTC)", f"{total_volume:,.4f}")
c4.metric("Trade value (USDT)", f"${total_value:,.2f}")

st.caption(
    f"Latest event time: {latest['event_time']} UTC · "
    "Metrics are calculated from the selected period."
)

# ---------- Price chart ----------
st.subheader("Price movement")

price_df = df.sort_values("event_time")

price_fig = px.line(
    price_df,
    x="event_time",
    y="price",
    title=f"{symbol} price over time",
    labels={
        "event_time": "Event time",
        "price": "Price (USDT)",
    },
)
price_fig.update_layout(hovermode="x unified")
st.plotly_chart(price_fig, use_container_width=True)

# ---------- Volume chart ----------
st.subheader("Trading volume")

volume_df = (
    df.set_index("event_time")
    .sort_index()
    .resample("1min")
    .agg(
        trade_count=("trade_id", "count"),
        volume=("quantity", "sum"),
        trade_value=("trade_value", "sum"),
    )
    .reset_index()
)

volume_fig = px.bar(
    volume_df,
    x="event_time",
    y="volume",
    title="Volume per minute",
    labels={
        "event_time": "Event time",
        "volume": "Volume (BTC)",
    },
)
st.plotly_chart(volume_fig, use_container_width=True)

# ---------- Buyer maker analysis ----------
st.subheader("Buyer maker analysis")

maker_df = (
    df.assign(
        buyer_maker_label=df["buyer_maker"].map(
            {True: "Buyer is maker", False: "Buyer is taker"}
        ).fillna("Unknown")
    )
    .groupby("buyer_maker_label", as_index=False)
    .agg(
        trades=("trade_id", "count"),
        volume=("quantity", "sum"),
    )
)

maker_fig = px.pie(
    maker_df,
    names="buyer_maker_label",
    values="trades",
    title="Trade count by buyer maker status",
    hole=0.45,
)
st.plotly_chart(maker_fig, use_container_width=True)

# ---------- Recent trades ----------
st.subheader("Recent trades")

recent_df = df.sort_values("event_time", ascending=False).head(100).copy()
recent_df["event_time"] = recent_df["event_time"].dt.strftime(
    "%Y-%m-%d %H:%M:%S UTC"
)

st.dataframe(
    recent_df[
        [
            "trade_id",
            "event_time",
            "price",
            "quantity",
            "trade_value",
            "buyer_maker",
        ]
    ],
    use_container_width=True,
    hide_index=True,
)
