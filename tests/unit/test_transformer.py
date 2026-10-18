from datetime import date, datetime
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from etl.transformer import Transformer
@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("unit-test-transformer")
        .getOrCreate()
    )

    yield session
    session.stop()
@pytest.fixture
def input_df(spark):
    schema = StructType([
        StructField("trade_id", LongType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False),
        StructField("symbol", StringType(), nullable=False),
        StructField("price", DoubleType(), nullable=False),
        StructField("quantity", DoubleType(), nullable=False),
        StructField("buyer_maker", BooleanType(), nullable=False),
    ])
    return spark.createDataFrame(
        [
            (
                1,
                datetime(2026, 10, 17, 10, 30, 0),
                "BTCUSDT",
                100.0,
                2.0,
                True,
            ),
            (
                2,
                datetime(2026, 10, 17, 11, 0, 0),
                "BTCUSDT",
                200.0,
                3.0,
                False,
            ),
        ],
        schema=schema,
    )


def test_transform_calculates_trade_value_and_date(input_df):
    result = Transformer().transform(input_df)
    rows = result.orderBy("trade_id").collect()
    assert len(rows) == 2
    # Trade value = price * quantity
    assert rows[0]["trade_value"] == pytest.approx(200.0)
    assert rows[1]["trade_value"] == pytest.approx(600.0)
    # Trade date derived from event_time
    assert rows[0]["trade_date"] == date(2026, 10, 17)
    assert rows[1]["trade_date"] == date(2026, 10, 17)
    # Preserve buyer_maker values
    assert rows[0]["buyer_maker"] is True
    assert rows[1]["buyer_maker"] is False
