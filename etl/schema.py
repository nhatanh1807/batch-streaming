from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    BooleanType,
)


trade_schema = StructType([
    StructField("trade_id", LongType(), True),
    StructField("event_time", LongType(), True),
    StructField("symbol", StringType(), True),
    StructField("price", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("is_buyer_maker", BooleanType(), True),
])