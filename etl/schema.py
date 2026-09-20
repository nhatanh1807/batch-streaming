from pyspark.sql.types import *

trade_schema = StructType([
    StructField("e", StringType(), True),
    StructField("E", LongType(), True),
    StructField("s", StringType(), True),
    StructField("p", StringType(), True),
    StructField("q", StringType(), True),
    StructField("t", LongType(), True),
    StructField("T", LongType(), True),
])