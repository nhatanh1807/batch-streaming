from pyspark.sql.functions import col, to_date


class Transformer:

    def transform(self, df):

        return (
            df
            .select(
                col("trade_id"),
                col("event_time"),
                col("symbol"),
                col("price").cast("decimal(20,8)").alias("price"),
                col("quantity").cast("decimal(20,8)").alias("quantity"),
                col("buyer_maker")
            )
            .withColumn(
                "trade_value",
                (col("price") * col("quantity")).cast("decimal(30,8)")
            )
            .withColumn(
                "trade_date",
                to_date(col("event_time"))
            )
        )