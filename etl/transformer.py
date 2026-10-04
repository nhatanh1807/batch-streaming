from pyspark.sql.functions import col, to_date, timestamp_millis


class Transformer:

    def transform(self, df):

        return (
            df
            .select(
                col("trade_id"),
                timestamp_millis(
                    col("event_time")
                ).alias("event_time"),
                col("symbol"),
                col("price")
                    .cast("decimal(20,8)")
                    .alias("price"),
                col("quantity")
                    .cast("decimal(20,8)")
                    .alias("quantity"),
                col("is_buyer_maker")
                    .alias("buyer_maker"),
            )
            .withColumn(
                "trade_value",
                (
                    col("price") * col("quantity")
                ).cast("decimal(30,8)")
            )
            .withColumn(
                "trade_date",
                to_date(col("event_time"))
            )
        )