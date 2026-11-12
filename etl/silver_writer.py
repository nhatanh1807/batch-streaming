
from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

import psycopg
from pyspark.sql import functions as F
from pyspark.sql.window import Window

class SilverWriter:
    STAGING_TABLE = "silver.trades_staging"
    TARGET_TABLE = "silver.trades"

    # Shared lock identifier for this Silver writer
    LOCK_ID = 20261031

    def __init__(self):
        self.jdbc_url = (
            f"jdbc:postgresql://"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )

    def _get_connection(self):
        return psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

    def _create_staging_table(self):
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS silver.trades_staging (
                        trade_id BIGINT PRIMARY KEY,
                        event_time TIMESTAMPTZ NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        price NUMERIC(20, 8) NOT NULL,
                        quantity NUMERIC(20, 8) NOT NULL,
                        buyer_maker BOOLEAN,
                        trade_value NUMERIC(30, 8) NOT NULL,
                        trade_date DATE NOT NULL
                    );
                """)

    def _clear_staging(self):
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"TRUNCATE TABLE {self.STAGING_TABLE};"
                )

    def _merge_to_target(self):
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO silver.trades (
                        trade_id,
                        event_time,
                        symbol,
                        price,
                        quantity,
                        buyer_maker,
                        trade_value,
                        trade_date
                    )
                    SELECT
                        trade_id,
                        event_time,
                        symbol,
                        price,
                        quantity,
                        buyer_maker,
                        trade_value,
                        trade_date
                    FROM silver.trades_staging
                    ON CONFLICT (trade_id) DO NOTHING;
                """)
    def _deduplicate(self, df):
        payload_cols = [
            "event_time",
            "symbol",
            "price",
            "quantity",
            "buyer_maker",
            "trade_value",
            "trade_date",
        ]

        # Check conflicting records with the same trade_id
        conflicts = (
            df.groupBy("trade_id")
            .agg(
                F.countDistinct(
                    F.struct(*[F.col(c) for c in payload_cols])
                ).alias("payload_count")
            )
            .filter(F.col("payload_count") > 1)
        )

        conflict_count = conflicts.count()

        if conflict_count > 0:
            conflicts.show(20, truncate=False)
            raise ValueError(
                f"Found {conflict_count} trade_ids with conflicting payloads"
            )

        window_spec = (
            Window
            .partitionBy("trade_id")
            .orderBy(F.col("event_time").desc())
        )

        return (
            df
            .withColumn("_row_num", F.row_number().over(window_spec))
            .filter(F.col("_row_num") == 1)
            .drop("_row_num")
        )
    def save(self, df):
        self._create_staging_table()

        with self._get_connection() as lock_conn:
            with lock_conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_lock(%s);",
                    (self.LOCK_ID,),
                )

            try:
                self._clear_staging()

                original_count = df.count()

                df = self._deduplicate(df)

                deduplicated_count = df.count()

                print(f"Original rows: {original_count}")
                print(f"Rows after deduplication: {deduplicated_count}")
                print(f"Removed duplicates: {original_count - deduplicated_count}")

                (
                    df.write
                    .format("jdbc")
                    .option("url", self.jdbc_url)
                    .option("dbtable", self.STAGING_TABLE)
                    .option("user", POSTGRES_USER)
                    .option("password", POSTGRES_PASSWORD)
                    .option("driver", "org.postgresql.Driver")
                    .mode("append")
                    .save()
                )

                self._merge_to_target()
                print("Silver data written to PostgreSQL")

            finally:
                try:
                    self._clear_staging()
                finally:
                    with lock_conn.cursor() as cur:
                        cur.execute(
                            "SELECT pg_advisory_unlock(%s);",
                            (self.LOCK_ID,),
                        )
