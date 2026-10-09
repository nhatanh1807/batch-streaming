
from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

import psycopg


class BronzeWriter:
    STAGING_TABLE = "bronze.trades_staging"
    TARGET_TABLE = "bronze.trades"

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
                    DROP TABLE IF EXISTS bronze.trades_staging;

                    CREATE TABLE bronze.trades_staging (
                        trade_id BIGINT PRIMARY KEY,
                        event_time_ms BIGINT NOT NULL,
                        symbol VARCHAR(20) NOT NULL,
                        price VARCHAR(50) NOT NULL,
                        quantity VARCHAR(50) NOT NULL,
                        buyer_maker BOOLEAN
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
                cur.execute(f"""
                    INSERT INTO {self.TARGET_TABLE} (
                        trade_id,
                        event_time_ms,
                        symbol,
                        price,
                        quantity,
                        buyer_maker
                    )
                    SELECT
                        trade_id,
                        event_time_ms,
                        symbol,
                        price,
                        quantity,
                        buyer_maker
                    FROM {self.STAGING_TABLE}
                    ON CONFLICT (trade_id) DO NOTHING;
                """)

    def write(self, df):
        self._create_staging_table()
        self._clear_staging()

        (
            df.select(
                "trade_id",
                "event_time",
                "symbol",
                "price",
                "quantity",
                "is_buyer_maker",
            )
            .withColumnRenamed("event_time", "event_time_ms")
            .withColumnRenamed("is_buyer_maker", "buyer_maker")
            .write
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
        self._clear_staging()

        print("Bronze data written to PostgreSQL")
