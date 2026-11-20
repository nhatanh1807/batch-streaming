
from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

import psycopg


class CheckpointManager:
    def __init__(self):
        self.connection_config = {
            "host": POSTGRES_HOST,
            "port": POSTGRES_PORT,
            "dbname": POSTGRES_DB,
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD,
        }

    def _get_connection(self):
        return psycopg.connect(**self.connection_config)

    def create_tables(self):
        """Create independent checkpoints for the two ETL stages."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS public.bronze_silver_checkpoint (
                        object_key TEXT PRIMARY KEY,
                        etag TEXT NOT NULL,
                        output_prefix TEXT,
                        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                """)

                cur.execute("""
                    CREATE TABLE IF NOT EXISTS public.silver_postgres_checkpoint (
                        object_key TEXT PRIMARY KEY,
                        etag TEXT NOT NULL,
                        processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    );
                """)

    def get_processed_objects(self, table_name: str) -> dict[str, str]:
        allowed_tables = {
            "public.bronze_silver_checkpoint",
            "public.silver_postgres_checkpoint",
        }

        if table_name not in allowed_tables:
            raise ValueError(
                f"Unsupported checkpoint table: {table_name}"
            )

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT object_key, etag FROM {table_name}"
                )
                return dict(cur.fetchall())

    def save_checkpoint(
        self,
        table_name: str,
        object_key: str,
        etag: str,
        output_prefix: str | None = None,
    ):
        allowed_tables = {
            "public.bronze_silver_checkpoint",
            "public.silver_postgres_checkpoint",
        }

        if table_name not in allowed_tables:
            raise ValueError(
                f"Unsupported checkpoint table: {table_name}"
            )

        if table_name == "public.bronze_silver_checkpoint":
            query = """
                INSERT INTO public.bronze_silver_checkpoint (
                    object_key, etag, output_prefix, processed_at
                )
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (object_key)
                DO UPDATE SET
                    etag = EXCLUDED.etag,
                    output_prefix = EXCLUDED.output_prefix,
                    processed_at = NOW();
            """
            params = (object_key, etag, output_prefix)

        else:
            query = """
                INSERT INTO public.silver_postgres_checkpoint (
                    object_key, etag, processed_at
                )
                VALUES (%s, %s, NOW())
                ON CONFLICT (object_key)
                DO UPDATE SET
                    etag = EXCLUDED.etag,
                    processed_at = NOW();
            """
            params = (object_key, etag)

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)

    def save_bronze_silver_batch(
        self,
        objects: list[dict],
        output_prefix: str,
    ):
        """Save source checkpoints after Parquet upload succeeds."""
        if not objects:
            return

        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO public.bronze_silver_checkpoint (
                        object_key,
                        etag,
                        output_prefix,
                        processed_at
                    )
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (object_key)
                    DO UPDATE SET
                        etag = EXCLUDED.etag,
                        output_prefix = EXCLUDED.output_prefix,
                        processed_at = NOW();
                    """,
                    [
                        (
                            obj["object_key"],
                            obj["etag"],
                            output_prefix,
                        )
                        for obj in objects
                    ],
                )
