
import os
import tempfile
from pathlib import Path

import boto3
import psycopg
from pyspark.sql import SparkSession

from config.settings import (
    S3_BUCKET,
    S3_RAW_PREFIX,
    AWS_REGION,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)
from etl.object_filter import select_objects
from etl.schema import trade_schema


class BronzeReader:

    def __init__(self):
        self.spark = None
        self.temp_dir = None
        self.local_dir = None
        self.pending_objects = []

        event_log_dir = "/opt/spark-events"
        os.makedirs(event_log_dir, exist_ok=True)

        try:
            self.spark = (
                SparkSession.builder
                .master("local[*]")
                .appName("Binance ETL")
                .config(
                    "spark.jars",
                    "/opt/jdbc/postgresql-42.7.8.jar",
                )
                .config("spark.eventLog.enabled", "true")
                .config(
                    "spark.eventLog.dir",
                    f"file:{event_log_dir}",
                )
                .config("spark.eventLog.filePermissions", "644")
                .getOrCreate()
            )

            self.s3 = boto3.client(
                "s3",
                region_name=AWS_REGION,
            )

            self.temp_dir = tempfile.TemporaryDirectory(
                prefix="batch_streaming_s3_"
            )
            self.local_dir = Path(self.temp_dir.name)

        except Exception:
            self.close()
            raise

    def _get_connection(self):
        return psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
        )

    def _get_processed_objects(self):
        """Return previously processed S3 object keys and ETags."""
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT object_key, etag
                    FROM public.etl_file_checkpoint;
                """)

                return {
                    key: etag
                    for key, etag in cur.fetchall()
                }

    def _list_raw_objects(self):
        """List JSONL objects under the configured S3 prefix."""
        paginator = self.s3.get_paginator("list_objects_v2")
        objects = []

        prefix = f"{S3_RAW_PREFIX.rstrip('/')}/"

        for page in paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=prefix,
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                if not key.endswith(".jsonl"):
                    continue

                objects.append({
                    "object_key": key,
                    "etag": obj.get("ETag", "").strip('"'),
                })

        return objects

    def read(self):
        """Download selected S3 objects and return a Spark DataFrame."""

        if self.spark is None or self.temp_dir is None:
            raise RuntimeError("BronzeReader is not initialized.")

        processed = self._get_processed_objects()
        all_objects = self._list_raw_objects()

        start_date = os.getenv("BACKFILL_START_DATE") or None
        end_date = os.getenv("BACKFILL_END_DATE") or None

        ignore_checkpoint = (
            os.getenv("BACKFILL_IGNORE_CHECKPOINT", "false").lower()
            == "true"
        )

        self.pending_objects = select_objects(
            objects=all_objects,
            processed=processed,
            start_date=start_date,
            end_date=end_date,
            ignore_checkpoint=ignore_checkpoint,
        )

        if start_date and end_date:
            print(
                f"Backfill range: {start_date} to {end_date}; "
                f"selected {len(self.pending_objects)} files."
            )

        if not self.pending_objects:
            print("No eligible S3 files to process.")
            return None

        files = []
        prefix = f"{S3_RAW_PREFIX.rstrip('/')}/"

        for obj in self.pending_objects:
            key = obj["object_key"]

            if not key.startswith(prefix):
                raise ValueError(
                    f"S3 object is outside configured prefix: {key}"
                )

            relative_key = key[len(prefix):]

            local_file = self.local_dir / relative_key
            local_file.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            self.s3.download_file(
                S3_BUCKET,
                key,
                str(local_file),
            )

            files.append(str(local_file))

        print(
            f"Downloaded {len(files)} files "
            f"from s3://{S3_BUCKET}/{S3_RAW_PREFIX}/"
        )

        return (
            self.spark.read
            .schema(trade_schema)
            .json(files)
        )

    def close(self):
        """Release Spark and temporary resources."""
        if self.spark is not None:
            try:
                self.spark.catalog.clearCache()
            finally:
                self.spark.stop()
                self.spark = None

        if self.temp_dir is not None:
            self.temp_dir.cleanup()
            self.temp_dir = None
