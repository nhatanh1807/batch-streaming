import tempfile
from pathlib import Path
import os 
from etl.object_filter import select_objects
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
from etl.schema import trade_schema


class BronzeReader:

    def __init__(self):
        
        self.spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("Binance ETL")
            .config(
                "spark.jars",
                "/opt/jdbc/postgresql-42.7.8.jar",
            )
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "file:/opt/spark-events")
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

        # S3 objects selected for this ETL run.
        self.pending_objects = []

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
        """Download new/changed S3 objects or objects selected for backfill."""
        processed = self._get_processed_objects()
        all_objects = self._list_raw_objects()

        # Backfill settings are optional.
        start_date = os.getenv("BACKFILL_START_DATE") or None
        end_date = os.getenv("BACKFILL_END_DATE") or None

        ignore_checkpoint = (
            os.getenv("BACKFILL_IGNORE_CHECKPOINT", "false").lower()
            == "true"
        )

        # Select files for incremental ETL or date-range backfill.
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

        for obj in self.pending_objects:
            key = obj["object_key"]
            prefix = f"{S3_RAW_PREFIX.rstrip('/')}/"
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

