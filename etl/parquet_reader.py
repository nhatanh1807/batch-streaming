import json
import os
import tempfile
from pathlib import Path, PurePosixPath

import boto3
import psycopg
from pyspark.sql import SparkSession

from config.settings import (
    AWS_REGION,
    S3_BUCKET,
    S3_SILVER_PREFIX,
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


class ParquetReader:

    COMPLETE_MARKER = "_BATCH_COMPLETE.json"
    CHECKPOINT_TABLE = "public.silver_postgres_checkpoint"

    def __init__(self):
        self.spark = None
        self.temp_dir = None
        self.local_dir = None
        self.pending_batches = []

        self.s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
        )

        try:
            self.spark = (
                SparkSession.builder
                .master("local[*]")
                .appName("Parquet to PostgreSQL ETL")
                .config(
                    "spark.jars",
                    "/opt/jdbc/postgresql-42.7.8.jar",
                )
                .getOrCreate()
            )

            self.temp_dir = tempfile.TemporaryDirectory(
                prefix="batch_streaming_parquet_"
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

    def _get_processed_batches(self):
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT object_key, etag
                    FROM {self.CHECKPOINT_TABLE};
                    """
                )

                return dict(cur.fetchall())

    def _list_batch_prefixes(self):
        base_prefix = (
            f"{S3_SILVER_PREFIX.rstrip('/')}/batches/"
        )

        paginator = self.s3.get_paginator("list_objects_v2")
        prefixes = []

        for page in paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=base_prefix,
            Delimiter="/",
        ):
            for item in page.get("CommonPrefixes", []):
                prefixes.append(item["Prefix"])

        return sorted(prefixes)

    def _read_manifest(self, batch_prefix):
        marker_key = (
            f"{batch_prefix}{self.COMPLETE_MARKER}"
        )

        try:
            response = self.s3.get_object(
                Bucket=S3_BUCKET,
                Key=marker_key,
            )

        except self.s3.exceptions.ClientError as exc:
            error_code = exc.response.get(
                "Error", {}
            ).get("Code")

            if error_code in ("NoSuchKey", "404", "NotFound"):
                return None

            raise

        manifest = json.loads(
            response["Body"].read().decode("utf-8")
        )

        required_fields = {
            "batch_id",
            "created_at",
            "file_count",
            "files",
        }

        if not required_fields.issubset(manifest):
            raise RuntimeError(
                f"Invalid manifest: {marker_key}"
            )

        batch_id = manifest["batch_id"]

        expected_batch_id = (
            batch_prefix.rstrip("/").split("/")[-1]
        )

        if batch_id != expected_batch_id:
            raise RuntimeError(
                f"Batch ID mismatch: {marker_key}"
            )

        files = manifest["files"]

        if (
            not isinstance(files, list)
            or manifest["file_count"] != len(files)
            or manifest["file_count"] == 0
        ):
            raise RuntimeError(
                f"Invalid file list: {marker_key}"
            )

        for item in files:
            if (
                not isinstance(item, dict)
                or "key" not in item
                or "size" not in item
            ):
                raise RuntimeError(
                    f"Invalid file entry: {marker_key}"
                )

            key = item["key"]

            if not key.startswith(batch_prefix):
                raise RuntimeError(
                    f"File outside batch prefix: {key}"
                )

            if not key.endswith(".parquet"):
                raise RuntimeError(
                    f"Unexpected file type: {key}"
                )

            relative_path = key[len(batch_prefix):]
            path_parts = PurePosixPath(relative_path).parts

            if (
                not relative_path
                or ".." in path_parts
                or relative_path.startswith("/")
            ):
                raise RuntimeError(
                    f"Invalid relative file path: {key}"
                )

            if not isinstance(item["size"], int) or item["size"] < 0:
                raise RuntimeError(
                    f"Invalid file size: {key}"
                )

        return {
            "batch_id": batch_id,
            "marker_key": marker_key,
            "etag": response["ETag"].strip('"'),
            "files": files,
            "batch_prefix": batch_prefix,
        }

    def list_pending_batches(self):
        processed = self._get_processed_batches()
        batch_prefixes = self._list_batch_prefixes()

        eligible_batches = []

        target_batch_id = os.getenv("STAGE2_BATCH_ID")

        for prefix in batch_prefixes:
            batch = self._read_manifest(prefix)

            # Ignore unpublished or incomplete batches.
            if batch is None:
                continue

            # Optional batch filter for testing.
            if (
                target_batch_id
                and batch["batch_id"] != target_batch_id
            ):
                continue

            # Skip batches already processed with the same ETag.
            if processed.get(batch["marker_key"]) == batch["etag"]:
                continue

            eligible_batches.append(batch)

        self.pending_batches = eligible_batches

        print(
            f"Found {len(self.pending_batches)} pending batches."
        )

        return self.pending_batches

    def _download_batch(self, batch):
        batch_local_dir = (
            self.local_dir / batch["batch_id"]
        )

        batch_local_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        local_files = []

        for item in batch["files"]:
            key = item["key"]

            relative_path = key[len(batch["batch_prefix"]):]
            local_file = batch_local_dir / relative_path

            local_file.parent.mkdir(
                parents=True,
                exist_ok=True,
            )

            self.s3.download_file(
                S3_BUCKET,
                key,
                str(local_file),
            )

            actual_size = local_file.stat().st_size

            if actual_size != item["size"]:
                raise RuntimeError(
                    f"Downloaded file size mismatch: {key}"
                )

            local_files.append(str(local_file))

        return local_files

    def read_batch(self, batch):
        if self.spark is None or self.temp_dir is None:
            raise RuntimeError(
                "ParquetReader is not initialized."
            )

        local_files = self._download_batch(batch)

        if not local_files:
            raise RuntimeError(
                f"Batch has no Parquet files: {batch['batch_id']}"
            )

        print(
            f"Reading batch {batch['batch_id']}: "
            f"{len(local_files)} Parquet files"
        )

        # Read only the exact files declared in this manifest.
        df = (
            self.spark.read
            .option(
                "basePath",
                str(self.local_dir / batch["batch_id"]),
            )
            .parquet(*local_files)
        )

        return df

    def close(self):
        if self.spark is not None:
            try:
                self.spark.catalog.clearCache()
            finally:
                self.spark.stop()
                self.spark = None

        if self.temp_dir is not None:
            self.temp_dir.cleanup()
            self.temp_dir = None
            self.local_dir = None