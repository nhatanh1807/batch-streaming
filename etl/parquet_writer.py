
import shutil
import tempfile
from pathlib import Path

import boto3
from pyspark.sql import DataFrame

from config.settings import (
    AWS_REGION,
    S3_BUCKET,
    S3_SILVER_PREFIX,
)


class ParquetWriter:
    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
        )

    def _delete_prefix(self, prefix: str):
        """Delete only objects belonging to this batch."""
        paginator = self.s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=prefix,
        ):
            objects = [
                {"Key": item["Key"]}
                for item in page.get("Contents", [])
            ]

            if objects:
                self.s3.delete_objects(
                    Bucket=S3_BUCKET,
                    Delete={"Objects": objects},
                )

    def write(self, df: DataFrame, batch_id: str) -> str:
        if not batch_id or "/" in batch_id:
            raise ValueError("Invalid batch_id")

        if "trade_date" not in df.columns:
            raise ValueError("DataFrame must contain trade_date")

        batch_prefix = (
            f"{S3_SILVER_PREFIX.rstrip('/')}/batches/{batch_id}/"
        )

        local_dir = tempfile.mkdtemp(
            prefix="parquet_batch_"
        )

        try:
            (
                df.write
                .mode("overwrite")
                .partitionBy("trade_date")
                .parquet(local_dir)
            )

            parquet_files = list(
                Path(local_dir).rglob("*.parquet")
            )

            if not parquet_files:
                raise RuntimeError(
                    f"No Parquet files generated: {batch_id}"
                )

            # Remove only the previous output of this batch.
            self._delete_prefix(batch_prefix)

            uploaded_keys = []

            try:
                for file_path in parquet_files:
                    relative_path = file_path.relative_to(local_dir)

                    s3_key = (
                        f"{batch_prefix}{relative_path.as_posix()}"
                    )

                    self.s3.upload_file(
                        str(file_path),
                        S3_BUCKET,
                        s3_key,
                    )

                    uploaded_keys.append(s3_key)

                # Verify uploaded Parquet objects.
                for key in uploaded_keys:
                    self.s3.head_object(
                        Bucket=S3_BUCKET,
                        Key=key,
                    )

            except Exception:
                # Clean partial uploads so the batch can be retried.
                self._delete_prefix(batch_prefix)
                raise

            print(
                f"Uploaded {len(uploaded_keys)} Parquet files "
                f"to s3://{S3_BUCKET}/{batch_prefix}"
            )

            return batch_prefix

        finally:
            shutil.rmtree(
                local_dir,
                ignore_errors=True,
            )