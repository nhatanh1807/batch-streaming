
import json
import shutil
import tempfile
from pathlib import Path
from datetime import datetime, timezone

import boto3
from pyspark.sql import DataFrame

from config.settings import (
    AWS_REGION,
    S3_BUCKET,
    S3_SILVER_PREFIX,
)


class ParquetWriter:
    COMPLETE_MARKER = "_BATCH_COMPLETE.json"

    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
        )

    def _delete_prefix(self, prefix: str):
        """Delete objects under a specific batch prefix."""
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

    def _batch_completed(self, marker_key: str) -> bool:
        try:
            response = self.s3.get_object(
                Bucket=S3_BUCKET,
                Key=marker_key,
            )

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
                    f"Invalid batch manifest: {marker_key}"
                )

            if (
                not isinstance(manifest["files"], list)
                or manifest["file_count"] != len(manifest["files"])
                or manifest["file_count"] == 0
            ):
                raise RuntimeError(
                    f"Invalid file list in manifest: {marker_key}"
                )

            return True

        except self.s3.exceptions.ClientError as exc:
            status_code = exc.response.get(
                "ResponseMetadata", {}
            ).get("HTTPStatusCode")

            error_code = exc.response.get(
                "Error", {}
            ).get("Code")

            if status_code == 404 or error_code in (
                "404",
                "NoSuchKey",
                "NotFound",
            ):
                return False

            raise
    def write(self, df: DataFrame, batch_id: str) -> str:
        if not batch_id or "/" in batch_id:
            raise ValueError("Invalid batch_id")

        if "trade_date" not in df.columns:
            raise ValueError(
                "DataFrame must contain trade_date"
            )

        batch_prefix = (
            f"{S3_SILVER_PREFIX.rstrip('/')}/batches/"
            f"{batch_id}/"
        )

        marker_key = (
            f"{batch_prefix}{self.COMPLETE_MARKER}"
        )

        # Never overwrite a published batch.
        if self._batch_completed(marker_key):
            print(
            f"Batch already published: {batch_id}. "
            "Skipping upload and returning existing prefix."
        )
            return batch_prefix

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

            # Clean only this unpublished batch prefix before retry.
            self._delete_prefix(batch_prefix)

            uploaded_files = []

            try:
                for file_path in parquet_files:
                    relative_path = file_path.relative_to(
                        local_dir
                    )

                    s3_key = (
                        f"{batch_prefix}"
                        f"{relative_path.as_posix()}"
                    )

                    self.s3.upload_file(
                        str(file_path),
                        S3_BUCKET,
                        s3_key,
                    )

                    uploaded_files.append({
                        "key": s3_key,
                        "size": file_path.stat().st_size,
                    })

                # Verify every uploaded file.
                for item in uploaded_files:
                    response = self.s3.head_object(
                        Bucket=S3_BUCKET,
                        Key=item["key"],
                    )

                    if response["ContentLength"] != item["size"]:
                        raise RuntimeError(
                            "Parquet size mismatch: "
                            f"{item['key']}"
                        )

                # Publish the batch only after all files are verified.
                manifest = {
                    "batch_id": batch_id,
                    "created_at": datetime.now(
                        timezone.utc
                    ).isoformat(),
                    "file_count": len(uploaded_files),
                    "files": uploaded_files,
                }

                self.s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=marker_key,
                    Body=json.dumps(
                        manifest,
                        indent=2,
                    ).encode("utf-8"),
                    ContentType="application/json",
                )

                # Verify publication marker.
                self.s3.head_object(
                    Bucket=S3_BUCKET,
                    Key=marker_key,
                )

            except Exception:
                # Unpublished batches can be cleaned for retry.
                try:
                    self._delete_prefix(batch_prefix)
                except Exception as cleanup_error:
                    print(
                        "WARNING: failed to clean partial batch: "
                        f"{cleanup_error}"
                    )

                raise

            print(
                f"Published batch {batch_id}: "
                f"{len(uploaded_files)} Parquet files "
                f"to s3://{S3_BUCKET}/{batch_prefix}"
            )

            return batch_prefix

        finally:
            shutil.rmtree(
                local_dir,
                ignore_errors=True,
            )
