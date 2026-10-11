import tempfile
from pathlib import Path

import boto3
from pyspark.sql import SparkSession

from config.settings import (
    S3_BUCKET,
    S3_RAW_PREFIX,
    AWS_REGION,
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
            .getOrCreate()
        )

        self.s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
        )

        # Keep the temporary directory alive while Spark uses its files.
        self.temp_dir = tempfile.TemporaryDirectory(
            prefix="batch_streaming_s3_"
        )
        self.local_dir = Path(self.temp_dir.name)

    def read(self):
        paginator = self.s3.get_paginator("list_objects_v2")

        files = []

        for page in paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=f"{S3_RAW_PREFIX.rstrip('/')}/",
        ):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                if not key.endswith(".jsonl"):
                    continue

                relative_key = key[
                    len(S3_RAW_PREFIX.rstrip("/")) + 1:
                ]
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

        if not files:
            raise FileNotFoundError(
                f"No JSONL files found at "
                f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}/"
            )

        print(
            f"Downloaded {len(files)} raw files from "
            f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}/"
        )

        return (
            self.spark.read
            .schema(trade_schema)
            .json(files)
        )