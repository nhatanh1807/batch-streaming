import json
import os
import tempfile
from datetime import datetime, timezone

import boto3

from config.settings import S3_BUCKET, S3_RAW_PREFIX, AWS_REGION


class BronzeWriter:

    def __init__(self):
        self.s3 = boto3.client(
            "s3",
            region_name=AWS_REGION,
        )

    def save(self, records):
        if not records:
            return

        now = datetime.now(timezone.utc)
        timestamp = now.strftime("%Y%m%d_%H%M%S_%f")

        # Partition raw data by UTC ingestion date
        s3_key = (
            f"{S3_RAW_PREFIX}/"
            f"year={now:%Y}/"
            f"month={now:%m}/"
            f"day={now:%d}/"
            f"{timestamp}.jsonl"
        )

        tmp_path = None

        try:
            # Write one JSON object per line
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".jsonl",
                delete=False,
            ) as f:
                tmp_path = f.name

                for record in records:
                    f.write(
                        json.dumps(record, separators=(",", ":")) + "\n"
                    )

            # Upload only after the local batch file is complete
            self.s3.upload_file(
                tmp_path,
                S3_BUCKET,
                s3_key,
            )

            print(
                f"Uploaded {len(records)} records to "
                f"s3://{S3_BUCKET}/{s3_key}"
            )

        finally:
            # Remove temporary file even if upload fails
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
