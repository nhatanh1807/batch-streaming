
import hashlib

from etl.bronze_reader import BronzeReader
from etl.checkpoint_manager import CheckpointManager
from etl.parquet_writer import ParquetWriter
from etl.transformer import Transformer


CHECKPOINT_TABLE = "public.bronze_silver_checkpoint"


def generate_batch_id(objects: list[dict]) -> str:
    """
    Generate a deterministic batch ID from source object keys and ETags.
    The same source files produce the same batch ID.
    """
    source_signature = "\n".join(
        f"{obj['object_key']}|{obj['etag']}"
        for obj in sorted(
            objects,
            key=lambda item: item["object_key"],
        )
    )

    return hashlib.sha256(
        source_signature.encode("utf-8")
    ).hexdigest()


class JSONLToParquetPipeline:
    def __init__(self):
        self.reader = None
        self.transformer = Transformer()
        self.parquet_writer = ParquetWriter()
        self.checkpoint_manager = CheckpointManager()

    def run(self):
        self.reader = BronzeReader(
            checkpoint_table=CHECKPOINT_TABLE,
        )

        try:
            # 1. Read unprocessed JSONL objects from S3.
            raw_df = self.reader.read()

            if raw_df is None:
                print("Stage 1 skipped: no new JSONL files.")
                return

            objects = self.reader.pending_objects

            if not objects:
                print("Stage 1 skipped: no pending source objects.")
                return

            # 2. Generate a stable batch ID.
            batch_id = generate_batch_id(objects)

            print(f"Stage 1 batch ID: {batch_id}")
            print(f"Source JSONL files: {len(objects)}")

            # 3. Transform raw data.
            transformed_df = self.transformer.transform(raw_df)

            print("Transformed DataFrame schema:")
            transformed_df.printSchema()

            # 4. Write Parquet to S3.
            output_prefix = self.parquet_writer.write(
                transformed_df,
                batch_id=batch_id,
            )

            # 5. Save checkpoints only after Parquet upload succeeds.
            self.checkpoint_manager.save_bronze_silver_batch(
                objects=objects,
                output_prefix=output_prefix,
            )

            print(
                f"Stage 1 completed successfully. "
                f"Processed {len(objects)} JSONL files."
            )

        finally:
            if self.reader is not None:
                self.reader.close()


if __name__ == "__main__":
    pipeline = JSONLToParquetPipeline()
    pipeline.run()
