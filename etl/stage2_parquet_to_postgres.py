from etl.parquet_reader import ParquetReader
from etl.silver_writer import SilverWriter
from etl.checkpoint_manager import CheckpointManager


CHECKPOINT_TABLE = "public.silver_postgres_checkpoint"


class ParquetToPostgresPipeline:

    def __init__(self):
        self.reader = None
        self.silver_writer = SilverWriter()
        self.checkpoint_manager = CheckpointManager()

    def run(self):
        self.checkpoint_manager.create_tables()

        self.reader = ParquetReader()

        try:
            batches = self.reader.list_pending_batches()

            if not batches:
                print("Stage 2 skipped: no new Parquet batches.")
                return

            print(f"Stage 2 pending batches: {len(batches)}")

            for batch in batches:
                print("=" * 60)
                print(f"Processing batch: {batch['batch_id']}")

                df = self.reader.read_batch(batch)

                row_count = df.count()

                print(
                    f"Batch {batch['batch_id']} "
                    f"contains {row_count:,} rows."
                )

                if row_count == 0:
                    print(
                        "Empty batch. Saving checkpoint and skipping write."
                    )

                    self.checkpoint_manager.save_checkpoint(
                        table_name=CHECKPOINT_TABLE,
                        object_key=batch["marker_key"],
                        etag=batch["etag"],
                    )

                    continue

                # PostgreSQL write and merge.
                self.silver_writer.save(df)

                # Checkpoint only after successful PostgreSQL write.
                self.checkpoint_manager.save_checkpoint(
                    table_name=CHECKPOINT_TABLE,
                    object_key=batch["marker_key"],
                    etag=batch["etag"],
                )

                print(
                    f"Batch completed successfully: "
                    f"{batch['batch_id']}"
                )

            print("Stage 2 completed successfully.")

        finally:
            if self.reader is not None:
                self.reader.close()


if __name__ == "__main__":
    pipeline = ParquetToPostgresPipeline()
    pipeline.run()
