
from etl.bronze_reader import BronzeReader
from etl.bronze_writer import BronzeWriter
from etl.transformer import Transformer
from etl.silver_writer import SilverWriter


class ETLPipeline:

    def __init__(self):
        self.reader = BronzeReader()
        self.bronze_writer = BronzeWriter()
        self.transformer = Transformer()
        self.silver_writer = SilverWriter()

    def _save_checkpoint(self):
        objects = self.reader.pending_objects

        if not objects:
            return

        with self.reader._get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO public.etl_file_checkpoint (
                        object_key,
                        etag,
                        processed_at
                    )
                    VALUES (%s, %s, NOW())
                    ON CONFLICT (object_key)
                    DO UPDATE SET
                        etag = EXCLUDED.etag,
                        processed_at = NOW();
                    """,
                    [
                        (obj["object_key"], obj["etag"])
                        for obj in objects
                    ],
                )

        print(f"Checkpoint saved for {len(objects)} files.")

    def run(self):
        raw_df = self.reader.read()

        if raw_df is None:
            print("ETL skipped: no new files.")
            return

        try:
            print("Raw Bronze data:")
            raw_df.printSchema()
            raw_df.show(5, truncate=False)

            # Bronze
            self.bronze_writer.write(raw_df)

            # Transform
            transformed_df = self.transformer.transform(raw_df)

            print("Transformed Silver data:")
            transformed_df.printSchema()
            transformed_df.show(5, truncate=False)

            # Silver
            self.silver_writer.save(transformed_df)

            # Checkpoint only after both writers succeed
            self._save_checkpoint()

            print("ETL pipeline completed successfully.")

        finally:
            self.reader.spark.catalog.clearCache()
            self.reader.temp_dir.cleanup()
            self.reader.spark.stop()
