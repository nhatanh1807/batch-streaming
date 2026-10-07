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

    def run(self):

        # 1. Read raw JSONL files
        raw_df = self.reader.read()

        print("Raw Bronze data:")
        raw_df.printSchema()
        raw_df.show(5, truncate=False)

        # 2. Write raw data to PostgreSQL Bronze
        self.bronze_writer.write(raw_df)

        # 3. Transform raw data
        transformed_df = self.transformer.transform(raw_df)

        print("Transformed Silver data:")
        transformed_df.printSchema()
        transformed_df.show(5, truncate=False)

        # 4. Write transformed data to PostgreSQL Silver
        self.silver_writer.save(transformed_df)

        print("ETL pipeline completed successfully.")