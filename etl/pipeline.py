from etl.bronze_reader import BronzeReader
from etl.transformer import Transformer
from etl.silver_writer import SilverWriter


class ETLPipeline:

    def __init__(self):

        self.reader = BronzeReader()
        self.transformer = Transformer()
        self.writer = SilverWriter()

    def run(self):

        df = self.reader.read()

        print("Bronze data:")
        df.printSchema()
        df.show(5, truncate=False)

        df = self.transformer.transform(df)

        print("Transformed data:")
        df.printSchema()
        df.show(5, truncate=False)

        self.writer.save(df)