import glob

from pyspark.sql import SparkSession

from config.settings import BRONZE_DIR
from etl.schema import trade_schema


class BronzeReader:

    def __init__(self):

        self.spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("Binance ETL")
            .config(
                "spark.jars",
                "/opt/jdbc/postgresql-42.7.8.jar"
            )
            .getOrCreate()
        )

    def read(self):

        files = glob.glob(
            str(BRONZE_DIR / "*.jsonl")
        )

        if not files:
            raise FileNotFoundError(
                f"No Bronze files found in {BRONZE_DIR}"
            )

        print(
            f"Reading {len(files)} raw Bronze files..."
        )

        return (
            self.spark.read
            .schema(trade_schema)
            .json(files)
        )