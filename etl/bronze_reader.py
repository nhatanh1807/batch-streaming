from pyspark.sql import SparkSession

from config.settings import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)


class BronzeReader:

    def __init__(self):

        self.spark = (
            SparkSession.builder
            .master("local[*]")
            .appName("Binance ETL")
            .config(
                "spark.jars.packages",
                "org.postgresql:postgresql:42.7.8"
            )
            .getOrCreate()
        )

    def read(self):

        jdbc_url = (
            f"jdbc:postgresql://"
            f"{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )

        return (
            self.spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", "bronze.trades")
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver", "org.postgresql.Driver")
            .load()
        )