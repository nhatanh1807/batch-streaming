import json
import time

from config.settings import SAVE_INTERVAL, BINANCE_SOCKET
from consumer.writer import BronzeWriter
from consumer.websocket_client import BinanceClient


class Consumer:

    def __init__(self):

        self.buffer = []
        self.last_save = time.time()
        self.writer = BronzeWriter()

    def normalize_trade(self, record):

        return {
            "trade_id": record["t"],
            "event_time": record["T"],
            "symbol": record["s"],
            "price": record["p"],
            "quantity": record["q"],
            "is_buyer_maker": record["m"],
        }

    def on_message(self, ws, message):

        raw_record = json.loads(message)

        record = self.normalize_trade(raw_record)

        self.buffer.append(record)

        if time.time() - self.last_save >= SAVE_INTERVAL:

            print(
                f"Writing {len(self.buffer)} records to PostgreSQL..."
            )

            self.writer.save(self.buffer)

            self.buffer = []

            self.last_save = time.time()

    def run(self):

        print("Starting Binance consumer...")
        print("Connecting to Binance:", BINANCE_SOCKET)

        client = BinanceClient(
            BINANCE_SOCKET,
            self.on_message
        )

        client.start()