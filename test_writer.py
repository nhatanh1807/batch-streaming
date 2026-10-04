from consumer.writer import BronzeWriter


records = [
    {
        "trade_id": 999999999,
        "event_time": 1750000000000,
        "symbol": "BTCUSDT",
        "price": "110000.50",
        "quantity": "0.001",
        "is_buyer_maker": False,
    }
]


writer = BronzeWriter()
writer.save(records)