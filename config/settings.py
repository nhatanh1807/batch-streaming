from pathlib import Path
import os


BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"

BRONZE_DIR = DATA_DIR / "bronze"
PROCESSED_DIR = DATA_DIR / "processed"
SILVER_DIR = DATA_DIR / "silver"

SAVE_INTERVAL = 10

BINANCE_SOCKET = "wss://stream.binance.com:9443/ws/btcusdt@trade"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "batch_streaming")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")