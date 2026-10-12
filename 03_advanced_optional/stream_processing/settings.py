from pathlib import Path

BROKER_URL = "localhost:19092"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
INPUT_DATA_PATH = PROJECT_ROOT / "data/raw/yellow_tripdata_2025-01.parquet"
SOURCE_TOPIC = "rides_json"
HIGH_AMOUNT_TOPIC = "rides_high_amount"
LOW_AMOUNT_TOPIC = "rides_low_amount"
