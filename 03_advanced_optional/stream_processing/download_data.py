from pathlib import Path
from urllib.request import urlretrieve

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet"
PROJECT_ROOT = Path(__file__).resolve().parents[2]
OUTPUT_PATH = PROJECT_ROOT / "data/raw/yellow_tripdata_2025-01.parquet"


def main():
    # Reuse the local file by default so repeated runs of the walkthrough stay fast.
    if OUTPUT_PATH.exists():
        print(f"Skipping download: {OUTPUT_PATH} already exists.")
        return

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {URL}")
    urlretrieve(URL, OUTPUT_PATH)
    print(f"Saved {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
