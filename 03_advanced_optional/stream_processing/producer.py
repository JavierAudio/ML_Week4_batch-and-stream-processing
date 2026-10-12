import time

import polars as pl
from kafka import KafkaProducer
from kafka.serializer import JsonSerializer
from settings import BROKER_URL, INPUT_DATA_PATH, SOURCE_TOPIC

producer = KafkaProducer(
    bootstrap_servers=[BROKER_URL],
    key_serializer=JsonSerializer(),
    value_serializer=JsonSerializer(),
)


def main():
    frame = pl.read_parquet(INPUT_DATA_PATH).select(
        [
            pl.col("VendorID").cast(pl.Int64).alias("vendor_id"),
            pl.col("passenger_count").fill_null(0).cast(pl.Int64),
            pl.col("trip_distance").fill_null(0.0).cast(pl.Float64),
            pl.col("payment_type").fill_null(0).cast(pl.Int64),
            pl.col("total_amount").fill_null(0.0).cast(pl.Float64),
        ]
    )

    for row in frame.head(25).iter_rows(named=True):
        key = {"vendor_id": int(row["vendor_id"])}
        value = {
            "vendor_id": int(row["vendor_id"]),
            "passenger_count": int(row["passenger_count"]),
            "trip_distance": float(row["trip_distance"]),
            "payment_type": int(row["payment_type"]),
            "total_amount": float(row["total_amount"]),
        }
        producer.send(SOURCE_TOPIC, key=key, value=value)
        print(f"Produced: {value}")
        time.sleep(0.3)

    producer.flush()


if __name__ == "__main__":
    main()
