"""Small asyncio streaming demo for the beginner path."""

import asyncio
from pathlib import Path

import click
import polars as pl

SAMPLE_RIDES = [
    # These tiny sample events let the demo run even before any Parquet data is downloaded.
    {
        "vendor_id": 1,
        "passenger_count": 1,
        "trip_distance": 1.2,
        "payment_type": 1,
        "total_amount": 14.5,
    },
    {
        "vendor_id": 2,
        "passenger_count": 2,
        "trip_distance": 8.4,
        "payment_type": 1,
        "total_amount": 47.2,
    },
    {
        "vendor_id": 1,
        "passenger_count": 1,
        "trip_distance": 2.3,
        "payment_type": 2,
        "total_amount": 18.8,
    },
    {
        "vendor_id": 2,
        "passenger_count": 3,
        "trip_distance": 12.7,
        "payment_type": 1,
        "total_amount": 63.4,
    },
    {
        "vendor_id": 1,
        "passenger_count": 1,
        "trip_distance": 4.9,
        "payment_type": 2,
        "total_amount": 29.1,
    },
]


def create_ride(row):
    # Convert the Parquet row into a small, predictable dictionary for the demo.
    return {
        "vendor_id": int(row["vendor_id"]),
        "passenger_count": int(row["passenger_count"]),
        "trip_distance": float(row["trip_distance"]),
        "payment_type": int(row["payment_type"]),
        "total_amount": float(row["total_amount"]),
    }


def read_rides_from_parquet(path, limit):
    frame = pl.read_parquet(path).select(
        [
            pl.col("VendorID").cast(pl.Int64).alias("vendor_id"),
            pl.col("passenger_count").fill_null(0).cast(pl.Int64),
            pl.col("trip_distance").fill_null(0.0).cast(pl.Float64),
            pl.col("payment_type").fill_null(0).cast(pl.Int64),
            pl.col("total_amount").fill_null(0.0).cast(pl.Float64),
        ]
    )
    rides = []
    for row in frame.head(limit).iter_rows(named=True):
        rides.append(create_ride(row))
    return rides


def load_rides(path, limit):
    if path is None:
        return [SAMPLE_RIDES[index % len(SAMPLE_RIDES)] for index in range(limit)]

    if not path.exists():
        raise click.ClickException(
            f"Input file not found: {path}. Run src/data_ingestion.py first or omit --input."
        )

    return read_rides_from_parquet(path, limit)


async def producer(queue, rides, delay_seconds):
    for ride in rides:
        click.echo(f"Produced ride total=${ride['total_amount']:.2f}")
        await queue.put(ride)
        await asyncio.sleep(delay_seconds)

    # `None` marks the end of the stream so the consumer can stop cleanly.
    await queue.put(None)


def print_window(window, amount_threshold):
    high_amount_count = sum(ride["total_amount"] >= amount_threshold for ride in window)
    low_amount_count = len(window) - high_amount_count
    average_amount = sum(ride["total_amount"] for ride in window) / len(window)
    average_distance = sum(ride["trip_distance"] for ride in window) / len(window)

    print(
        "Window summary:",
        {
            "events": len(window),
            "high_amount": high_amount_count,
            "low_amount": low_amount_count,
            "avg_total_amount": round(average_amount, 2),
            "avg_trip_distance": round(average_distance, 2),
        },
    )


async def consumer(queue, window_size, amount_threshold):
    window = []

    while True:
        ride = await queue.get()
        if ride is None:
            # Print the last partial window before finishing.
            if window:
                print_window(window, amount_threshold)
            click.echo("Consumer finished.")
            return

        bucket = (
            "high_amount" if ride["total_amount"] >= amount_threshold else "low_amount"
        )
        click.echo(f"Consumed ride -> {bucket}")
        window.append(ride)

        if len(window) == window_size:
            print_window(window, amount_threshold)
            window = []


async def run_demo(input_path, limit, delay_seconds, window_size, amount_threshold):
    rides = load_rides(input_path, limit)
    queue = asyncio.Queue()

    await asyncio.gather(
        producer(queue, rides, delay_seconds),
        consumer(queue, window_size, amount_threshold),
    )


@click.command()
@click.option(
    "--input",
    "input_path",
    default=None,
    type=click.Path(exists=False, path_type=Path),
    help="Optional Parquet file with taxi rides. If omitted, a tiny sample is used.",
)
@click.option(
    "--limit", default=12, type=int, show_default=True, help="Number of rides to emit."
)
@click.option(
    "--delay-seconds",
    default=0.2,
    type=float,
    show_default=True,
    help="Delay between produced messages.",
)
@click.option(
    "--window-size",
    default=4,
    type=int,
    show_default=True,
    help="How many events to include in each printed summary window.",
)
@click.option(
    "--amount-threshold",
    default=40.0,
    type=float,
    show_default=True,
    help="Threshold that splits rides into high and low amount groups.",
)
def main(input_path, limit, delay_seconds, window_size, amount_threshold):
    asyncio.run(
        run_demo(input_path, limit, delay_seconds, window_size, amount_threshold)
    )


if __name__ == "__main__":
    main()
