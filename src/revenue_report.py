"""Create a monthly revenue report from local NYC taxi Parquet files."""

from pathlib import Path

import click
import duckdb
import pyarrow.parquet as pq

REPORT_SQL = """
WITH trips_data AS (
    SELECT
        PULocationID,
        CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
        passenger_count,
        trip_distance,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge
    FROM read_parquet(?)
)
SELECT
    PULocationID AS revenue_zone,
    date_trunc('month', pickup_datetime) AS revenue_month,
    SUM(fare_amount) AS revenue_monthly_fare,
    SUM(extra) AS revenue_monthly_extra,
    SUM(mta_tax) AS revenue_monthly_mta_tax,
    SUM(tip_amount) AS revenue_monthly_tip_amount,
    SUM(tolls_amount) AS revenue_monthly_tolls_amount,
    SUM(improvement_surcharge) AS revenue_monthly_improvement_surcharge,
    SUM(total_amount) AS revenue_monthly_total_amount,
    SUM(congestion_surcharge) AS revenue_monthly_congestion_surcharge,
    AVG(passenger_count) AS avg_monthly_passenger_count,
    AVG(trip_distance) AS avg_monthly_trip_distance
FROM trips_data
GROUP BY 1, 2
ORDER BY revenue_month, revenue_zone
"""


@click.command()
@click.option(
    "--input",
    "input_pattern",
    required=True,
    help="A Parquet file path or glob, for example data/raw/yellow_tripdata_2025-*.parquet.",
)
@click.option(
    "--output",
    default="data/output/revenue_report.parquet",
    show_default=True,
    help="Where the report Parquet file should be written.",
)
def main(input_pattern, output):
    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    connection = duckdb.connect()
    # DuckDB can query Parquet files directly, so we do not need to load them into a
    # separate database first.
    report_table = connection.execute(REPORT_SQL, [input_pattern]).to_arrow_table()
    pq.write_table(report_table, output_path)

    row_count = report_table.num_rows
    click.echo(f"Wrote {row_count} rows to {output_path}")


if __name__ == "__main__":
    main()
