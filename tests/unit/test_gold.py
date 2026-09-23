"""
Unit tests untuk transformasi PySpark Gold Layer (Star Schema).
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from nyc_taxi_etl.gold.dimensions import (
    generate_dim_date,
    transform_dim_location,
)
from nyc_taxi_etl.gold.facts import build_gold_fact_trips


@pytest.fixture(scope="session")
def spark():
    """Membuat session Spark lokal untuk pengujian pytest."""
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("nyc-taxi-gold-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_generate_dim_date(spark):
    df_date = generate_dim_date(
        spark, start_date="2024-01-01", end_date="2024-01-07"
    )
    rows = df_date.collect()

    assert df_date.count() == 7
    # 2024-01-01 adalah Senin (day_of_week = 1, is_weekend = False)
    assert rows[0]["date_key"] == 20240101
    assert rows[0]["day_of_week"] == 1
    assert rows[0]["day_name"] == "Monday"
    assert rows[0]["is_weekend"] is False

    # 2024-01-07 adalah Minggu (day_of_week = 7, is_weekend = True)
    assert rows[6]["date_key"] == 20240107
    assert rows[6]["day_of_week"] == 7
    assert rows[6]["is_weekend"] is True


def test_transform_dim_location(spark):
    sample_data = [
        (1, "Manhattan", "Central Park", "Yellow Zone"),
        (1, "Manhattan", "Central Park", "Yellow Zone"),  # Duplikat
        (2, "Queens", "JFK Airport", "Airports"),
    ]
    columns = ["location_id", "borough", "zone", "service_zone"]
    df = spark.createDataFrame(sample_data, columns)
    transformed = transform_dim_location(df)

    assert transformed.count() == 2
    assert set(transformed.columns) == {
        "location_key",
        "location_id",
        "borough",
        "zone",
        "service_zone",
    }
    first_row = transformed.filter("location_id = 1").first()
    assert first_row["location_key"] == 1


def test_build_gold_fact_trips(spark):
    sample_data = [
        (
            1,  # vendor_id
            datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 15, 10, 20, 0, tzinfo=UTC),
            2,  # passenger_count
            3.5,  # trip_distance
            1,  # rate_code_id (Standard)
            "N",
            100,  # pu_location_id
            200,  # do_location_id
            1,  # payment_type (Credit Card)
            Decimal("15.00"),
            Decimal("0.00"),
            Decimal("0.50"),
            Decimal("3.00"),
            Decimal("0.00"),
            Decimal("0.30"),
            Decimal("18.80"),
            2.50,
            1.25,
            2024,
            1,
            "run-abc-123",
        ),
        (
            999,  # vendor_id tak dikenal (harus fallback ke key 0)
            datetime(2024, 1, 20, 14, 0, 0, tzinfo=UTC),
            datetime(2024, 1, 20, 14, 10, 0, tzinfo=UTC),
            1,
            1.2,
            99,  # rate_code_id unknown (harus fallback ke key 0)
            "N",
            None,  # pu null (harus fallback ke key 0)
            205,
            888,  # payment_type tak dikenal (harus fallback ke key 0)
            Decimal("10.00"),
            Decimal("0.00"),
            Decimal("0.50"),
            Decimal("0.00"),
            Decimal("0.00"),
            Decimal("0.30"),
            Decimal("10.80"),
            0.0,
            0.0,
            2024,
            1,
            "run-abc-123",
        ),
    ]
    columns = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_fwd_flag",
        "pu_location_id",
        "do_location_id",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "source_year",
        "source_month",
        "pipeline_run_id",
    ]
    df = spark.createDataFrame(sample_data, columns)
    fact_df = build_gold_fact_trips(df)

    rows = fact_df.collect()

    # Baris 1: Transaksi Normal
    assert rows[0]["pickup_date_key"] == 20240115
    assert rows[0]["dropoff_date_key"] == 20240115
    assert rows[0]["trip_duration_seconds"] == 1200  # 20 menit = 1200 detik
    assert rows[0]["vendor_key"] == 1
    assert rows[0]["rate_code_key"] == 1
    assert rows[0]["payment_type_key"] == 1
    assert rows[0]["pickup_location_key"] == 100

    # Baris 2: Transaksi Unknown/Anomali (Semua mapped ke Key 0)
    assert rows[1]["vendor_key"] == 0
    assert rows[1]["rate_code_key"] == 0
    assert rows[1]["payment_type_key"] == 0
    assert rows[1]["pickup_location_key"] == 0
    assert rows[1]["trip_duration_seconds"] == 600  # 10 menit = 600 detik