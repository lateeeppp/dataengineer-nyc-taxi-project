"""
Unit tests untuk transformasi PySpark Silver Layer.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from nyc_taxi_etl.silver.standardization import (
    enrich_silver_lineage,
    filter_valid_yellow_taxi_trips,
    standardize_yellow_taxi_columns,
    transform_taxi_zone_lookup,
)


@pytest.fixture(scope="session")
def spark():
    """Membuat session Spark lokal untuk pengujian pytest."""
    spark_session = (
        SparkSession.builder.master("local[1]")
        .appName("nyc-taxi-test")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def test_standardize_yellow_taxi_columns(spark):
    sample_data = [
        # Baris 1: Terisi lengkap agar PySpark tahu tipe data setiap kolom
        (1, "2024-01-01 10:00:00", "2024-01-01 10:30:00", 2, 2.5, 1, "N", 100, 200, 1, 15.5, 2.5, 1.75),
        # Baris 2: Nilai NULL yang kita uji penanganannya
        (2, "2024-01-01 11:00:00", "2024-01-01 11:30:00", None, 3.0, None, None, 101, 201, 2, 20.0, None, None),
    ]
    columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee",
    ]
    df = spark.createDataFrame(sample_data, columns)
    standardized = standardize_yellow_taxi_columns(df)

    # Ambil baris kedua yang aslinya berisi NULL
    row_null = standardized.collect()[1]
    assert "vendor_id" in standardized.columns
    assert "pu_location_id" in standardized.columns

    # Verifikasi seluruh nilai NULL tertangani menjadi nilai default
    assert row_null["passenger_count"] == 0
    assert row_null["rate_code_id"] == 99
    assert row_null["store_and_fwd_flag"] == "N"
    assert row_null["congestion_surcharge"] == 0.0
    assert row_null["airport_fee"] == 0.0


def test_filter_valid_yellow_taxi_trips(spark):
    sample_data = [
        # 1. Baris VALID
        (1, datetime(2024, 1, 15, 10, 0, tzinfo=UTC), datetime(2024, 1, 15, 10, 30, tzinfo=UTC), 2.5, 100, 200, Decimal("15.50")),
        # 2. INVALID: Dropoff <= Pickup (waktu mundur)
        (2, datetime(2024, 1, 15, 12, 0, tzinfo=UTC), datetime(2024, 1, 15, 11, 0, tzinfo=UTC), 3.0, 101, 201, Decimal("20.00")),
        # 3. INVALID: Jarak negatif
        (1, datetime(2024, 1, 15, 14, 0, tzinfo=UTC), datetime(2024, 1, 15, 14, 20, tzinfo=UTC), -1.0, 102, 202, Decimal("10.00")),
        # 4. INVALID: Tanggal di luar Januari 2024 (misal Februari)
        (1, datetime(2024, 2, 1, 8, 0, tzinfo=UTC), datetime(2024, 2, 1, 8, 30, tzinfo=UTC), 5.0, 100, 200, Decimal("25.00")),
        # 5. DUPLIKAT (identik dengan baris 1)
        (1, datetime(2024, 1, 15, 10, 0, tzinfo=UTC), datetime(2024, 1, 15, 10, 30, tzinfo=UTC), 2.5, 100, 200, Decimal("15.50")),
    ]
    columns = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "trip_distance",
        "pu_location_id",
        "do_location_id",
        "total_amount",
    ]
    df = spark.createDataFrame(sample_data, columns)
    clean_df, metrics = filter_valid_yellow_taxi_trips(df, year=2024, month=1)

    assert metrics["total_input_rows"] == 5
    assert metrics["accepted_rows"] == 1
    assert metrics["rejected_rows"] == 4
    assert clean_df.count() == 1


def test_enrich_silver_lineage(spark):
    df = spark.createDataFrame([(1, "test")], ["id", "val"])
    enriched = enrich_silver_lineage(df, source_file="s3://test.parquet", year=2024, month=1, pipeline_run_id="run-123")

    assert "source_file" in enriched.columns
    assert "source_year" in enriched.columns
    assert "source_month" in enriched.columns
    assert "pipeline_run_id" in enriched.columns
    assert enriched.select("pipeline_run_id").first()[0] == "run-123"


def test_transform_taxi_zone_lookup(spark):
    sample_data = [("1", "Manhattan", "Central Park", "Yellow Zone"), ("2", None, None, None)]
    columns = ["LocationID", "Borough", "Zone", "service_zone"]
    df = spark.createDataFrame(sample_data, columns)
    cleaned = transform_taxi_zone_lookup(df)

    assert cleaned.columns == ["location_id", "borough", "zone", "service_zone"]
    assert isinstance(cleaned.first()["location_id"], int)
    second_row = cleaned.collect()[1]
    assert second_row["borough"] == "Unknown"