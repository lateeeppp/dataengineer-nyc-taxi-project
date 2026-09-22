"""
nyc_taxi_etl.silver.standardization
Transformasi, pembersihan, dan validasi data Silver Layer menggunakan PySpark.
"""

import uuid
from datetime import UTC, datetime
from typing import Any

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame

COLUMN_RENAME_MAPPING = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pu_location_id",
    "DOLocationID": "do_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
}

COLUMN_TYPE_MAPPING = {
    "pickup_datetime": T.TimestampType(),
    "dropoff_datetime": T.TimestampType(),
    "passenger_count": T.IntegerType(),
    "trip_distance": T.DoubleType(),
    "rate_code_id": T.IntegerType(),
    "store_and_fwd_flag": T.StringType(),
    "pu_location_id": T.IntegerType(),
    "do_location_id": T.IntegerType(),
    "payment_type": T.IntegerType(),
    "fare_amount": T.DecimalType(10, 2),
    "extra": T.DecimalType(10, 2),
    "mta_tax": T.DecimalType(10, 2),
    "tip_amount": T.DecimalType(10, 2),
    "tolls_amount": T.DecimalType(10, 2),
    "improvement_surcharge": T.DecimalType(10, 2),
    "total_amount": T.DecimalType(10, 2),
    "congestion_surcharge": T.DoubleType(),
    "airport_fee": T.DoubleType(),
    "vendor_id": T.IntegerType(),
}

# Gunakan nilai Python biasa (bukan F.lit) di tingkat modul
NULL_IMPUTATION_DEFAULTS = {
    "passenger_count": 0,
    "rate_code_id": 99,
    "store_and_fwd_flag": "N",
    "congestion_surcharge": 0.0,
    "airport_fee": 0.0,
}


def standardize_yellow_taxi_columns(df: DataFrame) -> DataFrame:
    """Mengubah nama kolom ke snake_case, menangani NULL imputation, dan casting tipe data secara aman."""
    # 1. Rename kolom ke snake_case
    for old_col, new_col in COLUMN_RENAME_MAPPING.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    # 2. Imputasi NULL aman (F.lit dipanggil di dalam fungsi saat Spark aktif)
    for col_name, default_val in NULL_IMPUTATION_DEFAULTS.items():
        if col_name in df.columns:
            df = df.withColumn(
                col_name, F.coalesce(F.col(col_name), F.lit(default_val))
            )

    # 3. Casting tipe data aman (hanya dijalankan jika kolom ada di DataFrame)
    for col_name, target_type in COLUMN_TYPE_MAPPING.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(target_type))

    return df


def filter_valid_yellow_taxi_trips(
    df: DataFrame,
    year: int,
    month: int,
) -> tuple[DataFrame, dict[str, Any]]:
    """Menyaring anomali, membuang duplikat, dan menghasilkan laporan kualitas data."""
    total_input_rows = df.count()

    # 1. Batas waktu partisi target
    start_date = f"{year:04d}-{month:02d}-01"
    next_month = 1 if month == 12 else month + 1
    next_year = year + 1 if month == 12 else year
    end_date = f"{next_year:04d}-{next_month:02d}-01"

    valid_date_range = (F.col("pickup_datetime") >= F.lit(start_date)) & (
        F.col("pickup_datetime") < F.lit(end_date)
    )

    # 2. Logika waktu fisik: dropoff harus setelah pickup
    valid_duration = F.col("dropoff_datetime") > F.col("pickup_datetime")

    # 3. Logika jarak fisik: jarak >= 0 dan < 500 mil (outlier filter)
    valid_distance = (F.col("trip_distance") >= 0.0) & (F.col("trip_distance") < 500.0)

    # Terapkan filter validasi
    clean_df = df.filter(valid_date_range & valid_duration & valid_distance)

    # 4. Deduplikasi pada 6 kolom komposit bisnis
    clean_df = clean_df.dropDuplicates(
        [
            "vendor_id",
            "pickup_datetime",
            "dropoff_datetime",
            "pu_location_id",
            "do_location_id",
            "total_amount",
        ]
    )

    accepted_rows = clean_df.count()
    rejected_rows = total_input_rows - accepted_rows

    quality_report = {
        "dataset": "yellow_tripdata",
        "year": year,
        "month": month,
        "total_input_rows": total_input_rows,
        "accepted_rows": accepted_rows,
        "rejected_rows": rejected_rows,
        "acceptance_rate_pct": round((accepted_rows / total_input_rows * 100), 2)
        if total_input_rows > 0
        else 0.0,
        "processed_at_utc": datetime.now(UTC).isoformat(),
    }

    return clean_df, quality_report


def enrich_silver_lineage(
    df: DataFrame,
    source_file: str,
    year: int,
    month: int,
    pipeline_run_id: str | None = None,
) -> DataFrame:
    """Menambahkan kolom metadata audit lineage ke DataFrame."""
    run_id = pipeline_run_id or str(uuid.uuid4())
    processed_time = datetime.now(UTC).isoformat()

    return (
        df.withColumn("source_file", F.lit(source_file))
        .withColumn("source_year", F.lit(year).cast(T.ShortType()))
        .withColumn("source_month", F.lit(month).cast(T.ShortType()))
        .withColumn("processed_at_utc", F.lit(processed_time))
        .withColumn("pipeline_run_id", F.lit(run_id))
    )


def transform_taxi_zone_lookup(df: DataFrame) -> DataFrame:
    """Standarisasi dataset referensi Taxi Zone Lookup CSV."""
    mapping = {
        "LocationID": "location_id",
        "Borough": "borough",
        "Zone": "zone",
        "service_zone": "service_zone",
    }
    for old_col, new_col in mapping.items():
        if old_col in df.columns:
            df = df.withColumnRenamed(old_col, new_col)

    return (
        df.withColumn("location_id", F.col("location_id").cast(T.IntegerType()))
        .withColumn(
            "borough",
            F.coalesce(F.col("borough"), F.lit("Unknown")).cast(T.StringType()),
        )
        .withColumn(
            "zone", F.coalesce(F.col("zone"), F.lit("Unknown")).cast(T.StringType())
        )
        .withColumn(
            "service_zone",
            F.coalesce(F.col("service_zone"), F.lit("Unknown")).cast(T.StringType()),
        )
    )
