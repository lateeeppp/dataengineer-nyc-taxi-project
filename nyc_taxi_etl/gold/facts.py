"""
nyc_taxi_etl.gold.facts
Logika pembentukan dataset Tabel Fakta Kimball Star Schema.
"""

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame


def build_gold_fact_trips(silver_trips_df: DataFrame) -> DataFrame:
    """
    Mengubah DataFrame Silver menjadi skema Fact Yellow Taxi untuk Redshift.
    Melakukan mapping Foreign Keys, durasi trip, dan perlindungan Key 0 (Unknown).
    """
    # 1. Hitung surrogate date keys (Format YYYYMMDD integer)
    pickup_date_key = F.date_format("pickup_datetime", "yyyyMMdd").cast(
        T.IntegerType()
    )
    dropoff_date_key = F.date_format("dropoff_datetime", "yyyyMMdd").cast(
        T.IntegerType()
    )

    # 2. Hitung durasi perjalanan dalam detik (Additive metric)
    duration_seconds = (
        F.unix_timestamp("dropoff_datetime")
        - F.unix_timestamp("pickup_datetime")
    ).cast(T.LongType())

    # 3. Mapping aman kode referensi ke Dimensi dengan proteksi fallback ke 0 (Unknown)
    vendor_key = (
        F.when(F.col("vendor_id").isin(1, 2), F.col("vendor_id"))
        .otherwise(F.lit(0))
        .cast(T.IntegerType())
    )

    rate_code_key = (
        F.when(F.col("rate_code_id").isin(1, 2, 3, 4, 5, 6), F.col("rate_code_id"))
        .otherwise(F.lit(0))
        .cast(T.IntegerType())
    )

    payment_type_key = (
        F.when(
            F.col("payment_type").isin(1, 2, 3, 4, 5, 6), F.col("payment_type")
        )
        .otherwise(F.lit(0))
        .cast(T.IntegerType())
    )

    # Location keys (Bila null atau out of range, fallback ke 0)
    pickup_loc_key = (
        F.coalesce(F.col("pu_location_id"), F.lit(0)).cast(T.IntegerType())
    )
    dropoff_loc_key = (
        F.coalesce(F.col("do_location_id"), F.lit(0)).cast(T.IntegerType())
    )

    # 4. Proyeksi akhir kolom persis sesuai DDL fact_yellow_taxi_trip di Redshift
    return silver_trips_df.select(
        # Foreign Keys
        pickup_date_key.alias("pickup_date_key"),
        dropoff_date_key.alias("dropoff_date_key"),
        pickup_loc_key.alias("pickup_location_key"),
        dropoff_loc_key.alias("dropoff_location_key"),
        rate_code_key.alias("rate_code_key"),
        payment_type_key.alias("payment_type_key"),
        vendor_key.alias("vendor_key"),
        # Degenerate & Timestamps
        F.col("store_and_fwd_flag").cast(T.StringType()),
        F.col("pickup_datetime").cast(T.TimestampType()),
        F.col("dropoff_datetime").cast(T.TimestampType()),
        # Measures
        F.col("passenger_count").cast(T.IntegerType()),
        F.col("trip_distance").cast(T.DoubleType()),
        duration_seconds.alias("trip_duration_seconds"),
        F.col("fare_amount").cast(T.DecimalType(10, 2)),
        F.col("extra").cast(T.DecimalType(10, 2)),
        F.col("mta_tax").cast(T.DecimalType(10, 2)),
        F.col("tip_amount").cast(T.DecimalType(10, 2)),
        F.col("tolls_amount").cast(T.DecimalType(10, 2)),
        F.col("improvement_surcharge").cast(T.DecimalType(10, 2)),
        F.col("congestion_surcharge").cast(T.DoubleType()),
        F.col("airport_fee").cast(T.DoubleType()),
        F.col("total_amount").cast(T.DecimalType(10, 2)),
        # Audit Lineage
        F.col("source_year").cast(T.ShortType()),
        F.col("source_month").cast(T.ShortType()),
        F.col("pipeline_run_id").cast(T.StringType()),
        F.current_timestamp().alias("inserted_at_utc"),
    )