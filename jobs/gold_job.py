"""
jobs.gold_job
Script AWS Glue PySpark Job untuk mentransformasikan data Silver ke Gold Layer (Star Schema).
"""

import argparse
import logging
import sys
from typing import Any

from pyspark.sql import SparkSession

from nyc_taxi_etl.config import config
from nyc_taxi_etl.gold.dimensions import (
    generate_dim_date,
    transform_dim_location,
)
from nyc_taxi_etl.gold.facts import build_gold_fact_trips

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("gold_job")


def parse_job_arguments() -> dict[str, Any]:
    """Membaca parameter job (mendukung runtime AWS Glue dan Local CLI)."""
    try:
        from awsglue.utils import getResolvedOptions  # type: ignore[import-not-found]

        args = getResolvedOptions(
            sys.argv,
            ["JOB_NAME", "YEAR", "MONTH", "S3_BUCKET"],
        )
        return {
            "job_name": args["JOB_NAME"],
            "year": int(args["YEAR"]),
            "month": int(args["MONTH"]),
            "s3_bucket": args["S3_BUCKET"],
        }
    except ImportError:
        parser = argparse.ArgumentParser(description="Local PySpark Gold Job")
        parser.add_argument(
            "--job_name", default="local_gold_job", help="Job Name"
        )
        parser.add_argument(
            "--year", type=int, default=2024, help="Year (YYYY)"
        )
        parser.add_argument("--month", type=int, default=1, help="Month (MM)")
        parser.add_argument(
            "--s3_bucket", default=config.s3_bucket, help="S3 Bucket Name"
        )
        parsed = parser.parse_args()
        return vars(parsed)


def main() -> None:
    args = parse_job_arguments()
    year = args["year"]
    month = args["month"]
    bucket = args["s3_bucket"]

    logger.info(
        f"Memulai Gold Job untuk Periode: {year:04d}-{month:02d}, Bucket: {bucket}"
    )

    spark = SparkSession.builder.appName(
        args.get("job_name", "gold_job")
    ).getOrCreate()

    # -------------------------------------------------------------
    # 1. Bangun Dimensi Kalender (dim_date) — Rentang 2020 s.d. 2030
    # -------------------------------------------------------------
    gold_dim_date_path = f"s3://{bucket}/{config.gold_prefix}/dim_date"
    logger.info(
        f"Menghasilkan tabel kalender dim_date (2020-2030) ke: {gold_dim_date_path}"
    )
    dim_date_df = generate_dim_date(
        spark, start_date="2020-01-01", end_date="2030-12-31"
    )
    (
        dim_date_df.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(gold_dim_date_path)
    )

    # -------------------------------------------------------------
    # 2. Bangun Dimensi Lokasi (dim_location) dari Silver Lookup
    # -------------------------------------------------------------
    silver_lookup_path = (
        f"s3://{bucket}/{config.silver_prefix}/reference/taxi_zone_lookup"
    )
    gold_dim_location_path = f"s3://{bucket}/{config.gold_prefix}/dim_location"

    logger.info(
        f"Membaca Silver lookup dari {silver_lookup_path} dan menulis ke {gold_dim_location_path}"
    )
    silver_lookup_df = spark.read.parquet(silver_lookup_path)
    dim_location_df = transform_dim_location(silver_lookup_df)
    (
        dim_location_df.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(gold_dim_location_path)
    )

    # -------------------------------------------------------------
    # 3. Bangun Tabel Fakta (fact_yellow_taxi_trip)
    # -------------------------------------------------------------
    silver_trip_path = f"s3://{bucket}/{config.silver_prefix}/yellow/year={year:04d}/month={month:02d}"
    gold_fact_path = f"s3://{bucket}/{config.gold_prefix}/fact_yellow_taxi_trip/year={year:04d}/month={month:02d}"

    logger.info(
        f"Membaca Silver trips dari {silver_trip_path} dan membangun Fact ke {gold_fact_path}"
    )
    silver_trips_df = spark.read.parquet(silver_trip_path)
    fact_trips_df = build_gold_fact_trips(silver_trips_df)

    (
        fact_trips_df.write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(gold_fact_path)
    )

    logger.info("Gold Job selesai dengan sukses!")


if __name__ == "__main__":
    main()