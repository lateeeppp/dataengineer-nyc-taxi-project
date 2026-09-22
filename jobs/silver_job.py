"""
jobs.silver_job
Script AWS Glue PySpark Job untuk mentransformasikan data Bronze ke Silver Layer.
"""

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

from nyc_taxi_etl.config import config
from nyc_taxi_etl.silver.standardization import (
    enrich_silver_lineage,
    filter_valid_yellow_taxi_trips,
    standardize_yellow_taxi_columns,
    transform_taxi_zone_lookup,
)
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_job")


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
        # Fallback untuk pengujian lokal dengan argparse standar
        parser = argparse.ArgumentParser(description="Local PySpark Silver Job")
        parser.add_argument("--job_name", default="local_silver_job", help="Job Name")
        parser.add_argument("--year", type=int, default=2024, help="Year (YYYY)")
        parser.add_argument("--month", type=int, default=1, help="Month (MM)")
        parser.add_argument("--s3_bucket", default=config.s3_bucket, help="S3 Bucket Name")
        parsed = parser.parse_args()
        return vars(parsed)


def main() -> None:
    args = parse_job_arguments()
    year = args["year"]
    month = args["month"]
    bucket = args["s3_bucket"]

    logger.info(f"Memulai Silver Job untuk Periode: {year:04d}-{month:02d}, Bucket: {bucket}")

    # Inisialisasi Spark Session
    spark = SparkSession.builder.appName(args.get("job_name", "silver_job")).getOrCreate()

    # -------------------------------------------------------------
    # 1. Transformasi Data Perjalanan (Yellow Taxi)
    # -------------------------------------------------------------
    bronze_trip_path = f"s3://{bucket}/{config.get_bronze_trip_s3_key(year=year, month=month)}"
    silver_trip_path = f"s3://{bucket}/{config.silver_prefix}/yellow/year={year:04d}/month={month:02d}"

    logger.info(f"Membaca data raw trip dari: {bronze_trip_path}")
    raw_trip_df = spark.read.parquet(bronze_trip_path)

    # Eksekusi Transformasi & Pembersihan
    standardized_df = standardize_yellow_taxi_columns(raw_trip_df)
    clean_df, quality_report = filter_valid_yellow_taxi_trips(standardized_df, year=year, month=month)
    final_trip_df = enrich_silver_lineage(
        clean_df,
        source_file=bronze_trip_path,
        year=year,
        month=month,
        pipeline_run_id=args.get("job_name"),
    )

    logger.info(f"Menulis data bersih ke S3 Silver: {silver_trip_path}")
    (
        final_trip_df.write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(silver_trip_path)
    )

    # -------------------------------------------------------------
    # 2. Simpan Laporan Kualitas Data (Quality Gate Report)
    # -------------------------------------------------------------
    logger.info(f"Metrik Kualitas Data: {json.dumps(quality_report, indent=2)}")
    quality_report["uploaded_at_utc"] = datetime.now(UTC).isoformat()

    quality_df = spark.read.json(spark.sparkContext.parallelize([json.dumps(quality_report)]))
    quality_df.coalesce(1).write.mode("overwrite").json(
        f"s3://{bucket}/{config.silver_prefix}/yellow/_quality/year={year:04d}/month={month:02d}"
    )

    # -------------------------------------------------------------
    # 3. Transformasi Data Referensi (Taxi Zone Lookup)
    # -------------------------------------------------------------
    bronze_lookup_path = f"s3://{bucket}/{config.get_bronze_lookup_s3_key()}"
    silver_lookup_path = f"s3://{bucket}/{config.silver_prefix}/reference/taxi_zone_lookup"

    logger.info(f"Membaca data raw lookup dari: {bronze_lookup_path}")
    raw_lookup_df = spark.read.option("header", "true").csv(bronze_lookup_path)
    clean_lookup_df = transform_taxi_zone_lookup(raw_lookup_df)

    logger.info(f"Menulis data lookup bersih ke: {silver_lookup_path}")
    (
        clean_lookup_df.coalesce(1)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(silver_lookup_path)
    )

    logger.info("Silver Job selesai dengan sukses!")


if __name__ == "__main__":
    main()