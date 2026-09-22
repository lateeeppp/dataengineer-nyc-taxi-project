"""
nyc_taxi_etl.config
Konfigurasi terpusat untuk pipeline NYC Yellow Taxi ETL.
"""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class PipelineConfig:
    # AWS Settings
    aws_region: str = os.getenv("AWS_DEFAULT_REGION", "ap-southeast-3")
    s3_bucket: str = os.getenv("S3_BUCKET_NAME", "nyc-taxi-bucket-lathief")

    # Data Source URLs
    taxi_parquet_url_template: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    zone_lookup_csv_url: str = (
        "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
    )

    # S3 Prefixes (Medallion Architecture)
    bronze_prefix: str = "bronze/nyc_taxi"
    silver_prefix: str = "silver/nyc_taxi"
    gold_prefix: str = "gold/nyc_taxi"

    # Redshift Settings
    redshift_schema: str = "nyc_taxi_gold"

    def get_bronze_trip_s3_key(self, year: int, month: int) -> str:
        """Menghasilkan S3 key deterministik untuk raw Yellow Taxi Parquet di Bronze."""
        return f"{self.bronze_prefix}/yellow/year={year:04d}/month={month:02d}/yellow_tripdata_{year:04d}-{month:02d}.parquet"

    def get_bronze_manifest_s3_key(self, year: int, month: int) -> str:
        """Menghasilkan S3 key untuk manifest checksum Bronze."""
        return f"{self.bronze_prefix}/yellow/manifests/year={year:04d}/month={month:02d}.json"

    def get_bronze_lookup_s3_key(self) -> str:
        """Menghasilkan S3 key untuk Taxi Zone Lookup CSV di Bronze."""
        return f"{self.bronze_prefix}/reference/taxi_zone_lookup/taxi_zone_lookup.csv"

    def get_silver_trip_s3_path(self, year: int, month: int) -> str:
        """Menghasilkan S3 path untuk Silver Parquet."""
        return f"s3://{self.s3_bucket}/{self.silver_prefix}/yellow/year={year:04d}/month={month:02d}"

    def get_source_trip_url(self, year: int, month: int) -> str:
        """Menghasilkan URL publik TLC untuk bulan yang diminta."""
        return self.taxi_parquet_url_template.format(year=year, month=month)


# Singleton instance default
config = PipelineConfig()
