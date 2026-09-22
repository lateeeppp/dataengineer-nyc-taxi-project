"""
nyc_taxi_etl.bronze.ingestion
Modul untuk mengambil raw data NYC Taxi & Lookup CSV dan menyimpannya ke S3 Bronze.
"""

import hashlib
import io
import json
import logging
from datetime import UTC, datetime
from typing import Any

import boto3
import requests
from botocore.config import Config

from nyc_taxi_etl.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_s3_client() -> Any:
    """Membuat client S3 dengan konfigurasi path-style addressing untuk kompatibilitas DNS."""
    s3_config = Config(
        region_name=config.aws_region,
        s3={"addressing_style": "path"},
        retries={"max_attempts": 3, "mode": "standard"},
    )
    return boto3.client("s3", region_name=config.aws_region, config=s3_config)


def stream_download_and_hash(
    url: str, chunk_size: int = 8 * 1024 * 1024
) -> tuple[io.BytesIO, str, int]:
    """Mengunduh data secara streaming, menghitung SHA-256 hash, dan mengembalikan in-memory buffer."""
    logger.info(f"Mengunduh file dari: {url}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    sha256 = hashlib.sha256()
    buffer = io.BytesIO()
    total_bytes = 0

    for chunk in response.iter_content(chunk_size=chunk_size):
        if chunk:
            buffer.write(chunk)
            sha256.update(chunk)
            total_bytes += len(chunk)

    buffer.seek(0)
    checksum = sha256.hexdigest()
    logger.info(f"Selesai mengunduh. Ukuran: {total_bytes} bytes, SHA-256: {checksum}")
    return buffer, checksum, total_bytes


def upload_stream_to_s3(
    buffer: io.BytesIO,
    s3_bucket: str,
    s3_key: str,
    s3_client: Any | None = None,
) -> None:
    """Mengunggah buffer in-memory ke S3 bucket."""
    client = s3_client or get_s3_client()
    logger.info(f"Mengunggah ke S3: s3://{s3_bucket}/{s3_key}")
    client.upload_fileobj(buffer, s3_bucket, s3_key)


def save_manifest_to_s3(
    manifest_data: dict[str, Any],
    s3_bucket: str,
    manifest_s3_key: str,
    s3_client: Any | None = None,
) -> None:
    """Menyimpan metadata manifest JSON ke S3 untuk audit lineage."""
    client = s3_client or get_s3_client()
    manifest_bytes = json.dumps(manifest_data, indent=2).encode("utf-8")
    client.put_object(
        Bucket=s3_bucket,
        Key=manifest_s3_key,
        Body=manifest_bytes,
        ContentType="application/json",
    )
    logger.info(f"Manifest tersimpan di: s3://{s3_bucket}/{manifest_s3_key}")


def ingest_yellow_taxi_monthly(
    year: int,
    month: int,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    """Eksekusi pipeline Bronze untuk data Yellow Taxi bulanan."""
    url = config.get_source_trip_url(year=year, month=month)
    s3_key = config.get_bronze_trip_s3_key(year=year, month=month)
    manifest_key = config.get_bronze_manifest_s3_key(year=year, month=month)

    # 1. Download & hitung checksum
    buffer, checksum, file_size = stream_download_and_hash(url)

    # 2. Upload raw Parquet ke S3 Bronze
    upload_stream_to_s3(buffer, config.s3_bucket, s3_key, s3_client=s3_client)

    # 3. Buat dan simpan Manifest metadata
    manifest_data = {
        "dataset": "yellow_tripdata",
        "year": year,
        "month": month,
        "source_url": url,
        "s3_bucket": config.s3_bucket,
        "s3_key": s3_key,
        "file_size_bytes": file_size,
        "sha256_checksum": checksum,
        "ingested_at_utc": datetime.now(UTC).isoformat(),
        "status": "SUCCESS",
    }
    save_manifest_to_s3(
        manifest_data, config.s3_bucket, manifest_key, s3_client=s3_client
    )

    return manifest_data


def ingest_taxi_zone_lookup(s3_client: Any | None = None) -> dict[str, Any]:
    """Eksekusi pipeline Bronze untuk referensi Taxi Zone Lookup CSV."""
    url = config.zone_lookup_csv_url
    s3_key = config.get_bronze_lookup_s3_key()
    manifest_key = f"{config.bronze_prefix}/reference/taxi_zone_lookup/manifest.json"

    buffer, checksum, file_size = stream_download_and_hash(url)
    upload_stream_to_s3(buffer, config.s3_bucket, s3_key, s3_client=s3_client)

    manifest_data = {
        "dataset": "taxi_zone_lookup",
        "source_url": url,
        "s3_bucket": config.s3_bucket,
        "s3_key": s3_key,
        "file_size_bytes": file_size,
        "sha256_checksum": checksum,
        "ingested_at_utc": datetime.now(UTC).isoformat(),
        "status": "SUCCESS",
    }
    save_manifest_to_s3(
        manifest_data, config.s3_bucket, manifest_key, s3_client=s3_client
    )

    return manifest_data
