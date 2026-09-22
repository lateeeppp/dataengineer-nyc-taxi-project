"""
Unit tests untuk modul Bronze Ingestion (menggunakan Mocking).
"""

import io
from unittest.mock import MagicMock, patch

from nyc_taxi_etl.bronze.ingestion import (
    ingest_yellow_taxi_monthly,
    stream_download_and_hash,
    upload_stream_to_s3,
)


def test_stream_download_and_hash():
    fake_content = b"sample_parquet_binary_content"
    mock_response = MagicMock()
    mock_response.iter_content.return_value = [fake_content]
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response):
        buf, checksum, size = stream_download_and_hash(
            "http://fake-url.com/data.parquet"
        )

        assert size == len(fake_content)
        assert len(checksum) == 64
        assert buf.getvalue() == fake_content


def test_upload_stream_to_s3():
    mock_s3 = MagicMock()
    buf = io.BytesIO(b"test data")

    upload_stream_to_s3(buf, "test-bucket", "bronze/test.parquet", s3_client=mock_s3)
    mock_s3.upload_fileobj.assert_called_once_with(
        buf, "test-bucket", "bronze/test.parquet"
    )


@patch("nyc_taxi_etl.bronze.ingestion.stream_download_and_hash")
@patch("nyc_taxi_etl.bronze.ingestion.upload_stream_to_s3")
@patch("nyc_taxi_etl.bronze.ingestion.save_manifest_to_s3")
def test_ingest_yellow_taxi_monthly(mock_save_manifest, mock_upload, mock_download):
    mock_download.return_value = (io.BytesIO(b"fake parquet"), "fake_hash_12345", 1024)

    result = ingest_yellow_taxi_monthly(year=2024, month=1)

    assert result["dataset"] == "yellow_tripdata"
    assert result["year"] == 2024
    assert result["month"] == 1
    assert result["sha256_checksum"] == "fake_hash_12345"
    assert result["status"] == "SUCCESS"
    mock_upload.assert_called_once()
    mock_save_manifest.assert_called_once()
