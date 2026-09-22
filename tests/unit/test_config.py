"""
Unit tests untuk modul konfigurasi nyc_taxi_etl.config
"""

from nyc_taxi_etl.config import PipelineConfig


def test_default_config_values():
    cfg = PipelineConfig()
    assert cfg.aws_region == "ap-southeast-3"
    assert cfg.s3_bucket == "nyc-taxi-bucket-lathief"
    assert "yellow_tripdata" in cfg.taxi_parquet_url_template


def test_get_source_trip_url():
    cfg = PipelineConfig()
    url = cfg.get_source_trip_url(year=2024, month=1)
    expected = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    assert url == expected


def test_get_bronze_trip_s3_key():
    cfg = PipelineConfig()
    key = cfg.get_bronze_trip_s3_key(year=2024, month=1)
    expected = "bronze/nyc_taxi/yellow/year=2024/month=01/yellow_tripdata_2024-01.parquet"
    assert key == expected