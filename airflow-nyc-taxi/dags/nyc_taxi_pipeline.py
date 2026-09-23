"""
dags/nyc_taxi_pipeline.py
Orkestrasi End-to-End Pipeline NYC Yellow Taxi (Bronze -> Silver -> Gold -> Redshift).
Mendukung Single Run, Backfill Rentang Tanggal, dan Eksekusi Terjadwal secara 100% Idempotent.
"""

import hashlib
import json
import logging
import urllib.request
from datetime import datetime, timedelta, timezone
import urllib.error

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.exceptions import AirflowSkipException

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

S3_BUCKET = "nyc-taxi-bucket-lathief"
AWS_CONN_ID = "aws_default"
REGION = "ap-southeast-3"
WORKGROUP_NAME = "nyc-taxi-workgroup"
DATABASE_NAME = "dev"


def get_target_period(context: dict) -> tuple[int, int]:
    """
    Mengambil tahun dan bulan secara cerdas:
    1. Dari JSON config jika ada.
    2. Dari Logical Date (picker UI / Backfill scheduler).
    """
    dag_run = context.get("dag_run")
    conf = dag_run.conf if dag_run else {}
    logical_date = context.get("logical_date") or context.get(
        "data_interval_start"
    )

    # Prioritas 1: Input JSON manual jika user memasukkannya
    if "year" in conf and "month" in conf:
        return int(conf["year"]), int(conf["month"])

    # Prioritas 2: Otomatis dari Logical Date (Single Run Picker / Backfill)
    if logical_date:
        return logical_date.year, logical_date.month

    return 2024, 1


def ingest_bronze_trip_data(bucket: str = S3_BUCKET, **context) -> None:
    """Mengunduh data Parquet dari TLC jika belum ada di S3 Bronze.

    Jika data di web sumber belum dirilis (404/403), skip seluruh pipeline
    secara graceful.
    """
    year, month = get_target_period(context)
    logging.info(
        f"--- MEMULAI INGEST BRONZE UNTUK PERIODE: {year:04d}-{month:02d} ---"
    )

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    s3_key = f"bronze/nyc_taxi/yellow/year={year:04d}/month={month:02d}/yellow_tripdata_{year:04d}-{month:02d}.parquet"
    manifest_key = f"bronze/nyc_taxi/yellow/manifests/year={year:04d}/month={month:02d}.json"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year:04d}-{month:02d}.parquet"

    # 1. Idempotency Gate (Success Marker)
    if s3_hook.check_for_key(
        key=manifest_key, bucket_name=bucket
    ) and s3_hook.check_for_key(key=s3_key, bucket_name=bucket):
        logging.info(
            f"Data dan Manifest untuk {year}-{month} sudah lengkap di S3 Bronze. Melewati proses unduh."
        )
        return

    # 2. Coba unduh dari URL TLC
    logging.info(f"Mengunduh data baru dari: {url}")
    sha256 = hashlib.sha256()
    temp_path = f"/tmp/yellow_tripdata_{year:04d}-{month:02d}.parquet"

    try:
        with (
            urllib.request.urlopen(url) as response,
            open(temp_path, "wb") as f,
        ):
            while chunk := response.read(1024 * 1024):
                sha256.update(chunk)
                f.write(chunk)
    except urllib.error.HTTPError as e:
        if e.code in [403, 404]:
            logging.warning(
                f"Data sumber di {url} belum dirilis oleh NYC TLC (HTTP {e.code})."
            )
            # SKIPPING GRACEFULLY: Menghentikan seluruh task berikutnya tanpa error!
            raise AirflowSkipException(
                f"Data Yellow Taxi untuk periode {year:04d}-{month:02d} belum tersedia di sumber. Melewati pipeline."
            )
        raise

    file_hash = sha256.hexdigest()
    logging.info(
        f"Unduhan selesai. SHA-256: {file_hash}. Mengunggah ke S3 Bronze..."
    )

    s3_hook.load_file(
        filename=temp_path,
        key=s3_key,
        bucket_name=bucket,
        replace=True,
    )

    # 3. Tulis audit manifest JSON
    manifest_data = {
        "dataset": "yellow_tripdata",
        "year": year,
        "month": month,
        "source_url": url,
        "s3_key": s3_key,
        "sha256": file_hash,
        "ingested_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    s3_hook.load_string(
        string_data=json.dumps(manifest_data, indent=2),
        key=manifest_key,
        bucket_name=bucket,
        replace=True,
    )
    logging.info(f"Manifest tersimpan di s3://{bucket}/{manifest_key}")


def verify_silver_quality_gate(bucket: str = S3_BUCKET, **context) -> None:
    """Membaca laporan kualitas data di S3 Silver dan bertindak sebagai Circuit Breaker."""
    year, month = get_target_period(context)
    logging.info(
        f"--- VERIFIKASI QUALITY GATE UNTUK PERIODE: {year:04d}-{month:02d} ---"
    )

    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    keys = s3_hook.list_keys(
        bucket_name=bucket,
        prefix=f"silver/nyc_taxi/yellow/_quality/year={year:04d}/month={month:02d}/",
    )

    if not keys:
        raise ValueError(
            f"Quality report tidak ditemukan untuk periode {year}-{month}!"
        )

    json_key = [k for k in keys if k.endswith(".json")][0]
    report_content = s3_hook.read_key(key=json_key, bucket_name=bucket)
    report = json.loads(report_content)

    logging.info(f"Laporan Kualitas Data Silver: {json.dumps(report, indent=2)}")

    accepted_rows = report.get("accepted_rows", 0)
    rejection_rate = 100.0 - report.get("acceptance_rate_pct", 0.0)

    # Circuit Breaker: Minimal 1000 baris dan rejection rate < 5%
    if accepted_rows < 1000:
        raise ValueError(
            f"Quality Gate GAGAL: Jumlah baris valid terlalu sedikit ({accepted_rows} baris)."
        )

    if rejection_rate > 5.0:
        raise ValueError(
            f"Quality Gate GAGAL: Rejection rate terlalu tinggi ({rejection_rate}% > 5%)."
        )

    logging.info(
        f"Quality Gate LULUS! {accepted_rows} baris siap dimuat ke Gold."
    )


def load_gold_to_redshift(bucket: str = S3_BUCKET, **context) -> None:
    """Mengeksekusi pemuatan data ke Redshift Serverless secara Scoped Partition Idempotent."""
    year, month = get_target_period(context)
    logging.info(
        f"--- PEMUATAN REDSHIFT UNTUK PERIODE: {year:04d}-{month:02d} ---"
    )

    redshift_hook = RedshiftDataHook(aws_conn_id=AWS_CONN_ID)

    idempotent_sql = f"""
    -- HANYA menghapus bulan dan tahun target. Bulan lain 100% UTUH!
    delete from nyc_taxi_gold.fact_yellow_taxi_trip 
    where source_year = {year} and source_month = {month};

    copy nyc_taxi_gold.fact_yellow_taxi_trip (
        pickup_date_key, dropoff_date_key, pickup_location_key, dropoff_location_key,
        rate_code_key, payment_type_key, vendor_key, store_and_fwd_flag,
        pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
        trip_duration_seconds, fare_amount, extra, mta_tax, tip_amount,
        tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee,
        total_amount, source_year, source_month, pipeline_run_id, inserted_at_utc
    )
    from 's3://{bucket}/gold/nyc_taxi/fact_yellow_taxi_trip/year={year:04d}/month={month:02d}/'
    iam_role default
    format as parquet;
    """

    response = redshift_hook.execute_query(
        sql=idempotent_sql,
        database=DATABASE_NAME,
        workgroup_name=WORKGROUP_NAME,
        wait_for_completion=True,
    )
    logging.info(f"Pemuatan Scoped Partition Redshift BERHASIL! Respons: {response}")


with DAG(
    dag_id="nyc_taxi_end_to_end_pipeline",
    default_args=default_args,
    description="Pipeline Otomatis NYC Yellow Taxi (Dinamis): Bronze -> Silver -> Quality Gate -> Gold -> Redshift",
    schedule="@monthly",
    start_date=datetime(2020, 1, 1),
    end_date=datetime(2026, 12, 31),
    catchup=False,
    tags=["nyc-taxi", "glue", "redshift", "kimball"],
) as dag:
    # 1. Ingest Raw Parquet dari TLC ke S3 Bronze
    task_ingest_bronze = PythonOperator(
        task_id="ingest_bronze_trip_data",
        python_callable=ingest_bronze_trip_data,
    )

    # 2. Trigger AWS Glue Silver Job (Dynamic Script Args via Jinja)
    task_glue_silver = GlueJobOperator(
        task_id="trigger_glue_silver_job",
        job_name="nyc-taxi-silver-job",
        script_args={
            "--YEAR": "{{ (dag_run.conf.get('year') if dag_run and dag_run.conf and 'year' in dag_run.conf else logical_date.year) }}",
            "--MONTH": "{{ (dag_run.conf.get('month') if dag_run and dag_run.conf and 'month' in dag_run.conf else logical_date.month) }}",
            "--S3_BUCKET": S3_BUCKET,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION,
        wait_for_completion=True,
    )

    # 3. Quality Gate Verification (Circuit Breaker)
    task_quality_gate = PythonOperator(
        task_id="verify_silver_quality_gate",
        python_callable=verify_silver_quality_gate,
    )

    # 4. Trigger AWS Glue Gold Job (Dynamic Script Args via Jinja)
    task_glue_gold = GlueJobOperator(
        task_id="trigger_glue_gold_job",
        job_name="nyc-taxi-gold-job",
        script_args={
            "--YEAR": "{{ (dag_run.conf.get('year') if dag_run and dag_run.conf and 'year' in dag_run.conf else logical_date.year) }}",
            "--MONTH": "{{ (dag_run.conf.get('month') if dag_run and dag_run.conf and 'month' in dag_run.conf else logical_date.month) }}",
            "--S3_BUCKET": S3_BUCKET,
        },
        aws_conn_id=AWS_CONN_ID,
        region_name=REGION,
        wait_for_completion=True,
    )

    # 5. Load Data ke Redshift Serverless
    task_load_redshift = PythonOperator(
        task_id="load_gold_to_redshift",
        python_callable=load_gold_to_redshift,
    )

    # Urutan Eksekusi
    (
        task_ingest_bronze
        >> task_glue_silver
        >> task_quality_gate
        >> task_glue_gold
        >> task_load_redshift
    )