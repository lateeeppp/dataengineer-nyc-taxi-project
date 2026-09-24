# NYC Yellow Taxi End-to-End Cloud Data Pipeline

### Production-Grade Medallion Architecture & Kimball Star Schema on AWS

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Airflow](https://img.shields.io/badge/orchestrator-Apache%20Airflow%203.x-017CEE.svg)](https://airflow.apache.org/)
[![Apache Spark](<https://img.shields.io/badge/compute-AWS%20Glue%206.0%20(Spark%204.1)-E25A1C.svg>)](https://aws.amazon.com/glue/)
[![Amazon Redshift](https://img.shields.io/badge/warehouse-Redshift%20Serverless-8C4FFF.svg)](https://aws.amazon.com/redshift/)
[![Package Manager](https://img.shields.io/badge/packaging-uv-DE5FE9.svg)](https://github.com/astral-sh/uv)
[![Code Style: Ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

---

## Executive Summary

Proyek ini mengimplementasikan data pipeline berbasis cloud untuk memproses data transaksi NYC Yellow Taxi dari sumber publik NYC TLC. Pembersihan, validasi, dan transformasi data dilakukan menggunakan Apache Spark 4.1 di AWS Glue 6.0, kemudian dimodelkan ke dalam Kimball Star Schema pada Amazon Redshift Serverless.

Orkestrasi pipeline dikelola secara otomatis menggunakan Apache Airflow dengan mekanisme idempotent untuk mencegah duplikasi data saat backfill atau eksekusi ulang, serta dilengkapi pemeriksaan kualitas data sebelum dimuat ke data warehouse.

> **Ringkasan Teknis:**
>
> - **Volume Data:** Telah diuji memproses data multi-partisi bulanan (`2024-01`, `2024-02`, dan `2025-01`) dengan total lebih dari 8,8 juta baris data.
> - **Waktu Eksekusi Pipeline:** Rata-rata durasi end-to-end per partisi bulanan sekitar 4 menit 20 detik (mencakup ingestion, Glue cleaning, validasi, Glue gold modeling, hingga Redshift `COPY`).
> - **Kualitas Data:** Penerapan validasi nilai non-null pada atribut esensial dan pembersihan anomali nilai numerik seperti durasi atau tarif negatif.

---

## Arsitektur Sistem (Medallion Architecture)

<figure align="center">
  <img src="img/architecture.png" alt="Arsitektur Sistem">
  <figcaption>Gambar 1. Arsitektur ETL Taxi Trip Data Pipeline</figcaption>
</figure>

---

## Data Modeling (Kimball Star Schema)

Model data dirancang untuk mendukung kebutuhan query analitik dan visualisasi BI dengan latensi rendah.

<figure align="center">
  <img src="img/star_schema.png" alt="Star Schema">
  <figcaption>Gambar 2. Star Schema</figcaption>
</figure>

### Karakteristik Model Data:

- **Fact Table Grain:** Satu baris merepresentasikan satu transaksi perjalanan Yellow Taxi yang valid.
- **Role-Playing Dimension:** Tabel `dim_date` digunakan sebagai referensi ganda untuk waktu penjemputan (`pickup_date_key`) dan waktu penurunan (`dropoff_date_key`).
- **Unknown Record (`Key = 0`):** Setiap tabel dimensi menyertakan record bootstrap `Key = 0` (`Unknown`) untuk menangani transaksi dengan referensi tidak dikenal tanpa membatalkan proses pemuatan data.
- **Strategi SCD (Slowly Changing Dimensions):**
  - `dim_date`: **SCD Type 0** (Statis, rentang 2020–2030).
  - `dim_location`, `dim_vendor`, `dim_rate_code`, `dim_payment_type`: **SCD Type 1** (Overwrite).

---

## Tantangan Teknis & Keputusan Desain

Berikut adalah beberapa tantangan teknis dan keputusan desain yang diterapkan selama pengembangan pipeline:

### 1. Penanganan Surrogate Key pada Arsitektur Terdistribusi Redshift

- **Konteks:** Pembuatan kunci surrogate pada tabel dimensi `dim_location` menggunakan `IDENTITY(1, 1)` menghasilkan ketidaksesuaian data ketika di-join dengan tabel fakta.
- **Penyebab:** Pada arsitektur terdistribusi Redshift, generator `IDENTITY` mengalokasikan nomor urut per-slice komputasi dalam rentang terpisah, bukan urutan kontigu 1..265. Hal ini menyebabkan nilai `location_key` berbeda dari `location_id` referensi argo taksi.
- **Solusi:**
  - Untuk **Tabel Dimensi Lokasi**: Menggunakan deterministic natural integer key (`location_key = location_id`).
  - Untuk **Tabel Fakta (`trip_key`)**: Tetap menggunakan `BIGINT IDENTITY(1, 1)` karena hanya berfungsi sebagai identifier unik transaksi dan tidak dirujuk sebagai foreign key oleh tabel lain.

### 2. Penanganan Nilai Default pada Perintah COPY Redshift dari Format Parquet

- **Konteks:** Perintah `COPY` Redshift dari file Parquet menghasilkan error saat mengevaluasi kolom dengan ekspresi `DEFAULT GETDATE()` yang tidak ada dalam file sumber.
- **Penyebab:** Driver pembacaan Parquet di Redshift tidak mengevaluasi ekspresi SQL skalar dinamis untuk kolom yang absen pada file binary Parquet.
- **Solusi:** Kolom timestamp audit dibentuk secara langsung pada tahap pemrosesan di PySpark:
  ```python
  F.current_timestamp().alias("inserted_at_utc")
  ```
  Pendekatan ini mengatasi keterbatasan evaluasi default pada format Parquet sekaligus menjaga konsistensi lineage waktu pemrosesan Spark.

### 3. Penerapan Idempotency Melalui Partition Replacement

- **Konteks:** Perintah `COPY` pada Redshift secara default menambahkan data (_append_), sehingga eksekusi ulang pada periode yang sama berpotensi menduplikasi data transaksi.
- **Solusi:** Menggunakan mekanisme penggantian partisi berbasis cakupan tanggal:

  ```sql
  DELETE FROM nyc_taxi_gold.fact_yellow_taxi_trip
  WHERE source_year = {year} AND source_month = {month};

  COPY nyc_taxi_gold.fact_yellow_taxi_trip (...)
  FROM 's3://.../fact_yellow_taxi_trip/year=YYYY/month=MM/'
  IAM_ROLE default FORMAT AS PARQUET;
  ```

  Mekanisme ini memastikan bahwa eksekusi berulang pada partisi bulan yang sama menghasilkan data yang konsisten tanpa memengaruhi partisi lain.

### 4. Penanganan Periode Data yang Belum Rilis (AirflowSkipException)

- **Konteks:** Penjadwalan pipeline bulanan dapat memicu eksekusi untuk periode data yang belum dirilis oleh NYC TLC, menghasilkan respons HTTP 403 atau 404 dari CloudFront.
- **Solusi:** Menangani respons HTTP 403/404 dengan memicu `AirflowSkipException`. Task unduhan beserta downstream task (Glue dan Redshift) akan otomatis berstatus **Skipped**, sehingga pipeline tidak mengalami failed state yang tidak perlu dan menghindari pemakaian resource komputasi tambahan.

### 5. Penegakan Data Contract: Zero-NULL Policy di PySpark vs DDL Redshift

- **Konteks:** Menghindari kegagalan kalkulasi pada query agregasi analitik (seperti perhitungan pendapatan, tip rate, dan durasi) akibat adanya nilai NULL pada data mentah argo taksi.
- **Analisis & Keputusan:** Klausul `DEFAULT` pada DDL SQL Redshift hanya dievaluasi pada operasi `INSERT` individual, bukan saat pemuatan data massal via perintah `COPY` dari file Parquet.
- **Solusi:** Memindahkan seluruh logika validasi dan imputasi nilai default (`NULL_IMPUTATION_DEFAULTS`) ke tahap pemrosesan hulu di PySpark (Silver & Gold). Dengan pendekatan ini, skema DDL tabel fakta di Redshift cukup menegakkan kontrak data menggunakan constraint `NOT NULL` secara murni, memastikan seluruh data yang masuk ke data warehouse bersih dan konsisten untuk kebutuhan analitik.

### 6. Optimasi Performa Query dan Efisiensi Biaya Melalui Strategi SORTKEY dan DISTSTYLE

- **Konteks:** Menjaga performa query analitik tetap responsif dan mengendalikan konsumsi komputasi (RPU-hours) pada Amazon Redshift Serverless dengan volume data puluhan juta baris.
- **Analisis & Keputusan:** Pada arsitektur MPP terdistribusi, pemindaian tabel secara penuh (_full table scan_) dan perpindahan data antar-node (_network shuffling_) saat operasi `JOIN` akan memperlambat waktu respons query serta meningkatkan konsumsi RPU.
- **Solusi:**
  - Menerapkan **`SORTKEY`** pada kolom `pickup_date_key` di tabel fakta untuk memanfaatkan mekanisme _Zone Map Pruning_, sehingga Redshift hanya memindai blok disk 1 MB yang relevan dengan filter rentang waktu query.
  - Menerapkan **`DISTSTYLE ALL`** pada tabel-tabel dimensi untuk menduplikasi data rujukan ke setiap _compute slice_, memastikan operasi `JOIN` dengan tabel fakta berjalan secara lokal (_colocated join_) tanpa _network data redistribution_.

---

## Validasi Query & Hasil Analitik

<figure align="center">
  <img src="img/image.gif" alt="Dashboard BI (Streamlit)">
  <figcaption>Gambar 3. Dashboard BI Streamlit</figcaption>
</figure>

Contoh query agregasi analitik yang dijalankan pada Amazon Redshift Serverless:

```sql
SET search_path TO nyc_taxi_gold;

-- Rekapitulasi volume perjalanan dan pendapatan lintas bulan
SELECT
    d.year,
    d.month_name,
    COUNT(f.trip_key) AS total_trips,
    ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
    ROUND(AVG(f.trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(f.trip_duration_seconds / 60.0), 2) AS avg_duration_minutes
FROM fact_yellow_taxi_trip AS f
LEFT JOIN dim_date AS d ON f.pickup_date_key = d.date_key
GROUP BY d.year, d.month_name, f.source_year, f.source_month
ORDER BY f.source_year, f.source_month;
```

### Ringkasan Hasil Query:

- **Volume Januari 2024:** 2.963.711 perjalanan valid dengan total pendapatan **\$79,41 Juta USD** (rata-rata durasi: 15,6 menit, rata-rata jarak: 3,23 mil).
- **Zona Pendapatan Tertinggi:** **Queens — JFK Airport** (145.183 trip) menghasilkan pendapatan sebesar **\$11,11 Juta USD**, dipengaruhi oleh tarif flat bandara, durasi perjalanan, dan biaya tol.
- **Distribusi Tip:** Transaksi menggunakan _Credit Card_ mencatat rata-rata tip sebesar **\$4,16**, sedangkan transaksi _Cash_ tercatat **\$0,00** karena tip tunai tidak masuk dalam pencatatan argo taksi.

---

## Struktur Direktori Repository

```text
.
├── airflow-nyc-taxi/               # Konfigurasi Airflow lokal (Docker Compose)
│   ├── dags/
│   │   └── nyc_taxi_pipeline.py    # Definisi DAG otomatisasi end-to-end
│   ├── docker-compose.yaml
│   └── .env
├── jobs/                           # Script PySpark untuk AWS Glue
│   ├── silver_job.py               # Pembersihan, filtering, dan standarisasi
│   └── gold_job.py                 # Pembentukan dimensi dan tabel fakta
├── notebooks/                      # Exploratory Data Analysis (EDA)
│   └── 01_eda_bronze_to_silver.ipynb # Analisis data mentah
├── nyc_taxi_etl/                   # Package Python modular
│   ├── bronze/                     # Download data dan pembuatan manifest
│   ├── silver/                     # Standarisasi schema dan validasi data
│   ├── gold/                       # Transformasi data modeling Kimball
│   └── warehouse/                  # Interaksi dengan Redshift Data API
├── sql/
│   └── redshift/                   # DDL dan query validasi
│       ├── 001_create_schema.sql
│       ├── 002_create_dimensions.sql
│       ├── 003_create_fact.sql
│       ├── 004_load_gold_data.sql
│       └── validation_queries.sql
├── tests/                          # Automated test suite
│   ├── unit/
│   │   ├── test_config.py
│   │   ├── test_bronze.py
│   │   ├── test_silver.py
│   │   └── test_gold.py
├── docs/
│   └── plan.md                     # Rencana pengembangan dan catatan teknis
├── pyproject.toml                  # Manajemen dependensi via uv
├── uv.lock
└── README.md
```

---

## Panduan Menjalankan Proyek

### 1. Prasyarat Sistem

- Python 3.11+ dan [`uv`](https://docs.astral.sh/uv/)
- Docker & Docker Compose
- Kredensial AWS CLI terkonfigurasi (`~/.aws/credentials`)

### 2. Setup Environment & Pengujian Unit

```bash
# Clone repository
git clone https://github.com/lateeeppp/dataengineer-nyc-taxi-project.git
cd dataengineer-nyc-taxi-project

# Install dependensi dan jalankan linter
uv sync
uv run ruff check .

# Jalankan pengujian unit
uv run pytest -v
```

### 3. Deploy Artifact ke AWS S3

```bash
# Package modul Python untuk dependensi AWS Glue
zip -r nyc_taxi_etl.zip nyc_taxi_etl -x "*/__pycache__/*"
aws s3 cp nyc_taxi_etl.zip s3://[YOUR_BUCKET_NAME]/packages/nyc_taxi_etl.zip

# Upload script AWS Glue
aws s3 cp jobs/silver_job.py s3://[YOUR_BUCKET_NAME]/scripts/silver_job.py
aws s3 cp jobs/gold_job.py s3://[YOUR_BUCKET_NAME]/scripts/gold_job.py
```

### 4. Menjalankan Airflow Orchestrator

```bash
cd airflow-nyc-taxi

# Inisialisasi dan jalankan container
docker compose run --rm airflow-init
docker compose up -d
```

Akses web UI Airflow melalui `http://localhost:8080`, pastikan koneksi `aws_default` telah terkonfigurasi, lalu jalankan DAG `nyc_taxi_end_to_end_pipeline`.

---

## Kontak Penulis

- **Nama:** Ikhsannudin Lathief
- **LinkedIn:** [www.linkedin.com/in/ikhsannudin-lathief/](https://www.linkedin.com/in/ikhsannudin-lathief/)
