-- sql/redshift/004_load_gold_data.sql
-- Menggunakan Fully Qualified Table Names (nyc_taxi_gold.<table_name>)

-- 1. Load Dimensi Kalender (dim_date)
copy nyc_taxi_gold.dim_date (
    date_key, full_date, day_of_week, day_name, 
    day_of_month, month, month_name, quarter, year, is_weekend
)
from 's3://nyc-taxi-bucket-lathief/gold/nyc_taxi/dim_date/'
iam_role default
format as parquet;

-- 2. Load Dimensi Lokasi (dim_location)
copy nyc_taxi_gold.dim_location (
    location_id, borough, zone, service_zone
)
from 's3://nyc-taxi-bucket-lathief/gold/nyc_taxi/dim_location/'
iam_role default
format as parquet;

-- 3. Load Tabel Fakta Transaksi (fact_yellow_taxi_trip)
copy nyc_taxi_gold.fact_yellow_taxi_trip (
    pickup_date_key, dropoff_date_key, pickup_location_key, dropoff_location_key,
    rate_code_key, payment_type_key, vendor_key, store_and_fwd_flag,
    pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
    trip_duration_seconds, fare_amount, extra, mta_tax, tip_amount,
    tolls_amount, improvement_surcharge, congestion_surcharge, airport_fee,
    total_amount, source_year, source_month, pipeline_run_id, inserted_at_utc
)
from 's3://nyc-taxi-bucket-lathief/gold/nyc_taxi/fact_yellow_taxi_trip/year=2024/month=01/'
iam_role default
format as parquet;