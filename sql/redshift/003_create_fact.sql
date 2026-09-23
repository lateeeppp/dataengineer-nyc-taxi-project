-- DDL untuk Tabel Fakta Transaksi NYC Yellow Taxi
-- Skema: Star Schema Kimball (Zero-NULL Policy Guaranteed)

set search_path to nyc_taxi_gold;

-- Tabel Fakta Utama
create table if not exists fact_yellow_taxi_trip (
    trip_key bigint identity(1,1) not null,

    -- Foreign Key ke Tabel Dimensi (NOT NULL & Non-enforced references for CBO)
    pickup_date_key integer not null sortkey references dim_date(date_key),
    dropoff_date_key integer not null references dim_date(date_key),
    pickup_location_key integer not null references dim_location(location_key),
    dropoff_location_key integer not null references dim_location(location_key),
    rate_code_key integer not null references dim_rate_code(rate_code_key),
    payment_type_key integer not null references dim_payment_type(payment_type_key),
    vendor_key integer not null references dim_vendor(vendor_key),

    -- Degenerate Dimensions & Timestamps Asli
    store_and_fwd_flag varchar(1) not null,
    pickup_datetime timestamp not null,
    dropoff_datetime timestamp not null,

    -- Additive Measures (Metrik Ukuran Bisnis - Guaranteed Clean by Silver)
    passenger_count integer not null,
    trip_distance double precision not null,
    trip_duration_seconds bigint not null,
    fare_amount decimal(10,2) not null,
    extra decimal(10,2) not null,
    mta_tax decimal(10,2) not null,
    tip_amount decimal(10,2) not null,
    tolls_amount decimal(10,2) not null,
    improvement_surcharge decimal(10,2) not null,
    congestion_surcharge double precision not null,
    airport_fee double precision not null,
    total_amount decimal(10,2) not null,

    -- Audit Lineage Metadata (Data Governance)
    source_year smallint not null,
    source_month smallint not null,
    pipeline_run_id varchar(100) not null,
    inserted_at_utc timestamp not null default getdate(),
    
    primary key (trip_key)
) diststyle auto;