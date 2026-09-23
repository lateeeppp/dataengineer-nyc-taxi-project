-- Query Analitik untuk Validasi Deliverable Star Schema

set search_path to nyc_taxi_gold;

-- 1. Total Volume Perjalanan dan Total Revenue Bulan Januari 2024
select
    d.year,
    d.month_name,
    count(f.trip_key) as total_trips,
    sum(f.total_amount) as total_revenue_usd,
    avg(f.trip_distance) as avg_distance_miles,
    avg(f.trip_duration_seconds / 60.0) as avg_duration_minutes
from fact_yellow_taxi_trip as f
left join
    dim_date as d on
    f.pickup_date_key = d.date_key
group by
    d.year, d.month_name;

-- 2. Top 5 Zona Pickup Terpopuler di New York City Berdasarkan Jumlah Perjalanan
select
    loc.borough,
    loc.zone,
    count(f.trip_key) as pickup_count,
    sum(f.total_amount) as total_revenue_usd
from
    fact_yellow_taxi_trip as f
left join
    dim_location as loc on
    f.pickup_location_key = loc.location_key
group by
    loc.borough, loc.zone
order by
    pickup_count desc
limit 5;

-- 3. Rata-rata Tip Berdasarkan Tipe Pembayaran
select
    p.payment_type_name,
    count(f.trip_key) as total_transactions,
    avg(f.tip_amount) as avg_tip_usd,
    avg(f.total_amount) as avg_total_usd
from
    fact_yellow_taxi_trip as f
left join
    dim_payment_type as p on
    f.payment_type_key = p.payment_type_key
group by
    p.payment_type_name
order by
    total_transactions desc;