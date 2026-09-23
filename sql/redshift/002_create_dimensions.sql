-- DDL untuk Tabel Dimensi Kimball Star Schema

set search_path to nyc_taxi_gold;

-- 1. Dimensi Tanggal
create table if not exists dim_date (
    date_key integer not null sortkey, -- Format YYYYMMDD (contoh: 20240115)
    full_date date not null,
    day_of_week integer not null, -- 1=Senin, 2=Selasa, ..., 7=Minggu
    day_name varchar(10) not null,
    day_of_month integer not null,
    month integer not null,
    month_name varchar(10) not null,
    quarter integer not null,
    year integer not null,
    is_weekend boolean not null,
    primary key (date_key)
) diststyle all;

-- 2. Dimensi Lokasi Zona NYC (dim_location)
create table if not exists dim_location (
    location_key integer identity(1,1) not null,
    location_id integer not null, -- Natural key dari data NYC Taxi
    borough varchar(50) not null,
    zone varchar(100) not null,
    service_zone varchar(50) not null,
    primary key (location_key)
) diststyle all;

-- 3 Dimensi Tarif (dim_rate_code)
create table if not exists dim_rate_code (
    rate_code_key integer not null,
    rate_code_id integer not null,
    rate_code_name varchar(50) not null,
    primary key (rate_code_key)
) diststyle all;

-- 4. Dimensi Tipe Pembayaran (dim_payment_type)
create table if not exists dim_payment_type (
    payment_type_key integer not null,
    payment_type_id integer not null,
    payment_type_name varchar(50) not null,
    primary key (payment_type_key)
) diststyle all;

-- 5. Dimensi Vendor Armada (dim_vendor)
create table if not exists dim_vendor (
    vendor_key integer not null,
    vendor_id integer not null,
    vendor_name varchar(100) not null,
    primary key (vendor_key)
) diststyle all;

-- =============================================================
-- SEED DATA MASTER & UNKNOWN ROW (Key 0)
-- =============================================================

insert into dim_rate_code (rate_code_key, rate_code_id, rate_code_name)
values
    (0, 99, 'Unknown / Unassigned'),
    (1, 1, 'Standar rate'),
    (2, 2, 'JFK'),
    (3, 3, 'Newark'),
    (4, 4, 'Nassau or Westchester'),
    (5, 5, 'Negotiated fare'),
    (6, 6, 'Group ride');

insert into dim_payment_type (payment_type_key, payment_type_id, payment_type_name)
values
    (0, 0, 'Unknown / Flex Fare'),
    (1, 1, 'Credit card'),
    (2, 2, 'Cash'),
    (3, 3, 'No charge'),
    (4, 4, 'Dispute'),
    (5, 5, 'Unknown'),
    (6, 6, 'Voided trip');

insert into dim_vendor (vendor_key, vendor_id, vendor_name)
values
    (0, 0, 'Unknown Vendor'),
    (1, 1, 'Creative Mobile Technologies, LLC'),
    (2, 2, 'VeriFone Inc.');