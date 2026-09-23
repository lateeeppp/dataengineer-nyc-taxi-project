"""
dashboard.queries
Eksekusi query analitik langsung ke Amazon Redshift Serverless via AWS Redshift Data API.
Mendukung agregasi tingkat lanjut termasuk Window Functions untuk Month-over-Month (MoM) Growth.
"""

import time
from typing import Any

import boto3
import pandas as pd


def get_redshift_client(
    profile_name: str = "nyc-taxi", region_name: str = "ap-southeast-3"
) -> Any:
    session = boto3.Session(profile_name=profile_name, region_name=region_name)
    return session.client("redshift-data")


def run_redshift_query(
    sql_text: str,
    workgroup_name: str = "nyc-taxi-workgroup",
    database: str = "dev",
) -> pd.DataFrame:
    """Mengeksekusi SQL di Redshift Serverless dan mengonversi format ke Pandas DataFrame."""
    client = get_redshift_client()

    response = client.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        Sql=sql_text,
    )
    statement_id = response["Id"]

    while True:
        status_resp = client.describe_statement(Id=statement_id)
        status = status_resp["Status"]
        if status in ["FINISHED", "FAILED", "ABORTED"]:
            break
        time.sleep(1)

    if status == "FAILED":
        raise RuntimeError(
            f"Query Execution Failed: {status_resp.get('Error', 'Unknown Error')}"
        )

    results_resp = client.get_statement_result(Id=statement_id)
    columns = [col["name"] for col in results_resp["ColumnMetadata"]]

    rows = []
    for record in results_resp["Records"]:
        row = []
        for val in record:
            if "stringValue" in val:
                row.append(val["stringValue"])
            elif "longValue" in val:
                row.append(val["longValue"])
            elif "doubleValue" in val:
                row.append(val["doubleValue"])
            elif "booleanValue" in val:
                row.append(val["booleanValue"])
            elif "isNull" in val and val["isNull"]:
                row.append(None)
            else:
                row.append(list(val.values())[0] if val else None)
        rows.append(row)

    return pd.DataFrame(rows, columns=columns)


def fetch_mom_metrics() -> pd.DataFrame:
    """Menghitung performa bulanan dan Month-over-Month (MoM) Growth menggunakan SQL Window Function LAG()."""
    sql = """
    WITH monthly_rollup AS (
        SELECT 
            d.year,
            d.month,
            d.month_name,
            COUNT(f.trip_key) AS total_trips,
            ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
            ROUND(AVG(f.trip_distance), 2) AS avg_distance_miles,
            ROUND(AVG(f.trip_duration_seconds / 60.0), 2) AS avg_duration_minutes,
            ROUND(AVG(f.tip_amount), 2) AS avg_tip_usd
        FROM nyc_taxi_gold.fact_yellow_taxi_trip AS f
        LEFT JOIN nyc_taxi_gold.dim_date AS d ON f.pickup_date_key = d.date_key
        GROUP BY d.year, d.month, d.month_name
    )
    SELECT 
        year,
        month,
        month_name,
        total_trips,
        total_revenue_usd,
        avg_distance_miles,
        avg_duration_minutes,
        avg_tip_usd,
        LAG(total_trips, 1) OVER (ORDER BY year, month) AS prev_month_trips,
        LAG(total_revenue_usd, 1) OVER (ORDER BY year, month) AS prev_month_revenue,
        ROUND(
            ((total_trips - LAG(total_trips, 1) OVER (ORDER BY year, month))::FLOAT / 
            NULLIF(LAG(total_trips, 1) OVER (ORDER BY year, month), 0)) * 100.0, 
            2
        ) AS mom_trip_growth_pct,
        ROUND(
            ((total_revenue_usd - LAG(total_revenue_usd, 1) OVER (ORDER BY year, month))::FLOAT / 
            NULLIF(LAG(total_revenue_usd, 1) OVER (ORDER BY year, month), 0)) * 100.0, 
            2
        ) AS mom_revenue_growth_pct
    FROM monthly_rollup
    ORDER BY year, month;
    """
    df = run_redshift_query(sql)
    num_cols = [
        "year",
        "month",
        "total_trips",
        "total_revenue_usd",
        "avg_distance_miles",
        "avg_duration_minutes",
        "avg_tip_usd",
        "mom_trip_growth_pct",
        "mom_revenue_growth_pct",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["period_label"] = (
        df["month_name"].astype(str) + " " + df["year"].astype(str)
    )
    return df


def fetch_daily_metrics() -> pd.DataFrame:
    """Mengambil metrik harian untuk mendukung visualisasi kurva granular per periode/bulan."""
    sql = """
    SELECT 
        d.year,
        d.month,
        d.day_of_month,
        d.full_date,
        d.month_name,
        COUNT(f.trip_key) AS total_trips,
        ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
        ROUND(AVG(f.trip_duration_seconds / 60.0), 2) AS avg_duration_minutes,
        ROUND(AVG(f.trip_distance), 2) AS avg_distance_miles
    FROM nyc_taxi_gold.fact_yellow_taxi_trip AS f
    JOIN nyc_taxi_gold.dim_date AS d ON f.pickup_date_key = d.date_key
    GROUP BY d.year, d.month, d.day_of_month, d.full_date, d.month_name
    ORDER BY d.full_date ASC;
    """
    df = run_redshift_query(sql)
    num_cols = [
        "year",
        "month",
        "day_of_month",
        "total_trips",
        "total_revenue_usd",
        "avg_duration_minutes",
        "avg_distance_miles",
    ]
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    df["period_label"] = (
        df["month_name"].astype(str) + " " + df["year"].astype(str)
    )
    df["day_label"] = (
        df["month_name"].astype(str).str[:3]
        + " "
        + df["day_of_month"].astype(str)
    )
    return df


def fetch_top_zones() -> pd.DataFrame:
    """Mengambil data zona penjemputan dengan atribut periode untuk filtering dinamis."""
    sql = """
    SELECT 
        d.year,
        d.month,
        d.month_name,
        loc.borough,
        loc.zone,
        COUNT(f.trip_key) AS total_trips,
        ROUND(SUM(f.total_amount), 2) AS total_revenue_usd,
        ROUND(AVG(f.fare_amount), 2) AS avg_fare_usd
    FROM nyc_taxi_gold.fact_yellow_taxi_trip AS f
    JOIN nyc_taxi_gold.dim_date AS d ON f.pickup_date_key = d.date_key
    LEFT JOIN nyc_taxi_gold.dim_location AS loc ON f.pickup_location_key = loc.location_key
    GROUP BY d.year, d.month, d.month_name, loc.borough, loc.zone;
    """
    df = run_redshift_query(sql)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["total_trips"] = pd.to_numeric(df["total_trips"], errors="coerce")
    df["total_revenue_usd"] = pd.to_numeric(
        df["total_revenue_usd"], errors="coerce"
    )
    df["avg_fare_usd"] = pd.to_numeric(df["avg_fare_usd"], errors="coerce")
    df["period_label"] = (
        df["month_name"].astype(str) + " " + df["year"].astype(str)
    )
    return df


def fetch_payment_breakdown() -> pd.DataFrame:
    """Mengambil komposisi transaksi dan analitik tip per periode untuk filtering dinamis."""
    sql = """
    SELECT 
        d.year,
        d.month,
        d.month_name,
        p.payment_type_name,
        COUNT(f.trip_key) AS transaction_count,
        ROUND(SUM(f.total_amount), 2) AS total_volume_usd,
        ROUND(AVG(f.tip_amount), 2) AS avg_tip_usd,
        ROUND(
            (SUM(f.tip_amount)::FLOAT / NULLIF(SUM(f.fare_amount), 0)) * 100.0, 
            2
        ) AS effective_tip_rate_pct
    FROM nyc_taxi_gold.fact_yellow_taxi_trip AS f
    JOIN nyc_taxi_gold.dim_date AS d ON f.pickup_date_key = d.date_key
    LEFT JOIN nyc_taxi_gold.dim_payment_type AS p ON f.payment_type_key = p.payment_type_key
    GROUP BY d.year, d.month, d.month_name, p.payment_type_name;
    """
    df = run_redshift_query(sql)
    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["transaction_count"] = pd.to_numeric(
        df["transaction_count"], errors="coerce"
    )
    df["total_volume_usd"] = pd.to_numeric(
        df["total_volume_usd"], errors="coerce"
    )
    df["avg_tip_usd"] = pd.to_numeric(df["avg_tip_usd"], errors="coerce")
    df["effective_tip_rate_pct"] = pd.to_numeric(
        df["effective_tip_rate_pct"], errors="coerce"
    )
    df["period_label"] = (
        df["month_name"].astype(str) + " " + df["year"].astype(str)
    )
    return df

