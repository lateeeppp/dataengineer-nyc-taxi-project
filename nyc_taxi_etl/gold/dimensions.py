"""
nyc_taxi_etl.gold.dimensions
Logika pembentukan dataset Dimensi Kimball Star Schema.
"""

from datetime import date, timedelta

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame, SparkSession


def generate_dim_date(
    spark: SparkSession,
    start_date: str = "2020-01-01",
    end_date: str = "2030-12-31",
) -> DataFrame:
    """
    Menghasilkan data kalender independen (SCD Type 0) untuk rentang tanggal tertentu.
    Cocok dengan skema nyc_taxi_gold.dim_date di Redshift.
    """
    # 1. Generate urutan tanggal secara deterministik di Python
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    delta = (end - start).days + 1
    date_list = [(start + timedelta(days=i),) for i in range(delta)]

    # 2. Buat Spark DataFrame dari list tanggal
    schema = T.StructType([T.StructField("full_date", T.DateType(), False)])
    df = spark.createDataFrame(date_list, schema)

    # 3. Ekstraksi atribut kalender analitik
    # ISO 8601 day_of_week: 1=Senin, ..., 7=Minggu
    day_of_week_expr = ((F.dayofweek("full_date") + 5) % 7) + 1

    return (
        df.withColumn(
            "date_key",
            F.date_format("full_date", "yyyyMMdd").cast(T.IntegerType()),
        )
        .withColumn("day_of_week", day_of_week_expr.cast(T.IntegerType()))
        .withColumn(
            "day_name",
            F.date_format("full_date", "EEEE").cast(T.StringType()),
        )
        .withColumn(
            "day_of_month", F.dayofmonth("full_date").cast(T.IntegerType())
        )
        .withColumn("month", F.month("full_date").cast(T.IntegerType()))
        .withColumn(
            "month_name",
            F.date_format("full_date", "MMMM").cast(T.StringType()),
        )
        .withColumn("quarter", F.quarter("full_date").cast(T.IntegerType()))
        .withColumn("year", F.year("full_date").cast(T.IntegerType()))
        .withColumn(
            "is_weekend",
            F.when(day_of_week_expr.isin(6, 7), True).otherwise(False),
        )
        .select(
            "date_key",
            "full_date",
            "day_of_week",
            "day_name",
            "day_of_month",
            "month",
            "month_name",
            "quarter",
            "year",
            "is_weekend",
        )
    )


def transform_dim_location(silver_lookup_df: DataFrame) -> DataFrame:
    """
    Menstandarkan dataset master zona lokasi untuk nyc_taxi_gold.dim_location.
    Menetapkan location_key bernilai persis sama dengan location_id (1 s.d. 265).
    """
    return (
        silver_lookup_df.select(
            F.col("location_id").cast(T.IntegerType()).alias("location_key"),
            F.col("location_id").cast(T.IntegerType()),
            F.col("borough").cast(T.StringType()),
            F.col("zone").cast(T.StringType()),
            F.col("service_zone").cast(T.StringType()),
        )
        .dropDuplicates(["location_id"])
    )