"""
nyc_taxi_etl.warehouse.redshift_loader
Eksekusi query dan perintah COPY di Amazon Redshift Serverless via AWS Redshift Data API.
"""

import logging
import time

import boto3

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("redshift_loader")


def execute_redshift_sql(
    sql_text: str,
    workgroup_name: str = "nyc-taxi-workgroup",
    database: str = "dev",
    region_name: str = "ap-southeast-3",
    profile_name: str = "nyc-taxi",
) -> dict:
    """Mengeksekusi SQL multi-statement di Redshift Serverless secara asinkron lalu menunggu hasilnya."""
    session = boto3.Session(
        profile_name=profile_name, region_name=region_name
    )
    client = session.client("redshift-data")

    logger.info(
        f"Mengirim SQL ke Redshift Serverless workgroup: {workgroup_name}, db: {database}"
    )
    response = client.execute_statement(
        WorkgroupName=workgroup_name,
        Database=database,
        Sql=sql_text,
    )
    statement_id = response["Id"]
    logger.info(f"Query dieksekusi dengan Statement ID: {statement_id}")

    # Polling status eksekusi hingga FINISHED / FAILED
    while True:
        status_resp = client.describe_statement(Id=statement_id)
        status = status_resp["Status"]
        if status in ["FINISHED", "FAILED", "ABORTED"]:
            break
        logger.info(f"Menunggu query selesai... Status saat ini: {status}")
        time.sleep(3)

    if status == "FAILED":
        error_msg = status_resp.get("Error", "Unknown error")
        logger.error(f"Eksekusi Redshift GAGAL: {error_msg}")
        raise RuntimeError(f"Redshift Query Failed: {error_msg}")

    logger.info("Eksekusi Redshift BERHASIL!")
    return status_resp


if __name__ == "__main__":
    with open("sql/redshift/004_load_gold_data.sql") as f:
        sql_content = f.read()

    execute_redshift_sql(sql_content)