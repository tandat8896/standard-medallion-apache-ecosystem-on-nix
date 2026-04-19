#!/usr/bin/env python3
"""
spark_gold_to_postgres_iot_anomaly.py

Loads Gold iot_anomaly_summary KPI from Parquet into PostgreSQL.

Usage:
    PROCESSING_DATE=2026-04-17 spark-submit \
        --master spark://127.0.0.1:7077 \
        scripts/spark_gold_to_postgres_iot_anomaly.py
"""

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GOLD_ROOT = PROJECT_ROOT / "datalake" / "gold"

PROCESSING_DATE = os.getenv("PROCESSING_DATE")
if not PROCESSING_DATE:
    logger.error("PROCESSING_DATE required!")
    sys.exit(1)

POSTGRES_URL = "jdbc:postgresql://localhost:5432/etl_analytics"
POSTGRES_PROPERTIES = {
    "driver": "org.postgresql.Driver",
    "user": os.getenv("PGUSER", os.getenv("USER")),
}


def read_gold_iot_anomaly(spark: SparkSession, date: str) -> DataFrame:
    """Read iot_anomaly_summary from Gold Parquet layer."""
    path = f"{GOLD_ROOT}/kpi=iot_anomaly_summary/dt={date}"
    logger.info(f"Reading Gold iot_anomaly_summary from: {path}")

    df = spark.read.parquet(str(path))
    count = df.count()
    logger.info(f"Loaded {count} anomaly records")

    if count == 0:
        logger.warning("No data found for this date! (This is expected if no IoT sensor events)")

    return df


def transform_for_postgres(df: DataFrame) -> DataFrame:
    """Transform Spark DataFrame to match PostgreSQL schema."""
    logger.info("Transforming data for PostgreSQL...")

    df_transformed = df.withColumnRenamed("_aggregated_at", "aggregated_at")

    df_final = df_transformed.select(
        "sensor_type",
        "anomaly_date",
        "total_readings",
        "anomaly_count",
        "avg_sensor_value",
        "min_sensor_value",
        "max_sensor_value",
        "avg_z_score",
        "max_abs_z_score",
        "anomaly_rate_pct",
        "aggregated_at",
        "dt"
    )

    logger.info("Schema transformed successfully")
    df_final.printSchema()

    return df_final


def write_to_postgres(df: DataFrame, table_name: str) -> None:
    """Write DataFrame to PostgreSQL using JDBC."""
    logger.info(f"Writing to PostgreSQL table: {table_name}")
    logger.info(f"Record count: {df.count()}")

    if df.count() == 0:
        logger.warning("No records to write - skipping JDBC write")
        return

    df.write \
        .mode("append") \
        .jdbc(
            url=POSTGRES_URL,
            table=table_name,
            properties=POSTGRES_PROPERTIES
        )

    logger.info(f"✅ Data written to {table_name} successfully")


def run_gold_to_postgres_iot_anomaly(processing_date: str) -> None:
    logger.info("=" * 70)
    logger.info("Starting Gold → PostgreSQL ETL for kpi_iot_anomaly_summary")
    logger.info(f"Processing date: {processing_date}")
    logger.info("=" * 70)

    spark = (
        SparkSession.builder
        .appName(f"Gold2Postgres_IoTAnomaly_{processing_date}")
        .master("spark://127.0.0.1:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    try:
        logger.info("\n--- Step 1: Reading Gold Parquet ---")
        gold_df = read_gold_iot_anomaly(spark, processing_date)

        logger.info("\n--- Step 2: Transforming for PostgreSQL ---")
        pg_df = transform_for_postgres(gold_df)

        logger.info("\n--- Step 3: Writing to PostgreSQL ---")
        write_to_postgres(pg_df, "kpi_iot_anomaly_summary")

        logger.info("\n" + "=" * 70)
        logger.info("✅ Gold → PostgreSQL ETL completed successfully!")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"❌ ETL failed: {e}", exc_info=True)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    run_gold_to_postgres_iot_anomaly(PROCESSING_DATE)
