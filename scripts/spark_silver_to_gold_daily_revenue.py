#!/usr/bin/env python3
"""
spark_silver_to_gold_daily_revenue.py

Aggregates daily revenue from Silver transactions and events.
Joins transaction data with event data to get user activity context.

Usage:
    PROCESSING_DATE=2026-04-17 spark-submit \
        --master spark://127.0.0.1:7077 \
        scripts/spark_silver_to_gold_daily_revenue.py
"""

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SILVER_ROOT = PROJECT_ROOT / "datalake" / "silver"
GOLD_ROOT = PROJECT_ROOT / "datalake" / "gold"

PROCESSING_DATE = os.getenv("PROCESSING_DATE")
if not PROCESSING_DATE:
    logger.error("PROCESSING_DATE required!")
    sys.exit(1)


def read_silver_transactions(spark: SparkSession, date: str) -> DataFrame:
    path = f"{SILVER_ROOT}/domain=transactions/dt={date}"
    logger.info(f"Reading transactions from: {path}")
    df = spark.read.parquet(str(path))
    logger.info(f"Loaded {df.count()} transaction records")
    return df


def read_silver_events(spark: SparkSession, date: str) -> DataFrame:
    path = f"{SILVER_ROOT}/domain=events/dt={date}"
    logger.info(f"Reading events from: {path}")
    df = spark.read.parquet(str(path))
    logger.info(f"Loaded {df.count()} event records")
    return df


def calculate_daily_revenue(transactions_df: DataFrame, events_df: DataFrame) -> DataFrame:
    """Calculate daily revenue metrics with user activity context."""
    logger.info("Calculating daily revenue metrics...")

    # Aggregate transactions by date
    daily_txn = transactions_df.groupBy(
        F.to_date("timestamp").alias("revenue_date")
    ).agg(
        F.sum("amount").alias("total_revenue"),
        F.count("transaction_id").alias("total_transactions"),
        F.avg("amount").alias("avg_transaction_value"),
        F.countDistinct(F.coalesce("customer_id", "user_id")).alias("unique_customers"),
        F.sum("transaction_fee").alias("total_fees")
    )

    # Aggregate events by date (for context)
    daily_events = events_df.groupBy(
        F.to_date("timestamp").alias("event_date")
    ).agg(
        F.count("event_id").alias("total_events"),
        F.countDistinct("user_id").alias("unique_users")
    )

    # Join and calculate conversion rate
    result = daily_txn.join(
        daily_events,
        daily_txn.revenue_date == daily_events.event_date,
        "left"
    ).select(
        "revenue_date",
        "total_revenue",
        "total_transactions",
        "avg_transaction_value",
        "unique_customers",
        "total_fees",
        "total_events",
        "unique_users",
        (F.col("total_transactions") / F.col("total_events") * 100).alias("conversion_rate_pct")
    )

    logger.info(f"Calculated revenue for {result.count()} days")
    return result


def add_gold_metadata(df: DataFrame, processing_date: str) -> DataFrame:
    return df.withColumn("_aggregated_at", F.current_timestamp()) \
             .withColumn("dt", F.to_date(F.lit(processing_date)))


def write_gold(df: DataFrame, output_path: str) -> None:
    logger.info(f"Writing to Gold: {output_path}")
    logger.info(f"Record count: {df.count()}")
    df.write.mode("overwrite").partitionBy("dt").parquet(output_path)
    logger.info("✅ Gold daily_revenue written successfully")


def run_silver_to_gold_daily_revenue(processing_date: str) -> None:
    logger.info("=" * 70)
    logger.info("Starting Silver → Gold ETL for kpi=daily_revenue")
    logger.info(f"Processing date: {processing_date}")
    logger.info("=" * 70)

    spark = (
        SparkSession.builder
        .appName(f"Silver2Gold_DailyRevenue_{processing_date}")
        .master("spark://127.0.0.1:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        logger.info("\n--- Step 1: Reading Silver data ---")
        transactions_df = read_silver_transactions(spark, processing_date)
        events_df = read_silver_events(spark, processing_date)

        logger.info("\n--- Step 2: Calculating daily revenue ---")
        revenue_df = calculate_daily_revenue(transactions_df, events_df)

        logger.info("\n--- Step 3: Adding Gold metadata ---")
        gold_df = add_gold_metadata(revenue_df, processing_date)

        logger.info("\n--- Step 4: Writing to Gold ---")
        output_path = str(GOLD_ROOT / "kpi=daily_revenue")
        write_gold(gold_df, output_path)

        logger.info("\n" + "=" * 70)
        logger.info("✅ Silver → Gold ETL completed successfully!")
        logger.info("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_to_gold_daily_revenue(PROCESSING_DATE)
