#!/usr/bin/env python3
"""
spark_silver_to_gold_user_retention.py

Performs cohort analysis for user retention from Silver events.
Tracks user activity patterns over time.

Usage:
    PROCESSING_DATE=2026-04-17 spark-submit \
        --master spark://127.0.0.1:7077 \
        scripts/spark_silver_to_gold_user_retention.py
"""

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
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


def read_silver_events(spark: SparkSession, date: str) -> DataFrame:
    path = f"{SILVER_ROOT}/domain=events/dt={date}"
    logger.info(f"Reading events from: {path}")
    df = spark.read.parquet(str(path))
    logger.info(f"Loaded {df.count()} event records")
    return df


def calculate_user_retention(events_df: DataFrame) -> DataFrame:
    """Calculate user retention metrics and cohort analysis."""
    logger.info("Calculating user retention metrics...")

    # Convert timestamp to date
    events_with_date = events_df.withColumn(
        "activity_date",
        F.to_date("timestamp")
    )

    # Find first activity date for each user (cohort)
    user_cohorts = events_with_date.groupBy("user_id").agg(
        F.min("activity_date").alias("cohort_date")
    )

    # Join back to get cohort for each event
    events_with_cohort = events_with_date.join(user_cohorts, "user_id")

    # Calculate days since cohort
    events_with_cohort = events_with_cohort.withColumn(
        "days_since_cohort",
        F.datediff("activity_date", "cohort_date")
    )

    # Aggregate retention by cohort
    cohort_retention = events_with_cohort.groupBy(
        "cohort_date",
        "days_since_cohort"
    ).agg(
        F.countDistinct("user_id").alias("active_users"),
        F.count("event_id").alias("total_events")
    )

    # Calculate cohort size (Day 0)
    cohort_size = cohort_retention.filter(F.col("days_since_cohort") == 0) \
        .select(
            F.col("cohort_date"),
            F.col("active_users").alias("cohort_size")
        )

    # Join to calculate retention rate
    result = cohort_retention.join(cohort_size, "cohort_date") \
        .withColumn(
            "retention_rate_pct",
            (F.col("active_users") / F.col("cohort_size") * 100)
        ) \
        .select(
            "cohort_date",
            "days_since_cohort",
            "cohort_size",
            "active_users",
            "retention_rate_pct",
            "total_events"
        ) \
        .orderBy("cohort_date", "days_since_cohort")

    logger.info(f"Calculated retention for {result.count()} cohort-day combinations")
    return result


def add_gold_metadata(df: DataFrame, processing_date: str) -> DataFrame:
    return df.withColumn("_aggregated_at", F.current_timestamp()) \
             .withColumn("dt", F.to_date(F.lit(processing_date)))


def write_gold(df: DataFrame, output_path: str) -> None:
    logger.info(f"Writing to Gold: {output_path}")
    logger.info(f"Record count: {df.count()}")
    df.write.mode("overwrite").partitionBy("dt").parquet(output_path)
    logger.info("✅ Gold user_retention written successfully")


def run_silver_to_gold_user_retention(processing_date: str) -> None:
    logger.info("=" * 70)
    logger.info("Starting Silver → Gold ETL for kpi=user_retention")
    logger.info(f"Processing date: {processing_date}")
    logger.info("=" * 70)

    spark = (
        SparkSession.builder
        .appName(f"Silver2Gold_UserRetention_{processing_date}")
        .master("spark://127.0.0.1:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        logger.info("\n--- Step 1: Reading Silver events ---")
        events_df = read_silver_events(spark, processing_date)

        logger.info("\n--- Step 2: Calculating user retention ---")
        retention_df = calculate_user_retention(events_df)

        logger.info("\n--- Step 3: Adding Gold metadata ---")
        gold_df = add_gold_metadata(retention_df, processing_date)

        logger.info("\n--- Step 4: Writing to Gold ---")
        output_path = str(GOLD_ROOT / "kpi=user_retention")
        write_gold(gold_df, output_path)

        logger.info("\n" + "=" * 70)
        logger.info("✅ Silver → Gold ETL completed successfully!")
        logger.info("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_to_gold_user_retention(PROCESSING_DATE)
