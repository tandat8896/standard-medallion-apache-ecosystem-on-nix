#!/usr/bin/env python3
"""
spark_bronze_to_silver_events.py

Transforms Bronze NDJSON → Silver Parquet for domain=events
Merges: source=user_events + source=clickstream

Usage:
    PROCESSING_DATE=2026-04-19 spark-submit \
        --master spark://127.0.0.1:7077 \
        --conf spark.sql.shuffle.partitions=8 \
        --conf spark.sql.parquet.compression.codec=snappy \
        scripts/spark_bronze_to_silver_events.py
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
    DateType,
)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_ROOT = PROJECT_ROOT / "datalake" / "bronze"
SILVER_ROOT = PROJECT_ROOT / "datalake" / "silver"

# Get processing date from environment (required)
import os
PROCESSING_DATE = os.getenv("PROCESSING_DATE")
if not PROCESSING_DATE:
    logger.error("PROCESSING_DATE environment variable is required!")
    logger.error("Example: PROCESSING_DATE=2026-04-19 spark-submit ...")
    sys.exit(1)

# -----------------------------------------------------------------------------
# Schemas
# -----------------------------------------------------------------------------

# Schema for user_events (from Bronze)
USER_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), False),
    StructField("page_url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("device", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("country", StringType(), True),
    StructField("timestamp", TimestampType(), False),
    # Bronze metadata fields
    StructField("_source", StringType(), False),
    StructField("_kafka_topic", StringType(), False),
    StructField("_kafka_partition", IntegerType(), True),
    StructField("_kafka_offset", LongType(), True),
    StructField("_ingested_at", TimestampType(), False),
    StructField("_flink_job", StringType(), True),
])

# Schema for clickstream (from Bronze)
CLICKSTREAM_SCHEMA = StructType([
    StructField("request_id", StringType(), False),
    StructField("method", StringType(), False),
    StructField("path", StringType(), False),
    StructField("status_code", IntegerType(), False),
    StructField("latency_ms", IntegerType(), True),
    StructField("bytes_sent", LongType(), True),
    StructField("user_agent", StringType(), True),
    StructField("ip_address", StringType(), True),
    StructField("timestamp", TimestampType(), False),
    # Bronze metadata fields
    StructField("_source", StringType(), False),
    StructField("_kafka_topic", StringType(), False),
    StructField("_kafka_partition", IntegerType(), True),
    StructField("_kafka_offset", LongType(), True),
    StructField("_ingested_at", TimestampType(), False),
    StructField("_flink_job", StringType(), True),
])

# Unified Silver schema for events domain
SILVER_EVENTS_SCHEMA = StructType([
    StructField("event_id", StringType(), False),  # PK
    StructField("event_source", StringType(), False),  # 'user_events' or 'clickstream'
    StructField("timestamp", TimestampType(), False),

    # User events fields
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("device", StringType(), True),
    StructField("browser", StringType(), True),
    StructField("country", StringType(), True),

    # Clickstream fields
    StructField("request_id", StringType(), True),
    StructField("method", StringType(), True),
    StructField("path", StringType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("latency_ms", IntegerType(), True),
    StructField("bytes_sent", LongType(), True),
    StructField("user_agent", StringType(), True),
    StructField("ip_address", StringType(), True),

    # Silver metadata
    StructField("_ingested_at", TimestampType(), False),
    StructField("_processed_at", TimestampType(), False),
    StructField("dt", DateType(), False),  # Partition column
])

# -----------------------------------------------------------------------------
# Transform Functions
# -----------------------------------------------------------------------------

def read_bronze_user_events(spark: SparkSession, date: str) -> DataFrame:
    """Read user_events from Bronze for a specific date."""
    # Bronze path pattern: source=user_events/dt=YYYY-MM-DD/hour=*/part-*.ndjson
    path_pattern = f"{BRONZE_ROOT}/source=user_events/dt={date}/hour=*/part-*.ndjson"

    logger.info(f"Reading user_events from: {path_pattern}")

    df = spark.read.json(str(path_pattern), schema=USER_EVENTS_SCHEMA)

    logger.info(f"Loaded {df.count()} user_events records")
    return df


def read_bronze_clickstream(spark: SparkSession, date: str) -> DataFrame:
    """Read clickstream from Bronze for a specific date."""
    path_pattern = f"{BRONZE_ROOT}/source=clickstream/dt={date}/hour=*/part-*.ndjson"

    logger.info(f"Reading clickstream from: {path_pattern}")

    df = spark.read.json(str(path_pattern), schema=CLICKSTREAM_SCHEMA)

    logger.info(f"Loaded {df.count()} clickstream records")
    return df


def transform_user_events(df: DataFrame) -> DataFrame:
    """Transform user_events to unified events schema."""
    return df.select(
        F.col("event_id").alias("event_id"),
        F.lit("user_events").alias("event_source"),
        F.col("timestamp"),
        F.col("user_id"),
        F.col("session_id"),
        F.col("event_type"),
        F.col("page_url"),
        F.col("referrer"),
        F.col("device"),
        F.col("browser"),
        F.col("country"),
        # Clickstream fields (null for user_events)
        F.lit(None).cast(StringType()).alias("request_id"),
        F.lit(None).cast(StringType()).alias("method"),
        F.lit(None).cast(StringType()).alias("path"),
        F.lit(None).cast(IntegerType()).alias("status_code"),
        F.lit(None).cast(IntegerType()).alias("latency_ms"),
        F.lit(None).cast(LongType()).alias("bytes_sent"),
        F.lit(None).cast(StringType()).alias("user_agent"),
        F.lit(None).cast(StringType()).alias("ip_address"),
        # Metadata
        F.col("_ingested_at"),
    )


def transform_clickstream(df: DataFrame) -> DataFrame:
    """Transform clickstream to unified events schema."""
    return df.select(
        F.col("request_id").alias("event_id"),  # Use request_id as event_id
        F.lit("clickstream").alias("event_source"),
        F.col("timestamp"),
        # User events fields (null for clickstream)
        F.lit(None).cast(StringType()).alias("user_id"),
        F.lit(None).cast(StringType()).alias("session_id"),
        F.lit(None).cast(StringType()).alias("event_type"),
        F.col("path").alias("page_url"),  # Map path → page_url
        F.lit(None).cast(StringType()).alias("referrer"),
        F.lit(None).cast(StringType()).alias("device"),
        F.lit(None).cast(StringType()).alias("browser"),
        F.lit(None).cast(StringType()).alias("country"),
        # Clickstream fields
        F.col("request_id"),
        F.col("method"),
        F.col("path"),
        F.col("status_code"),
        F.col("latency_ms"),
        F.col("bytes_sent"),
        F.col("user_agent"),
        F.col("ip_address"),
        # Metadata
        F.col("_ingested_at"),
    )


def dedup_events(df: DataFrame) -> DataFrame:
    """
    Deduplicate events by event_id.
    Keep the record with latest _ingested_at timestamp.
    """
    logger.info("Deduplicating by event_id (keeping latest _ingested_at)...")

    window = Window.partitionBy("event_id").orderBy(F.col("_ingested_at").desc())

    deduped = (
        df.withColumn("row_num", F.row_number().over(window))
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    original_count = df.count()
    deduped_count = deduped.count()
    duplicates = original_count - deduped_count

    logger.info(f"Deduplication: {original_count} → {deduped_count} records ({duplicates} duplicates removed)")

    return deduped


def add_silver_metadata(df: DataFrame, processing_date: str) -> DataFrame:
    """Add Silver layer metadata fields."""
    return df.withColumn(
        "_processed_at", F.current_timestamp()
    ).withColumn(
        "dt", F.to_date(F.lit(processing_date))
    )


def write_silver_events(df: DataFrame, output_path: str) -> None:
    """Write events to Silver layer as Parquet."""
    logger.info(f"Writing to Silver: {output_path}")
    logger.info(f"Record count: {df.count()}")

    (
        df.write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(output_path)
    )

    logger.info("✅ Silver events written successfully")


# -----------------------------------------------------------------------------
# Main Pipeline
# -----------------------------------------------------------------------------

def run_bronze_to_silver_events(processing_date: str) -> None:
    """Main ETL pipeline: Bronze → Silver for events domain."""
    logger.info("=" * 70)
    logger.info("Starting Bronze → Silver ETL for domain=events")
    logger.info(f"Processing date: {processing_date}")
    logger.info("=" * 70)

    # 1. Create Spark session
    spark = (
        SparkSession.builder
        .appName(f"Bronze2Silver_Events_{processing_date}")
        .master("spark://127.0.0.1:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        # 2. Read Bronze data
        logger.info("\n--- Step 1: Reading Bronze data ---")
        user_events_df = read_bronze_user_events(spark, processing_date)
        clickstream_df = read_bronze_clickstream(spark, processing_date)

        # 3. Transform to unified schema
        logger.info("\n--- Step 2: Transforming to unified schema ---")
        user_events_transformed = transform_user_events(user_events_df)
        clickstream_transformed = transform_clickstream(clickstream_df)

        # 4. Union both sources
        logger.info("\n--- Step 3: Merging user_events + clickstream ---")
        merged_df = user_events_transformed.union(clickstream_transformed)
        logger.info(f"Merged record count: {merged_df.count()}")

        # 5. Deduplication
        logger.info("\n--- Step 4: Deduplication ---")
        deduped_df = dedup_events(merged_df)

        # 6. Drop nulls on mandatory fields
        logger.info("\n--- Step 5: Data quality - drop nulls on mandatory fields ---")
        before_count = deduped_df.count()
        cleaned_df = deduped_df.dropna(subset=["event_id", "timestamp", "event_source"])
        after_count = cleaned_df.count()
        dropped = before_count - after_count
        logger.info(f"Dropped {dropped} records with null mandatory fields")

        # 7. Add Silver metadata
        logger.info("\n--- Step 6: Adding Silver metadata ---")
        silver_df = add_silver_metadata(cleaned_df, processing_date)

        # 8. Show sample
        logger.info("\n--- Sample Silver records ---")
        silver_df.show(5, truncate=False)

        # 9. Write to Silver
        logger.info("\n--- Step 7: Writing to Silver ---")
        output_path = str(SILVER_ROOT / "domain=events")
        write_silver_events(silver_df, output_path)

        logger.info("\n" + "=" * 70)
        logger.info("✅ Bronze → Silver ETL completed successfully!")
        logger.info("=" * 70)

    finally:
        spark.stop()


# -----------------------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    run_bronze_to_silver_events(PROCESSING_DATE)
