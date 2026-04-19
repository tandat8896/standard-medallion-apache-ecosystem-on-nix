#!/usr/bin/env python3
"""
spark_bronze_to_silver_transactions.py

Transforms Bronze NDJSON → Silver Parquet for domain=transactions
Merges: source=transactions (streaming) + source=payments (batch)

Usage:
    PROCESSING_DATE=2026-04-17 spark-submit \
        --master spark://127.0.0.1:7077 \
        --conf spark.sql.shuffle.partitions=8 \
        --conf spark.sql.parquet.compression.codec=snappy \
        scripts/spark_bronze_to_silver_transactions.py
"""

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# Configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BRONZE_ROOT = PROJECT_ROOT / "datalake" / "bronze"
SILVER_ROOT = PROJECT_ROOT / "datalake" / "silver"

PROCESSING_DATE = os.getenv("PROCESSING_DATE")
if not PROCESSING_DATE:
    logger.error("PROCESSING_DATE environment variable is required!")
    sys.exit(1)

# Schemas
TRANSACTIONS_SCHEMA = StructType([
    StructField("txn_id", StringType(), False),
    StructField("user_id", StringType(), True),
    StructField("txn_type", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("payment_method", StringType(), True),
    StructField("status", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("timestamp", TimestampType(), False),
    StructField("_source", StringType(), False),
    StructField("_kafka_topic", StringType(), True),
    StructField("_kafka_partition", IntegerType(), True),
    StructField("_kafka_offset", LongType(), True),
    StructField("_ingested_at", TimestampType(), False),
])

PAYMENTS_SCHEMA = StructType([
    StructField("payment_id", StringType(), False),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("gateway", StringType(), True),
    StructField("transaction_fee", DoubleType(), True),
    StructField("payment_date", StringType(), True),
    StructField("_source", StringType(), False),
    StructField("_file_path", StringType(), True),
    StructField("_ingested_at", TimestampType(), False),
])

SILVER_TRANSACTIONS_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),  # PK
    StructField("transaction_source", StringType(), False),  # 'transactions' or 'payments'
    StructField("timestamp", TimestampType(), False),

    # Transactions fields
    StructField("user_id", StringType(), True),
    StructField("txn_type", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("status", StringType(), True),

    # Payments fields
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("gateway", StringType(), True),
    StructField("transaction_fee", DoubleType(), True),

    # Common fields
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), False),
    StructField("payment_method", StringType(), True),

    # Silver metadata
    StructField("_ingested_at", TimestampType(), False),
    StructField("_processed_at", TimestampType(), False),
    StructField("dt", DateType(), False),
])


def read_bronze_transactions(spark: SparkSession, date: str) -> DataFrame:
    """Read transactions from Bronze."""
    path = f"{BRONZE_ROOT}/source=transactions/dt={date}/hour=*/part-*.ndjson"
    logger.info(f"Reading transactions from: {path}")
    df = spark.read.json(str(path), schema=TRANSACTIONS_SCHEMA)
    logger.info(f"Loaded {df.count()} transactions records")
    return df


def read_bronze_payments(spark: SparkSession, date: str) -> DataFrame:
    """Read payments from Bronze."""
    path = f"{BRONZE_ROOT}/source=payments/dt={date}/hour=*/part-*.ndjson"
    logger.info(f"Reading payments from: {path}")
    df = spark.read.json(str(path), schema=PAYMENTS_SCHEMA)
    logger.info(f"Loaded {df.count()} payments records")
    return df


def transform_transactions(df: DataFrame) -> DataFrame:
    """Transform transactions to unified schema."""
    return df.select(
        F.col("txn_id").alias("transaction_id"),
        F.lit("transactions").alias("transaction_source"),
        F.col("timestamp"),
        F.col("user_id"),
        F.col("txn_type"),
        F.col("merchant_id"),
        F.col("status"),
        # Payments fields (null)
        F.lit(None).cast(StringType()).alias("order_id"),
        F.lit(None).cast(StringType()).alias("customer_id"),
        F.lit(None).cast(StringType()).alias("payment_status"),
        F.lit(None).cast(StringType()).alias("gateway"),
        F.lit(None).cast(DoubleType()).alias("transaction_fee"),
        # Common
        F.col("amount"),
        F.col("currency"),
        F.col("payment_method"),
        F.col("_ingested_at"),
    )


def transform_payments(df: DataFrame) -> DataFrame:
    """Transform payments to unified schema."""
    return df.select(
        F.col("payment_id").alias("transaction_id"),
        F.lit("payments").alias("transaction_source"),
        F.to_timestamp(F.col("payment_date")).alias("timestamp"),
        # Transactions fields (null)
        F.lit(None).cast(StringType()).alias("user_id"),
        F.lit(None).cast(StringType()).alias("txn_type"),
        F.lit(None).cast(StringType()).alias("merchant_id"),
        F.lit(None).cast(StringType()).alias("status"),
        # Payments fields
        F.col("order_id"),
        F.col("customer_id"),
        F.col("payment_status"),
        F.col("gateway"),
        F.col("transaction_fee"),
        # Common
        F.col("amount"),
        F.col("currency"),
        F.col("payment_method"),
        F.col("_ingested_at"),
    )


def dedup_transactions(df: DataFrame) -> DataFrame:
    """Deduplicate by transaction_id."""
    logger.info("Deduplicating by transaction_id...")
    window = Window.partitionBy("transaction_id").orderBy(F.col("_ingested_at").desc())
    deduped = df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

    original = df.count()
    final = deduped.count()
    logger.info(f"Deduplication: {original} → {final} records ({original - final} duplicates removed)")
    return deduped


def add_silver_metadata(df: DataFrame, processing_date: str) -> DataFrame:
    """Add Silver metadata."""
    return df.withColumn("_processed_at", F.current_timestamp()).withColumn("dt", F.to_date(F.lit(processing_date)))


def write_silver(df: DataFrame, output_path: str) -> None:
    """Write to Silver layer."""
    logger.info(f"Writing to Silver: {output_path}")
    logger.info(f"Record count: {df.count()}")
    df.write.mode("overwrite").partitionBy("dt").parquet(output_path)
    logger.info("✅ Silver transactions written successfully")


def run_bronze_to_silver_transactions(processing_date: str) -> None:
    """Main ETL pipeline."""
    logger.info("=" * 70)
    logger.info("Starting Bronze → Silver ETL for domain=transactions")
    logger.info(f"Processing date: {processing_date}")
    logger.info("=" * 70)

    spark = (
        SparkSession.builder
        .appName(f"Bronze2Silver_Transactions_{processing_date}")
        .master("spark://127.0.0.1:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        logger.info("\n--- Step 1: Reading Bronze data ---")
        txn_df = read_bronze_transactions(spark, processing_date)
        pay_df = read_bronze_payments(spark, processing_date)

        logger.info("\n--- Step 2: Transforming to unified schema ---")
        txn_transformed = transform_transactions(txn_df)
        pay_transformed = transform_payments(pay_df)

        logger.info("\n--- Step 3: Merging transactions + payments ---")
        merged = txn_transformed.union(pay_transformed)
        logger.info(f"Merged count: {merged.count()}")

        logger.info("\n--- Step 4: Deduplication ---")
        deduped = dedup_transactions(merged)

        logger.info("\n--- Step 5: Data quality ---")
        before = deduped.count()
        cleaned = deduped.dropna(subset=["transaction_id", "timestamp", "amount"])
        after = cleaned.count()
        logger.info(f"Dropped {before - after} records with null mandatory fields")

        logger.info("\n--- Step 6: Adding Silver metadata ---")
        silver_df = add_silver_metadata(cleaned, processing_date)

        logger.info("\n--- Step 7: Writing to Silver ---")
        output_path = str(SILVER_ROOT / "domain=transactions")
        write_silver(silver_df, output_path)

        logger.info("\n" + "=" * 70)
        logger.info("✅ Bronze → Silver ETL completed successfully!")
        logger.info("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver_transactions(PROCESSING_DATE)
