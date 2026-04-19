#!/usr/bin/env python3
"""
spark_bronze_to_silver_customers.py

Transforms Bronze NDJSON → Silver Parquet for domain=customers
Merges: source=crm (base) + source=orders (aggregated)

Usage:
    PROCESSING_DATE=2026-04-17 spark-submit \
        --master spark://127.0.0.1:7077 \
        scripts/spark_bronze_to_silver_customers.py
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
BRONZE_ROOT = PROJECT_ROOT / "datalake" / "bronze"
SILVER_ROOT = PROJECT_ROOT / "datalake" / "silver"

PROCESSING_DATE = os.getenv("PROCESSING_DATE")
if not PROCESSING_DATE:
    logger.error("PROCESSING_DATE required!")
    sys.exit(1)

# Schemas
CRM_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("lifetime_value", DoubleType(), True),
    StructField("registered_date", StringType(), True),
    StructField("last_purchase_date", StringType(), True),
    StructField("_source", StringType(), False),
    StructField("_ingested_at", TimestampType(), False),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("discount_pct", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("_source", StringType(), False),
    StructField("_ingested_at", TimestampType(), False),
])


def read_bronze_crm(spark: SparkSession, date: str) -> DataFrame:
    path = f"{BRONZE_ROOT}/source=crm/dt={date}/hour=*/part-*.ndjson"
    logger.info(f"Reading crm from: {path}")
    df = spark.read.json(str(path), schema=CRM_SCHEMA)
    logger.info(f"Loaded {df.count()} crm records")
    return df


def read_bronze_orders(spark: SparkSession, date: str) -> DataFrame:
    path = f"{BRONZE_ROOT}/source=orders/dt={date}/hour=*/part-*.ndjson"
    logger.info(f"Reading orders from: {path}")
    df = spark.read.json(str(path), schema=ORDERS_SCHEMA)
    logger.info(f"Loaded {df.count()} orders records")
    return df


def aggregate_orders_by_customer(orders_df: DataFrame) -> DataFrame:
    """Aggregate orders per customer."""
    logger.info("Aggregating orders by customer...")
    agg_df = orders_df.groupBy("customer_id").agg(
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_order_value"),
        F.max("order_date").alias("latest_order_date"),
        F.avg("total_amount").alias("avg_order_value")
    )
    logger.info(f"Aggregated to {agg_df.count()} customers")
    return agg_df


def merge_crm_with_orders(crm_df: DataFrame, orders_agg_df: DataFrame) -> DataFrame:
    """Merge CRM with aggregated orders."""
    logger.info("Merging CRM with order aggregates...")
    merged = crm_df.join(orders_agg_df, "customer_id", "left")

    # Fill nulls for customers with no orders
    merged = merged.fillna({
        "total_orders": 0,
        "total_order_value": 0.0,
        "avg_order_value": 0.0
    })

    return merged


def dedup_customers(df: DataFrame) -> DataFrame:
    logger.info("Deduplicating by customer_id...")
    window = Window.partitionBy("customer_id").orderBy(F.col("_ingested_at").desc())
    deduped = df.withColumn("row_num", F.row_number().over(window)).filter(F.col("row_num") == 1).drop("row_num")

    original = df.count()
    final = deduped.count()
    logger.info(f"Deduplication: {original} → {final} ({original - final} removed)")
    return deduped


def add_silver_metadata(df: DataFrame, processing_date: str) -> DataFrame:
    return df.withColumn("_processed_at", F.current_timestamp()).withColumn("dt", F.to_date(F.lit(processing_date)))


def write_silver(df: DataFrame, output_path: str) -> None:
    logger.info(f"Writing to Silver: {output_path}")
    logger.info(f"Record count: {df.count()}")
    df.write.mode("overwrite").partitionBy("dt").parquet(output_path)
    logger.info("✅ Silver customers written successfully")


def run_bronze_to_silver_customers(processing_date: str) -> None:
    logger.info("=" * 70)
    logger.info("Starting Bronze → Silver ETL for domain=customers")
    logger.info(f"Processing date: {processing_date}")
    logger.info("=" * 70)

    spark = (
        SparkSession.builder
        .appName(f"Bronze2Silver_Customers_{processing_date}")
        .master("spark://127.0.0.1:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        logger.info("\n--- Step 1: Reading Bronze data ---")
        crm_df = read_bronze_crm(spark, processing_date)
        orders_df = read_bronze_orders(spark, processing_date)

        logger.info("\n--- Step 2: Aggregating orders by customer ---")
        orders_agg = aggregate_orders_by_customer(orders_df)

        logger.info("\n--- Step 3: Merging CRM with orders ---")
        merged = merge_crm_with_orders(crm_df, orders_agg)

        logger.info("\n--- Step 4: Deduplication ---")
        deduped = dedup_customers(merged)

        logger.info("\n--- Step 5: Data quality ---")
        before = deduped.count()
        cleaned = deduped.dropna(subset=["customer_id"])
        after = cleaned.count()
        logger.info(f"Dropped {before - after} records with null customer_id")

        logger.info("\n--- Step 6: Adding Silver metadata ---")
        silver_df = add_silver_metadata(cleaned, processing_date)

        logger.info("\n--- Step 7: Writing to Silver ---")
        output_path = str(SILVER_ROOT / "domain=customers")
        write_silver(silver_df, output_path)

        logger.info("\n" + "=" * 70)
        logger.info("✅ Bronze → Silver ETL completed successfully!")
        logger.info("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver_customers(PROCESSING_DATE)
