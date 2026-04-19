#!/usr/bin/env python3
"""
spark_bronze_to_silver_inventory.py

Transforms Bronze NDJSON → Silver Parquet for domain=inventory
Merges: source=inventory (base) + source=orders (sales aggregated)

Usage:
    PROCESSING_DATE=2026-04-17 spark-submit \
        --master spark://127.0.0.1:7077 \
        scripts/spark_bronze_to_silver_inventory.py
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
INVENTORY_SCHEMA = StructType([
    StructField("sku", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("warehouse", StringType(), True),
    StructField("quantity_on_hand", IntegerType(), True),
    StructField("reorder_point", IntegerType(), True),
    StructField("unit_cost", DoubleType(), True),
    StructField("last_restock_date", StringType(), True),
    StructField("snapshot_date", StringType(), True),
    StructField("_source", StringType(), False),
    StructField("_ingested_at", TimestampType(), False),
])

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("order_date", StringType(), True),
    StructField("_source", StringType(), False),
    StructField("_ingested_at", TimestampType(), False),
])


def read_bronze_inventory(spark: SparkSession, date: str) -> DataFrame:
    path = f"{BRONZE_ROOT}/source=inventory/dt={date}/hour=*/part-*.ndjson"
    logger.info(f"Reading inventory from: {path}")
    df = spark.read.json(str(path), schema=INVENTORY_SCHEMA)
    logger.info(f"Loaded {df.count()} inventory records")
    return df


def read_bronze_orders(spark: SparkSession, date: str) -> DataFrame:
    path = f"{BRONZE_ROOT}/source=orders/dt={date}/hour=*/part-*.ndjson"
    logger.info(f"Reading orders from: {path}")
    df = spark.read.json(str(path), schema=ORDERS_SCHEMA)
    logger.info(f"Loaded {df.count()} orders records")
    return df


def aggregate_orders_by_product(orders_df: DataFrame) -> DataFrame:
    """Aggregate orders per product."""
    logger.info("Aggregating orders by product...")
    agg_df = orders_df.groupBy("product_id").agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.count("order_id").alias("total_orders"),
        F.sum("total_amount").alias("total_sales_value")
    )
    logger.info(f"Aggregated to {agg_df.count()} products")
    return agg_df


def merge_inventory_with_orders(inventory_df: DataFrame, orders_agg_df: DataFrame) -> DataFrame:
    """Merge inventory with order aggregates."""
    logger.info("Merging inventory with order aggregates...")
    # Join on sku = product_id
    merged = inventory_df.join(
        orders_agg_df,
        inventory_df.sku == orders_agg_df.product_id,
        "left"
    ).drop("product_id")

    # Fill nulls for products with no sales
    merged = merged.fillna({
        "total_quantity_sold": 0,
        "total_orders": 0,
        "total_sales_value": 0.0
    })

    # Calculate stock status
    merged = merged.withColumn(
        "stock_status",
        F.when(F.col("quantity_on_hand") <= F.col("reorder_point"), "low_stock")
        .when(F.col("quantity_on_hand") <= 0, "out_of_stock")
        .otherwise("in_stock")
    )

    return merged


def dedup_inventory(df: DataFrame) -> DataFrame:
    logger.info("Deduplicating by sku...")
    window = Window.partitionBy("sku").orderBy(F.col("_ingested_at").desc())
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
    logger.info("✅ Silver inventory written successfully")


def run_bronze_to_silver_inventory(processing_date: str) -> None:
    logger.info("=" * 70)
    logger.info("Starting Bronze → Silver ETL for domain=inventory")
    logger.info(f"Processing date: {processing_date}")
    logger.info("=" * 70)

    spark = (
        SparkSession.builder
        .appName(f"Bronze2Silver_Inventory_{processing_date}")
        .master("spark://127.0.0.1:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        logger.info("\n--- Step 1: Reading Bronze data ---")
        inventory_df = read_bronze_inventory(spark, processing_date)
        orders_df = read_bronze_orders(spark, processing_date)

        logger.info("\n--- Step 2: Aggregating orders by product ---")
        orders_agg = aggregate_orders_by_product(orders_df)

        logger.info("\n--- Step 3: Merging inventory with orders ---")
        merged = merge_inventory_with_orders(inventory_df, orders_agg)

        logger.info("\n--- Step 4: Deduplication ---")
        deduped = dedup_inventory(merged)

        logger.info("\n--- Step 5: Data quality ---")
        before = deduped.count()
        cleaned = deduped.dropna(subset=["sku"])
        after = cleaned.count()
        logger.info(f"Dropped {before - after} records with null sku")

        logger.info("\n--- Step 6: Adding Silver metadata ---")
        silver_df = add_silver_metadata(cleaned, processing_date)

        logger.info("\n--- Step 7: Writing to Silver ---")
        output_path = str(SILVER_ROOT / "domain=inventory")
        write_silver(silver_df, output_path)

        logger.info("\n" + "=" * 70)
        logger.info("✅ Bronze → Silver ETL completed successfully!")
        logger.info("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_bronze_to_silver_inventory(PROCESSING_DATE)
