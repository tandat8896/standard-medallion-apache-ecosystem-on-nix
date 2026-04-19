#!/usr/bin/env python3
"""
spark_silver_to_gold_inventory_turnover.py

Calculates inventory turnover metrics from Silver inventory and transactions.
Analyzes stock movement, reorder needs, and product performance.

Usage:
    PROCESSING_DATE=2026-04-17 spark-submit \
        --master spark://127.0.0.1:7077 \
        scripts/spark_silver_to_gold_inventory_turnover.py
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


def read_silver_inventory(spark: SparkSession, date: str) -> DataFrame:
    path = f"{SILVER_ROOT}/domain=inventory/dt={date}"
    logger.info(f"Reading inventory from: {path}")
    df = spark.read.parquet(str(path))
    logger.info(f"Loaded {df.count()} inventory records")
    return df


def read_silver_transactions(spark: SparkSession, date: str) -> DataFrame:
    path = f"{SILVER_ROOT}/domain=transactions/dt={date}"
    logger.info(f"Reading transactions from: {path}")
    df = spark.read.parquet(str(path))
    logger.info(f"Loaded {df.count()} transaction records")
    return df


def calculate_inventory_turnover(inventory_df: DataFrame, transactions_df: DataFrame) -> DataFrame:
    """Calculate inventory turnover and stock performance metrics."""
    logger.info("Calculating inventory turnover...")

    # Note: Inventory Silver already has sales data from Bronze orders merge
    # Fields available: total_quantity_sold, total_orders, total_sales_value from Silver transform

    # Calculate turnover metrics using existing sales data
    turnover_metrics = inventory_df.withColumn(
        "turnover_ratio",
        F.when(
            F.col("quantity_on_hand") > 0,
            F.col("total_quantity_sold") / F.col("quantity_on_hand")
        ).otherwise(0)
    ).withColumn(
        "stock_coverage_days",
        F.when(
            F.col("total_quantity_sold") > 0,
            F.col("quantity_on_hand") / (F.col("total_quantity_sold") / 30)
        ).otherwise(999)
    ).withColumn(
        "needs_reorder",
        F.col("quantity_on_hand") <= F.col("reorder_point")
    ).withColumn(
        "stock_value",
        F.col("quantity_on_hand") * F.col("unit_cost")
    )

    # Aggregate by category
    category_summary = turnover_metrics.groupBy("category").agg(
        F.count("sku").alias("product_count"),
        F.sum("quantity_on_hand").alias("total_stock_units"),
        F.sum("total_quantity_sold").alias("total_units_sold"),
        F.sum("total_sales_value").alias("total_revenue"),
        F.avg("turnover_ratio").alias("avg_turnover_ratio"),
        F.sum("stock_value").alias("total_stock_value"),
        F.sum(F.when(F.col("needs_reorder"), 1).otherwise(0)).alias("products_need_reorder"),
        F.sum(F.when(F.col("stock_status") == "out_of_stock", 1).otherwise(0)).alias("products_out_of_stock")
    ).withColumn(
        "reorder_rate_pct",
        (F.col("products_need_reorder") / F.col("product_count") * 100)
    ).withColumn(
        "stock_out_rate_pct",
        (F.col("products_out_of_stock") / F.col("product_count") * 100)
    )

    logger.info(f"Calculated turnover for {category_summary.count()} categories")
    return category_summary


def add_gold_metadata(df: DataFrame, processing_date: str) -> DataFrame:
    return df.withColumn("_aggregated_at", F.current_timestamp()) \
             .withColumn("dt", F.to_date(F.lit(processing_date)))


def write_gold(df: DataFrame, output_path: str) -> None:
    logger.info(f"Writing to Gold: {output_path}")
    logger.info(f"Record count: {df.count()}")
    df.write.mode("overwrite").partitionBy("dt").parquet(output_path)
    logger.info("✅ Gold inventory_turnover written successfully")


def run_silver_to_gold_inventory_turnover(processing_date: str) -> None:
    logger.info("=" * 70)
    logger.info("Starting Silver → Gold ETL for kpi=inventory_turnover")
    logger.info(f"Processing date: {processing_date}")
    logger.info("=" * 70)

    spark = (
        SparkSession.builder
        .appName(f"Silver2Gold_InventoryTurnover_{processing_date}")
        .master("spark://127.0.0.1:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        logger.info("\n--- Step 1: Reading Silver data ---")
        inventory_df = read_silver_inventory(spark, processing_date)
        transactions_df = read_silver_transactions(spark, processing_date)

        logger.info("\n--- Step 2: Calculating inventory turnover ---")
        turnover_df = calculate_inventory_turnover(inventory_df, transactions_df)

        logger.info("\n--- Step 3: Adding Gold metadata ---")
        gold_df = add_gold_metadata(turnover_df, processing_date)

        logger.info("\n--- Step 4: Writing to Gold ---")
        output_path = str(GOLD_ROOT / "kpi=inventory_turnover")
        write_gold(gold_df, output_path)

        logger.info("\n" + "=" * 70)
        logger.info("✅ Silver → Gold ETL completed successfully!")
        logger.info("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_to_gold_inventory_turnover(PROCESSING_DATE)
