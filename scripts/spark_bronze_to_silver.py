#!/usr/bin/env python3
"""
spark_bronze_to_silver.py - Spark job to transform Bronze to Silver layer.

Reads Bronze partition for PROCESSING_DATE, performs:
- Schema enforcement
- Deduplication by primary key (keep latest _ingested_at)
- Drop nulls on mandatory fields
- Add _processed_at timestamp

Writes Parquet to datalake/silver/domain=<name>/dt=YYYY-MM-DD/

Usage:
    spark-submit --master spark://127.0.0.1:7077 \\
        --conf spark.sql.parquet.compression.codec=snappy \\
        --conf spark.sql.shuffle.partitions=8 \\
        scripts/spark_bronze_to_silver.py

    # Or with custom date:
    PROCESSING_DATE=2026-04-17 spark-submit ... scripts/spark_bronze_to_silver.py

Domains:
    - events       <- user_events + clickstream
    - transactions <- transactions + payments
    - customers    <- crm + orders (customer dimension)
    - inventory    <- inventory + orders (stock level)
"""

import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    lit,
    row_number,
    to_date,
    to_timestamp,
    when,
    coalesce,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
    DateType,
)
from pyspark.sql.window import Window

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger(__name__)

# Processing date - default to yesterday
PROCESSING_DATE = os.environ.get(
    "PROCESSING_DATE",
    (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"),
)

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATALAKE_BRONZE = PROJECT_ROOT / "datalake" / "bronze"
DATALAKE_SILVER = PROJECT_ROOT / "datalake" / "silver"

# Spark config
SPARK_MASTER = "spark://127.0.0.1:7077"

# Domain mappings - which bronze sources go into which silver domain
DOMAIN_SOURCES: dict[str, list[str]] = {
    "events": ["user_events", "clickstream"],
    "transactions": ["transactions", "payments"],
    "customers": ["crm", "orders"],
    "inventory": ["inventory", "orders"],
}

# Primary keys for deduplication
PRIMARY_KEYS: dict[str, str] = {
    "user_events": "event_id",
    "clickstream": "request_id",
    "transactions": "txn_id",
    "payments": "payment_id",
    "crm": "customer_id",
    "orders": "order_id",
    "inventory": "sku",
    "iot_sensors": "reading_id",
    "app_logs": "log_id",
}

# Mandatory fields per source (dropna)
MANDATORY_FIELDS: dict[str, list[str]] = {
    "user_events": ["event_id", "user_id", "timestamp"],
    "clickstream": ["request_id", "path", "timestamp"],
    "transactions": ["txn_id", "amount", "timestamp"],
    "payments": ["payment_id", "amount", "payment_date"],
    "crm": ["customer_id", "email"],
    "orders": ["order_id", "customer_id", "order_date"],
    "inventory": ["sku", "quantity_on_hand"],
    "iot_sensors": ["reading_id", "sensor_id", "value"],
    "app_logs": ["log_id", "level", "message"],
}


# -----------------------------------------------------------------------------
# Schema Definitions
# -----------------------------------------------------------------------------


def get_events_schema() -> StructType:
    """Schema for events domain (user_events + clickstream)."""
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("path", StringType(), True),
        StructField("method", StringType(), True),
        StructField("status_code", IntegerType(), True),
        StructField("latency_ms", IntegerType(), True),
        StructField("device", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("ip_address", StringType(), True),
        StructField("country", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_processed_at", TimestampType(), True),
        StructField("dt", DateType(), True),
    ])


def get_transactions_schema() -> StructType:
    """Schema for transactions domain (transactions + payments)."""
    return StructType([
        StructField("txn_id", StringType(), False),
        StructField("payment_id", StringType(), True),
        StructField("order_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("txn_type", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("status", StringType(), True),
        StructField("gateway", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("transaction_fee", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("payment_date", DateType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_processed_at", TimestampType(), True),
        StructField("dt", DateType(), True),
    ])


def get_customers_schema() -> StructType:
    """Schema for customers domain (crm + orders)."""
    return StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("lifetime_value", DoubleType(), True),
        StructField("registered_date", DateType(), True),
        StructField("last_purchase_date", DateType(), True),
        StructField("total_orders", IntegerType(), True),
        StructField("total_spent", DoubleType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_processed_at", TimestampType(), True),
        StructField("dt", DateType(), True),
    ])


def get_inventory_schema() -> StructType:
    """Schema for inventory domain."""
    return StructType([
        StructField("sku", StringType(), False),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("warehouse", StringType(), True),
        StructField("quantity_on_hand", IntegerType(), True),
        StructField("reorder_point", IntegerType(), True),
        StructField("unit_cost", DoubleType(), True),
        StructField("last_restock_date", DateType(), True),
        StructField("orders_count", IntegerType(), True),
        StructField("units_sold", IntegerType(), True),
        StructField("_source", StringType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_processed_at", TimestampType(), True),
        StructField("dt", DateType(), True),
    ])


# -----------------------------------------------------------------------------
# Spark Session
# -----------------------------------------------------------------------------


def create_spark_session(app_name: str) -> SparkSession:
    """Create Spark session with proper configuration."""
    return (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


# -----------------------------------------------------------------------------
# Bronze Reader
# -----------------------------------------------------------------------------


def read_bronze_source(spark: SparkSession, source: str, dt: str) -> DataFrame | None:
    """Read Bronze data for a specific source and date."""
    bronze_path = f"{DATALAKE_BRONZE}/source={source}/dt={dt}"

    if not Path(bronze_path).exists():
        logger.warning(f"Bronze path does not exist: {bronze_path}")
        return None

    logger.info(f"Reading Bronze: {bronze_path}")

    try:
        df = spark.read.json(f"{bronze_path}/hour=*/part-*.ndjson")
        record_count = df.count()
        logger.info(f"  -> Read {record_count} records from source={source}")
        return df
    except Exception as e:
        logger.error(f"Failed to read {bronze_path}: {e}")
        return None


# -----------------------------------------------------------------------------
# Transformations
# -----------------------------------------------------------------------------


def dedup_by_primary_key(df: DataFrame, pk: str) -> DataFrame:
    """Deduplicate by primary key, keeping latest _ingested_at."""
    window = Window.partitionBy(pk).orderBy(col("_ingested_at").desc())

    return (
        df
        .withColumn("_row_num", row_number().over(window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )


def drop_mandatory_nulls(df: DataFrame, mandatory_fields: list[str]) -> DataFrame:
    """Drop rows with nulls in mandatory fields."""
    for field in mandatory_fields:
        if field in df.columns:
            df = df.filter(col(field).isNotNull())
    return df


def add_processed_timestamp(df: DataFrame) -> DataFrame:
    """Add _processed_at timestamp."""
    return df.withColumn("_processed_at", current_timestamp())


def add_date_partition(df: DataFrame, dt: str) -> DataFrame:
    """Add dt partition column."""
    return df.withColumn("dt", to_date(lit(dt)))


def cast_timestamp_fields(df: DataFrame) -> DataFrame:
    """Cast timestamp string fields to TimestampType."""
    if "timestamp" in df.columns:
        df = df.withColumn(
            "timestamp",
            to_timestamp(col("timestamp")),
        )
    if "_ingested_at" in df.columns:
        df = df.withColumn(
            "_ingested_at",
            to_timestamp(col("_ingested_at")),
        )
    return df


# -----------------------------------------------------------------------------
# Domain Transformations
# -----------------------------------------------------------------------------


def transform_events_domain(spark: SparkSession, dt: str) -> DataFrame | None:
    """Transform user_events + clickstream into events domain."""
    logger.info("Transforming events domain...")

    dfs = []
    for source in DOMAIN_SOURCES["events"]:
        df = read_bronze_source(spark, source, dt)
        if df is not None:
            # Normalize event_id: clickstream uses request_id, user_events uses event_id
            if "request_id" in df.columns and "event_id" not in df.columns:
                df = df.withColumnRenamed("request_id", "event_id")
            df = cast_timestamp_fields(df)
            df = dedup_by_primary_key(df, "event_id")
            df = drop_mandatory_nulls(df, MANDATORY_FIELDS[source])
            dfs.append(df)

    if not dfs:
        return None

    # Union all sources
    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    result = add_processed_timestamp(result)
    result = add_date_partition(result, dt)

    # Select final columns
    final_columns = [
        "event_id", "user_id", "session_id", "event_type", "page_url",
        "path", "method", "status_code", "latency_ms", "device", "browser",
        "user_agent", "ip_address", "country", "timestamp",
        "_source", "_ingested_at", "_processed_at", "dt",
    ]
    existing_cols = [c for c in final_columns if c in result.columns]
    return result.select(*existing_cols)


def transform_transactions_domain(spark: SparkSession, dt: str) -> DataFrame | None:
    """Transform transactions + payments into transactions domain."""
    logger.info("Transforming transactions domain...")

    dfs = []
    for source in DOMAIN_SOURCES["transactions"]:
        df = read_bronze_source(spark, source, dt)
        if df is not None:
            # Normalize txn_id: payments uses payment_id, transactions uses txn_id
            if "payment_id" in df.columns and "txn_id" not in df.columns:
                df = df.withColumnRenamed("payment_id", "txn_id")
            df = cast_timestamp_fields(df)
            df = dedup_by_primary_key(df, "txn_id")
            df = drop_mandatory_nulls(df, MANDATORY_FIELDS[source])
            dfs.append(df)

    if not dfs:
        return None

    result = dfs[0]
    for df in dfs[1:]:
        result = result.unionByName(df, allowMissingColumns=True)

    result = add_processed_timestamp(result)
    result = add_date_partition(result, dt)

    final_columns = [
        "txn_id", "payment_id", "order_id", "user_id", "customer_id",
        "txn_type", "amount", "currency", "payment_method", "payment_status",
        "status", "gateway", "merchant_id", "transaction_fee",
        "timestamp", "payment_date",
        "_source", "_ingested_at", "_processed_at", "dt",
    ]
    existing_cols = [c for c in final_columns if c in result.columns]
    return result.select(*existing_cols)


def transform_customers_domain(spark: SparkSession, dt: str) -> DataFrame | None:
    """Transform crm + orders into customers domain."""
    logger.info("Transforming customers domain...")

    # Read CRM data
    crm_df = read_bronze_source(spark, "crm", dt)
    orders_df = read_bronze_source(spark, "orders", dt)

    if crm_df is None and orders_df is None:
        return None

    # Process CRM
    if crm_df is not None:
        crm_df = cast_timestamp_fields(crm_df)
        crm_df = dedup_by_primary_key(crm_df, "customer_id")
        crm_df = drop_mandatory_nulls(crm_df, MANDATORY_FIELDS["crm"])

    # Aggregate orders per customer
    if orders_df is not None:
        orders_df = cast_timestamp_fields(orders_df)
        orders_agg = (
            orders_df
            .groupBy("customer_id")
            .agg(
                {"order_id": "count", "total_amount": "sum"}
            )
            .withColumnRenamed("count(order_id)", "total_orders")
            .withColumnRenamed("sum(total_amount)", "total_spent")
        )

    # Join CRM with orders aggregates
    if crm_df is not None and orders_df is not None:
        result = crm_df.join(orders_agg, on="customer_id", how="left")
    elif crm_df is not None:
        result = crm_df.withColumn("total_orders", lit(0)).withColumn("total_spent", lit(0.0))
    else:
        result = orders_agg.withColumn("_source", lit("orders"))

    result = add_processed_timestamp(result)
    result = add_date_partition(result, dt)

    final_columns = [
        "customer_id", "first_name", "last_name", "email", "phone",
        "segment", "lifetime_value", "registered_date", "last_purchase_date",
        "total_orders", "total_spent",
        "_source", "_ingested_at", "_processed_at", "dt",
    ]
    existing_cols = [c for c in final_columns if c in result.columns]
    return result.select(*existing_cols)


def transform_inventory_domain(spark: SparkSession, dt: str) -> DataFrame | None:
    """Transform inventory + orders into inventory domain."""
    logger.info("Transforming inventory domain...")

    inventory_df = read_bronze_source(spark, "inventory", dt)
    orders_df = read_bronze_source(spark, "orders", dt)

    if inventory_df is None:
        return None

    inventory_df = cast_timestamp_fields(inventory_df)
    inventory_df = dedup_by_primary_key(inventory_df, "sku")
    inventory_df = drop_mandatory_nulls(inventory_df, MANDATORY_FIELDS["inventory"])

    # Aggregate orders to get units sold per product
    if orders_df is not None:
        orders_df = cast_timestamp_fields(orders_df)
        # Assuming product_id in orders maps to sku in inventory
        orders_agg = (
            orders_df
            .groupBy("product_id")
            .agg(
                {"order_id": "count", "quantity": "sum"}
            )
            .withColumnRenamed("product_id", "sku")
            .withColumnRenamed("count(order_id)", "orders_count")
            .withColumnRenamed("sum(quantity)", "units_sold")
        )
        result = inventory_df.join(orders_agg, on="sku", how="left")
    else:
        result = (
            inventory_df
            .withColumn("orders_count", lit(0))
            .withColumn("units_sold", lit(0))
        )

    result = add_processed_timestamp(result)
    result = add_date_partition(result, dt)

    final_columns = [
        "sku", "product_name", "category", "warehouse",
        "quantity_on_hand", "reorder_point", "unit_cost", "last_restock_date",
        "orders_count", "units_sold",
        "_source", "_ingested_at", "_processed_at", "dt",
    ]
    existing_cols = [c for c in final_columns if c in result.columns]
    return result.select(*existing_cols)


# -----------------------------------------------------------------------------
# Silver Writer
# -----------------------------------------------------------------------------


def write_silver(df: DataFrame, domain: str) -> None:
    """Write DataFrame to Silver layer."""
    silver_path = f"{DATALAKE_SILVER}/domain={domain}"
    logger.info(f"Writing Silver: {silver_path}")

    (
        df
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(silver_path)
    )

    logger.info(f"  -> Written {df.count()} records to domain={domain}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> None:
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Spark Bronze to Silver Job")
    logger.info(f"Processing Date: {PROCESSING_DATE}")
    logger.info(f"Bronze Path: {DATALAKE_BRONZE}")
    logger.info(f"Silver Path: {DATALAKE_SILVER}")
    logger.info("=" * 60)

    # Create output directory
    DATALAKE_SILVER.mkdir(parents=True, exist_ok=True)

    # Create Spark session
    spark = create_spark_session(f"Bronze2Silver_{PROCESSING_DATE}")

    try:
        # Transform each domain
        domain_transformers = {
            "events": transform_events_domain,
            "transactions": transform_transactions_domain,
            "customers": transform_customers_domain,
            "inventory": transform_inventory_domain,
        }

        summary: dict[str, int] = {}

        for domain, transformer in domain_transformers.items():
            logger.info(f"\n{'='*40}")
            logger.info(f"Processing domain: {domain}")
            logger.info(f"{'='*40}")

            df = transformer(spark, PROCESSING_DATE)

            if df is not None and df.count() > 0:
                write_silver(df, domain)
                summary[domain] = df.count()
            else:
                logger.warning(f"No data for domain: {domain}")
                summary[domain] = 0

        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY - Bronze to Silver")
        logger.info("=" * 60)
        for domain, count in summary.items():
            logger.info(f"  domain={domain}: {count} records")
        logger.info("=" * 60)

    finally:
        spark.stop()

    logger.info("Bronze to Silver job completed!")


if __name__ == "__main__":
    main()
