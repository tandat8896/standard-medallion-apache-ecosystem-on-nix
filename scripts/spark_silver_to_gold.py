#!/usr/bin/env python3
"""
spark_silver_to_gold.py - Spark job to aggregate Silver into Gold KPIs.

Reads Silver partitions and computes 4 KPIs:
    - daily_revenue       : Revenue aggregation from transactions × events
    - user_retention      : Cohort-based retention analysis from events
    - iot_anomaly_summary : 3-sigma anomaly detection from iot_sensors
    - inventory_turnover  : Inventory turnover ratio from inventory × transactions

Writes Parquet to datalake/gold/kpi=<name>/dt=YYYY-MM-DD/

Usage:
    spark-submit --master spark://127.0.0.1:7077 \\
        --conf spark.sql.parquet.compression.codec=snappy \\
        --conf spark.sql.shuffle.partitions=8 \\
        scripts/spark_silver_to_gold.py

    # Or with custom date:
    PROCESSING_DATE=2026-04-17 spark-submit ... scripts/spark_silver_to_gold.py
"""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    count,
    countDistinct,
    current_timestamp,
    date_sub,
    datediff,
    expr,
    first,
    lit,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    stddev,
    sum as spark_sum,
    to_date,
    when,
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
DATALAKE_SILVER = PROJECT_ROOT / "datalake" / "silver"
DATALAKE_GOLD = PROJECT_ROOT / "datalake" / "gold"
DATALAKE_BRONZE = PROJECT_ROOT / "datalake" / "bronze"

# Spark config
SPARK_MASTER = "spark://127.0.0.1:7077"


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
# Silver Readers
# -----------------------------------------------------------------------------


def read_silver_domain(spark: SparkSession, domain: str, dt: str) -> DataFrame | None:
    """Read Silver data for a specific domain and date."""
    silver_path = f"{DATALAKE_SILVER}/domain={domain}/dt={dt}"

    if not Path(silver_path).exists():
        logger.warning(f"Silver path does not exist: {silver_path}")
        return None

    logger.info(f"Reading Silver: {silver_path}")

    try:
        df = spark.read.parquet(silver_path)
        record_count = df.count()
        logger.info(f"  -> Read {record_count} records from domain={domain}")
        return df
    except Exception as e:
        logger.error(f"Failed to read {silver_path}: {e}")
        return None


def read_bronze_source(spark: SparkSession, source: str, dt: str) -> DataFrame | None:
    """Read Bronze data directly for sources not in Silver (e.g., iot_sensors)."""
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
# KPI 1: Daily Revenue
# -----------------------------------------------------------------------------


def compute_daily_revenue(spark: SparkSession, dt: str) -> DataFrame | None:
    """
    Compute daily revenue KPI.

    Aggregates:
    - Total revenue
    - Transaction count
    - Average transaction value
    - Revenue by payment method
    - Revenue by currency
    """
    logger.info("Computing KPI: daily_revenue")

    transactions_df = read_silver_domain(spark, "transactions", dt)
    events_df = read_silver_domain(spark, "events", dt)

    if transactions_df is None:
        logger.warning("No transactions data for daily_revenue")
        return None

    # Filter successful transactions
    txn_df = transactions_df.filter(
        (col("status") == "completed") | (col("payment_status") == "success")
    )

    if txn_df.count() == 0:
        logger.warning("No completed transactions")
        return None

    # Overall daily revenue
    overall = txn_df.agg(
        spark_sum("amount").alias("total_revenue"),
        count("txn_id").alias("transaction_count"),
        avg("amount").alias("avg_transaction_value"),
        countDistinct("user_id").alias("unique_customers"),
    ).withColumn("breakdown_type", lit("overall")).withColumn("breakdown_value", lit("all"))

    # Revenue by payment method
    by_payment = (
        txn_df
        .groupBy("payment_method")
        .agg(
            spark_sum("amount").alias("total_revenue"),
            count("txn_id").alias("transaction_count"),
            avg("amount").alias("avg_transaction_value"),
            countDistinct("user_id").alias("unique_customers"),
        )
        .withColumn("breakdown_type", lit("payment_method"))
        .withColumnRenamed("payment_method", "breakdown_value")
    )

    # Revenue by currency
    by_currency = (
        txn_df
        .groupBy("currency")
        .agg(
            spark_sum("amount").alias("total_revenue"),
            count("txn_id").alias("transaction_count"),
            avg("amount").alias("avg_transaction_value"),
            countDistinct("user_id").alias("unique_customers"),
        )
        .withColumn("breakdown_type", lit("currency"))
        .withColumnRenamed("currency", "breakdown_value")
    )

    # Add event conversion metrics if events available
    if events_df is not None:
        event_count = events_df.count()
        conversion_rate = txn_df.count() / event_count if event_count > 0 else 0
        overall = overall.withColumn("conversion_rate", lit(conversion_rate))
        by_payment = by_payment.withColumn("conversion_rate", lit(None))
        by_currency = by_currency.withColumn("conversion_rate", lit(None))
    else:
        overall = overall.withColumn("conversion_rate", lit(None))
        by_payment = by_payment.withColumn("conversion_rate", lit(None))
        by_currency = by_currency.withColumn("conversion_rate", lit(None))

    # Union all breakdowns
    result = overall.unionByName(by_payment).unionByName(by_currency)

    # Add metadata
    result = (
        result
        .withColumn("dt", to_date(lit(dt)))
        .withColumn("_processed_at", current_timestamp())
    )

    # Round numeric columns
    result = (
        result
        .withColumn("total_revenue", spark_round("total_revenue", 2))
        .withColumn("avg_transaction_value", spark_round("avg_transaction_value", 2))
    )

    return result


# -----------------------------------------------------------------------------
# KPI 2: User Retention (Cohort Analysis)
# -----------------------------------------------------------------------------


def compute_user_retention(spark: SparkSession, dt: str) -> DataFrame | None:
    """
    Compute user retention cohort analysis.

    For each cohort (first activity date), calculate:
    - Day 1, Day 7, Day 30 retention rates
    - Total users in cohort
    - Active users at each retention window
    """
    logger.info("Computing KPI: user_retention")

    events_df = read_silver_domain(spark, "events", dt)

    if events_df is None:
        logger.warning("No events data for user_retention")
        return None

    # Need multiple days of data for retention
    # For demo, we'll compute based on available data
    # In production, would read 30+ days of Silver

    # Get first activity date per user
    user_first_activity = (
        events_df
        .groupBy("user_id")
        .agg(
            spark_min("timestamp").alias("first_activity"),
            spark_max("timestamp").alias("last_activity"),
            count("event_id").alias("total_events"),
        )
    )

    # Calculate cohort date (truncate to date)
    user_cohorts = (
        user_first_activity
        .withColumn("cohort_date", to_date("first_activity"))
        .withColumn("last_active_date", to_date("last_activity"))
        .withColumn("days_active", datediff("last_activity", "first_activity"))
    )

    # Aggregate by cohort
    cohort_stats = (
        user_cohorts
        .groupBy("cohort_date")
        .agg(
            count("user_id").alias("cohort_size"),
            avg("days_active").alias("avg_days_active"),
            avg("total_events").alias("avg_events_per_user"),
            spark_sum(when(col("days_active") >= 1, 1).otherwise(0)).alias("retained_day_1"),
            spark_sum(when(col("days_active") >= 7, 1).otherwise(0)).alias("retained_day_7"),
            spark_sum(when(col("days_active") >= 30, 1).otherwise(0)).alias("retained_day_30"),
        )
    )

    # Calculate retention rates
    result = (
        cohort_stats
        .withColumn("retention_day_1", spark_round(col("retained_day_1") / col("cohort_size") * 100, 2))
        .withColumn("retention_day_7", spark_round(col("retained_day_7") / col("cohort_size") * 100, 2))
        .withColumn("retention_day_30", spark_round(col("retained_day_30") / col("cohort_size") * 100, 2))
        .withColumn("avg_days_active", spark_round("avg_days_active", 2))
        .withColumn("avg_events_per_user", spark_round("avg_events_per_user", 2))
        .withColumn("dt", to_date(lit(dt)))
        .withColumn("_processed_at", current_timestamp())
    )

    return result


# -----------------------------------------------------------------------------
# KPI 3: IoT Anomaly Summary (3-sigma detection)
# -----------------------------------------------------------------------------


def compute_iot_anomaly_summary(spark: SparkSession, dt: str) -> DataFrame | None:
    """
    Compute IoT anomaly detection using 3-sigma rule.

    For each sensor type:
    - Calculate mean and stddev
    - Flag readings outside 3 standard deviations
    - Summarize anomaly counts by sensor and location
    """
    logger.info("Computing KPI: iot_anomaly_summary")

    # IoT sensors are in Bronze, not Silver (they're not in the 4 Silver domains)
    iot_df = read_bronze_source(spark, "iot_sensors", dt)

    if iot_df is None:
        logger.warning("No IoT sensor data for anomaly detection")
        return None

    # Cast value to double
    iot_df = iot_df.withColumn("value", col("value").cast("double"))

    # Calculate stats per sensor_type
    sensor_stats = (
        iot_df
        .groupBy("sensor_type")
        .agg(
            avg("value").alias("mean_value"),
            stddev("value").alias("stddev_value"),
            count("reading_id").alias("total_readings"),
            spark_min("value").alias("min_value"),
            spark_max("value").alias("max_value"),
        )
    )

    # Join back to get anomaly thresholds
    iot_with_stats = iot_df.join(sensor_stats, on="sensor_type")

    # Flag anomalies (outside 3 sigma)
    iot_with_anomaly = (
        iot_with_stats
        .withColumn("lower_bound", col("mean_value") - 3 * col("stddev_value"))
        .withColumn("upper_bound", col("mean_value") + 3 * col("stddev_value"))
        .withColumn(
            "is_anomaly",
            when(
                (col("value") < col("lower_bound")) | (col("value") > col("upper_bound")),
                True,
            ).otherwise(False),
        )
        .withColumn(
            "anomaly_type",
            when(col("value") < col("lower_bound"), "below_threshold")
            .when(col("value") > col("upper_bound"), "above_threshold")
            .otherwise("normal"),
        )
        .withColumn(
            "deviation_sigma",
            spark_abs(col("value") - col("mean_value")) / col("stddev_value"),
        )
    )

    # Aggregate anomalies by sensor_type and location
    anomaly_summary = (
        iot_with_anomaly
        .groupBy("sensor_type", "location")
        .agg(
            count("reading_id").alias("total_readings"),
            spark_sum(when(col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
            avg("value").alias("avg_value"),
            spark_max("deviation_sigma").alias("max_deviation_sigma"),
            first("mean_value").alias("baseline_mean"),
            first("stddev_value").alias("baseline_stddev"),
            first("lower_bound").alias("lower_threshold"),
            first("upper_bound").alias("upper_threshold"),
        )
    )

    # Calculate anomaly rate
    result = (
        anomaly_summary
        .withColumn(
            "anomaly_rate",
            spark_round(col("anomaly_count") / col("total_readings") * 100, 2),
        )
        .withColumn("avg_value", spark_round("avg_value", 2))
        .withColumn("max_deviation_sigma", spark_round("max_deviation_sigma", 2))
        .withColumn("baseline_mean", spark_round("baseline_mean", 2))
        .withColumn("baseline_stddev", spark_round("baseline_stddev", 2))
        .withColumn("lower_threshold", spark_round("lower_threshold", 2))
        .withColumn("upper_threshold", spark_round("upper_threshold", 2))
        .withColumn("dt", to_date(lit(dt)))
        .withColumn("_processed_at", current_timestamp())
    )

    return result


# -----------------------------------------------------------------------------
# KPI 4: Inventory Turnover
# -----------------------------------------------------------------------------


def compute_inventory_turnover(spark: SparkSession, dt: str) -> DataFrame | None:
    """
    Compute inventory turnover ratio.

    Turnover = Cost of Goods Sold / Average Inventory Value

    Metrics per category/warehouse:
    - Units sold
    - Units on hand
    - Turnover ratio
    - Days of supply
    - Reorder alerts
    """
    logger.info("Computing KPI: inventory_turnover")

    inventory_df = read_silver_domain(spark, "inventory", dt)
    transactions_df = read_silver_domain(spark, "transactions", dt)

    if inventory_df is None:
        logger.warning("No inventory data for turnover calculation")
        return None

    # Aggregate inventory by category and warehouse
    inventory_agg = (
        inventory_df
        .groupBy("category", "warehouse")
        .agg(
            spark_sum("quantity_on_hand").alias("total_units_on_hand"),
            spark_sum(col("quantity_on_hand") * col("unit_cost")).alias("inventory_value"),
            avg("unit_cost").alias("avg_unit_cost"),
            spark_sum(when(col("quantity_on_hand") <= col("reorder_point"), 1).otherwise(0)).alias("items_below_reorder"),
            count("sku").alias("total_skus"),
            spark_sum(col("units_sold")).alias("units_sold"),
            spark_sum(col("orders_count")).alias("orders_count"),
        )
    )

    # If we have transactions, calculate COGS
    if transactions_df is not None:
        # Estimate COGS from transaction amounts (simplified)
        cogs = (
            transactions_df
            .filter(col("txn_type") == "purchase")
            .agg(spark_sum("amount").alias("total_cogs"))
            .collect()[0]["total_cogs"]
        ) or 0
    else:
        cogs = 0

    # Calculate turnover metrics
    result = (
        inventory_agg
        .withColumn("cogs_estimate", lit(cogs) / count("*").over(Window.partitionBy()))
        .withColumn(
            "turnover_ratio",
            when(col("inventory_value") > 0, col("cogs_estimate") / col("inventory_value"))
            .otherwise(0),
        )
        .withColumn(
            "days_of_supply",
            when(
                col("units_sold") > 0,
                col("total_units_on_hand") / (col("units_sold") / 30),  # Assuming 30-day period
            ).otherwise(999),  # High number if no sales
        )
        .withColumn(
            "reorder_alert",
            when(col("items_below_reorder") > 0, True).otherwise(False),
        )
    )

    # Round and finalize
    result = (
        result
        .withColumn("inventory_value", spark_round("inventory_value", 2))
        .withColumn("avg_unit_cost", spark_round("avg_unit_cost", 2))
        .withColumn("turnover_ratio", spark_round("turnover_ratio", 4))
        .withColumn("days_of_supply", spark_round("days_of_supply", 1))
        .withColumn("dt", to_date(lit(dt)))
        .withColumn("_processed_at", current_timestamp())
    )

    # Select final columns
    final_columns = [
        "category", "warehouse", "total_skus", "total_units_on_hand",
        "inventory_value", "avg_unit_cost", "units_sold", "orders_count",
        "turnover_ratio", "days_of_supply", "items_below_reorder", "reorder_alert",
        "dt", "_processed_at",
    ]

    return result.select(*[c for c in final_columns if c in result.columns])


# -----------------------------------------------------------------------------
# Gold Writer
# -----------------------------------------------------------------------------


def write_gold(df: DataFrame, kpi: str) -> None:
    """Write DataFrame to Gold layer."""
    gold_path = f"{DATALAKE_GOLD}/kpi={kpi}"
    logger.info(f"Writing Gold: {gold_path}")

    (
        df
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(gold_path)
    )

    logger.info(f"  -> Written {df.count()} records to kpi={kpi}")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> None:
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Spark Silver to Gold Job")
    logger.info(f"Processing Date: {PROCESSING_DATE}")
    logger.info(f"Silver Path: {DATALAKE_SILVER}")
    logger.info(f"Gold Path: {DATALAKE_GOLD}")
    logger.info("=" * 60)

    # Create output directory
    DATALAKE_GOLD.mkdir(parents=True, exist_ok=True)

    # Create Spark session
    spark = create_spark_session(f"Silver2Gold_{PROCESSING_DATE}")

    try:
        # Compute each KPI
        kpi_computers = {
            "daily_revenue": compute_daily_revenue,
            "user_retention": compute_user_retention,
            "iot_anomaly_summary": compute_iot_anomaly_summary,
            "inventory_turnover": compute_inventory_turnover,
        }

        summary: dict[str, int] = {}

        for kpi, computer in kpi_computers.items():
            logger.info(f"\n{'='*40}")
            logger.info(f"Computing KPI: {kpi}")
            logger.info(f"{'='*40}")

            df = computer(spark, PROCESSING_DATE)

            if df is not None and df.count() > 0:
                write_gold(df, kpi)
                summary[kpi] = df.count()

                # Show sample data
                logger.info(f"Sample {kpi} data:")
                df.show(5, truncate=False)
            else:
                logger.warning(f"No data for KPI: {kpi}")
                summary[kpi] = 0

        # Print summary
        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY - Silver to Gold")
        logger.info("=" * 60)
        for kpi, count in summary.items():
            logger.info(f"  kpi={kpi}: {count} records")
        logger.info("=" * 60)

    finally:
        spark.stop()

    logger.info("Silver to Gold job completed!")


if __name__ == "__main__":
    main()
