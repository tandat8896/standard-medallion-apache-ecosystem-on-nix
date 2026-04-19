#!/usr/bin/env python3
"""
spark_silver_to_gold_iot_anomaly.py

Detects anomalies in IoT sensor data using 3-sigma rule.
Analyzes sensor readings from Silver events (source=iot_sensors).

Usage:
    PROCESSING_DATE=2026-04-17 spark-submit \
        --master spark://127.0.0.1:7077 \
        scripts/spark_silver_to_gold_iot_anomaly.py
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


def read_silver_events(spark: SparkSession, date: str) -> DataFrame:
    path = f"{SILVER_ROOT}/domain=events/dt={date}"
    logger.info(f"Reading events from: {path}")
    df = spark.read.parquet(str(path))
    logger.info(f"Loaded {df.count()} event records")
    return df


def detect_iot_anomalies(events_df: DataFrame) -> DataFrame:
    """Detect anomalies in IoT sensor data using 3-sigma rule."""
    logger.info("Detecting IoT anomalies...")

    # Filter IoT sensor events only (from event_source field)
    # Note: IoT sensors might not be in events Silver - this is a placeholder
    iot_events = events_df.filter(F.col("event_source") == "iot_sensors")

    count = iot_events.count()
    logger.info(f"Filtered to {count} IoT sensor events")

    if count == 0:
        logger.warning("No IoT sensor events found in events Silver domain")
        # Return empty DataFrame with expected schema
        from pyspark.sql.types import StructType, StructField, StringType, DateType, LongType, DoubleType
        empty_schema = StructType([
            StructField("sensor_type", StringType(), True),
            StructField("anomaly_date", DateType(), True),
            StructField("total_readings", LongType(), True),
            StructField("anomaly_count", LongType(), True),
            StructField("avg_sensor_value", DoubleType(), True),
            StructField("min_sensor_value", DoubleType(), True),
            StructField("max_sensor_value", DoubleType(), True),
            StructField("avg_z_score", DoubleType(), True),
            StructField("max_abs_z_score", DoubleType(), True),
            StructField("anomaly_rate_pct", DoubleType(), True)
        ])
        return events_df.sparkSession.createDataFrame([], empty_schema)

    # Extract numeric sensor values from latency_ms as a proxy for sensor data
    # (This is a placeholder - real IoT data would be in a separate domain)
    iot_with_values = iot_events.withColumn(
        "sensor_value",
        F.col("latency_ms").cast(DoubleType())
    ).withColumn(
        "sensor_type",
        F.lit("latency")
    ).filter(F.col("sensor_value").isNotNull())

    # Calculate mean and stddev per sensor type
    stats = iot_with_values.groupBy("sensor_type").agg(
        F.avg("sensor_value").alias("mean_value"),
        F.stddev("sensor_value").alias("stddev_value"),
        F.count("event_id").alias("total_readings")
    )

    # Join back to calculate z-score
    iot_with_stats = iot_with_values.join(stats, "sensor_type")

    # Calculate z-score and flag anomalies (|z| > 3)
    iot_with_anomaly = iot_with_stats.withColumn(
        "z_score",
        (F.col("sensor_value") - F.col("mean_value")) / F.col("stddev_value")
    ).withColumn(
        "is_anomaly",
        F.when(F.abs(F.col("z_score")) > 3, True).otherwise(False)
    )

    # Aggregate anomalies by sensor type and date
    anomaly_summary = iot_with_anomaly.groupBy(
        "sensor_type",
        F.to_date("timestamp").alias("anomaly_date")
    ).agg(
        F.count("event_id").alias("total_readings"),
        F.sum(F.when(F.col("is_anomaly"), 1).otherwise(0)).alias("anomaly_count"),
        F.avg("sensor_value").alias("avg_sensor_value"),
        F.min("sensor_value").alias("min_sensor_value"),
        F.max("sensor_value").alias("max_sensor_value"),
        F.avg("z_score").alias("avg_z_score"),
        F.max(F.abs(F.col("z_score"))).alias("max_abs_z_score")
    ).withColumn(
        "anomaly_rate_pct",
        (F.col("anomaly_count") / F.col("total_readings") * 100)
    )

    logger.info(f"Detected anomalies in {anomaly_summary.count()} sensor-day combinations")
    return anomaly_summary


def add_gold_metadata(df: DataFrame, processing_date: str) -> DataFrame:
    return df.withColumn("_aggregated_at", F.current_timestamp()) \
             .withColumn("dt", F.to_date(F.lit(processing_date)))


def write_gold(df: DataFrame, output_path: str) -> None:
    logger.info(f"Writing to Gold: {output_path}")
    logger.info(f"Record count: {df.count()}")
    df.write.mode("overwrite").partitionBy("dt").parquet(output_path)
    logger.info("✅ Gold iot_anomaly_summary written successfully")


def run_silver_to_gold_iot_anomaly(processing_date: str) -> None:
    logger.info("=" * 70)
    logger.info("Starting Silver → Gold ETL for kpi=iot_anomaly_summary")
    logger.info(f"Processing date: {processing_date}")
    logger.info("=" * 70)

    spark = (
        SparkSession.builder
        .appName(f"Silver2Gold_IoTAnomaly_{processing_date}")
        .master("spark://127.0.0.1:7077")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    try:
        logger.info("\n--- Step 1: Reading Silver events ---")
        events_df = read_silver_events(spark, processing_date)

        logger.info("\n--- Step 2: Detecting IoT anomalies ---")
        anomaly_df = detect_iot_anomalies(events_df)

        logger.info("\n--- Step 3: Adding Gold metadata ---")
        gold_df = add_gold_metadata(anomaly_df, processing_date)

        logger.info("\n--- Step 4: Writing to Gold ---")
        output_path = str(GOLD_ROOT / "kpi=iot_anomaly_summary")
        write_gold(gold_df, output_path)

        logger.info("\n" + "=" * 70)
        logger.info("✅ Silver → Gold ETL completed successfully!")
        logger.info("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_to_gold_iot_anomaly(PROCESSING_DATE)
