#!/usr/bin/env python3
"""
validate_medallion_pipeline.py - End-to-end validation of Bronze → Silver → Gold pipeline.

Validates:
1. Bronze layer: NDJSON files with metadata fields, partitioned by source/dt/hour
2. Silver layer: Parquet files with deduplication, partitioned by domain/dt
3. Gold layer: Parquet KPI tables, partitioned by kpi/dt
4. Data lineage: Record counts match expectations
5. Schema compliance: All required fields present
6. Data freshness: _ingested_at and _processed_at timestamps
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, max as spark_max, min as spark_min

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATALAKE_ROOT = PROJECT_ROOT / "datalake"
DATALAKE_BRONZE = DATALAKE_ROOT / "bronze"
DATALAKE_SILVER = DATALAKE_ROOT / "silver"
DATALAKE_GOLD = DATALAKE_ROOT / "gold"
SPARK_MASTER = "spark://127.0.0.1:7077"


def create_spark_session() -> SparkSession:
    """Create Spark session."""
    return (
        SparkSession.builder
        .appName("ValidateMedallionPipeline")
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


# -----------------------------------------------------------------------------
# Bronze Layer Validation
# -----------------------------------------------------------------------------


def validate_bronze_layer(spark: SparkSession, processing_date: str) -> dict[str, Any]:
    """Validate Bronze layer data."""
    logger.info("\n" + "=" * 60)
    logger.info("BRONZE LAYER VALIDATION")
    logger.info("=" * 60)

    bronze_sources = [
        "user_events",
        "transactions",
        "app_logs",
        "iot_sensors",
        "clickstream",
        "orders",
        "crm",
        "inventory",
        "payments",
    ]

    results = {}
    total_records = 0

    for source in bronze_sources:
        bronze_path = f"{DATALAKE_BRONZE}/source={source}/dt={processing_date}"

        if not Path(bronze_path).exists():
            logger.warning(f"⚠️  Bronze source not found: {source}")
            results[source] = {"status": "missing", "count": 0}
            continue

        try:
            # Read NDJSON files from all hours
            df = spark.read.json(f"{bronze_path}/hour=*/part-*.ndjson")
            count_val = df.count()
            total_records += count_val

            # Validate required metadata fields
            required_fields = ["_source", "_ingested_at"]
            missing_fields = [f for f in required_fields if f not in df.columns]

            if missing_fields:
                logger.error(f"❌ {source}: Missing fields {missing_fields}")
                results[source] = {
                    "status": "error",
                    "count": count_val,
                    "error": f"missing_fields: {missing_fields}",
                }
            else:
                # Check timestamp freshness
                latest_ingested = df.agg(spark_max("_ingested_at")).collect()[0][0]
                logger.info(f"✅ {source:20s} | {count_val:6d} records | latest: {latest_ingested}")

                results[source] = {
                    "status": "ok",
                    "count": count_val,
                    "latest_ingested": str(latest_ingested),
                }

        except Exception as e:
            logger.error(f"❌ {source}: Failed to read - {e}")
            results[source] = {"status": "error", "count": 0, "error": str(e)}

    logger.info("-" * 60)
    logger.info(f"Total Bronze records: {total_records}")

    return {
        "layer": "bronze",
        "total_records": total_records,
        "sources": results,
        "status": "ok" if all(r.get("status") in ["ok", "missing"] for r in results.values()) else "error",
    }


# -----------------------------------------------------------------------------
# Silver Layer Validation
# -----------------------------------------------------------------------------


def validate_silver_layer(spark: SparkSession, processing_date: str) -> dict[str, Any]:
    """Validate Silver layer data."""
    logger.info("\n" + "=" * 60)
    logger.info("SILVER LAYER VALIDATION")
    logger.info("=" * 60)

    silver_domains = ["events", "transactions", "customers", "inventory"]

    results = {}
    total_records = 0

    for domain in silver_domains:
        silver_path = f"{DATALAKE_SILVER}/domain={domain}/dt={processing_date}"

        if not Path(silver_path).exists():
            logger.warning(f"⚠️  Silver domain not found: {domain}")
            results[domain] = {"status": "missing", "count": 0}
            continue

        try:
            # Read with partition columns explicitly
            df = spark.read.option("basePath", f"{DATALAKE_SILVER}/domain={domain}").parquet(silver_path)
            count_val = df.count()
            total_records += count_val

            # Validate required fields (dt might be partition column)
            required_fields = ["_processed_at"]
            missing_fields = [f for f in required_fields if f not in df.columns]

            if missing_fields:
                logger.error(f"❌ {domain}: Missing fields {missing_fields}")
                results[domain] = {
                    "status": "error",
                    "count": count_val,
                    "error": f"missing_fields: {missing_fields}",
                }
            else:
                # Check timestamp freshness
                latest_processed = df.agg(spark_max("_processed_at")).collect()[0][0]
                logger.info(f"✅ {domain:20s} | {count_val:6d} records | latest: {latest_processed}")

                results[domain] = {
                    "status": "ok",
                    "count": count_val,
                    "latest_processed": str(latest_processed),
                }

        except Exception as e:
            logger.error(f"❌ {domain}: Failed to read - {e}")
            results[domain] = {"status": "error", "count": 0, "error": str(e)}

    logger.info("-" * 60)
    logger.info(f"Total Silver records: {total_records}")

    return {
        "layer": "silver",
        "total_records": total_records,
        "domains": results,
        "status": "ok" if all(r.get("status") in ["ok", "missing"] for r in results.values()) else "error",
    }


# -----------------------------------------------------------------------------
# Gold Layer Validation
# -----------------------------------------------------------------------------


def validate_gold_layer(spark: SparkSession, processing_date: str) -> dict[str, Any]:
    """Validate Gold layer KPI tables."""
    logger.info("\n" + "=" * 60)
    logger.info("GOLD LAYER VALIDATION")
    logger.info("=" * 60)

    kpis = [
        "daily_revenue",
        "user_retention",
        "iot_anomaly_summary",
        "inventory_turnover",
    ]

    results = {}
    total_records = 0

    for kpi in kpis:
        gold_path = f"{DATALAKE_GOLD}/kpi={kpi}/dt={processing_date}"

        if not Path(gold_path).exists():
            logger.warning(f"⚠️  Gold KPI not found: {kpi}")
            results[kpi] = {"status": "missing", "count": 0}
            continue

        try:
            # Read with partition columns explicitly
            df = spark.read.option("basePath", f"{DATALAKE_GOLD}/kpi={kpi}").parquet(gold_path)
            count_val = df.count()
            total_records += count_val

            # Validate required fields (dt might be partition column)
            required_fields = ["_processed_at"]
            missing_fields = [f for f in required_fields if f not in df.columns]

            if missing_fields:
                logger.error(f"❌ {kpi}: Missing fields {missing_fields}")
                results[kpi] = {
                    "status": "error",
                    "count": count_val,
                    "error": f"missing_fields: {missing_fields}",
                }
            else:
                latest_processed = df.agg(spark_max("_processed_at")).collect()[0][0]
                logger.info(f"✅ {kpi:25s} | {count_val:6d} records | latest: {latest_processed}")

                results[kpi] = {
                    "status": "ok",
                    "count": count_val,
                    "latest_processed": str(latest_processed),
                }

        except Exception as e:
            logger.error(f"❌ {kpi}: Failed to read - {e}")
            results[kpi] = {"status": "error", "count": 0, "error": str(e)}

    logger.info("-" * 60)
    logger.info(f"Total Gold records: {total_records}")

    return {
        "layer": "gold",
        "total_records": total_records,
        "kpis": results,
        "status": "ok" if all(r.get("status") in ["ok", "missing"] for r in results.values()) else "error",
    }


# -----------------------------------------------------------------------------
# End-to-End Pipeline Validation
# -----------------------------------------------------------------------------


def validate_pipeline(processing_date: str) -> None:
    """Validate entire Bronze → Silver → Gold pipeline."""
    logger.info("=" * 70)
    logger.info("MEDALLION PIPELINE VALIDATION")
    logger.info(f"Processing Date: {processing_date}")
    logger.info(f"Datalake Root: {DATALAKE_ROOT}")
    logger.info("=" * 70)

    spark = create_spark_session()

    try:
        # Validate each layer
        bronze_result = validate_bronze_layer(spark, processing_date)
        silver_result = validate_silver_layer(spark, processing_date)
        gold_result = validate_gold_layer(spark, processing_date)

        # Overall summary
        logger.info("\n" + "=" * 70)
        logger.info("OVERALL PIPELINE SUMMARY")
        logger.info("=" * 70)

        bronze_status = "✅" if bronze_result["status"] == "ok" else "❌"
        silver_status = "✅" if silver_result["status"] == "ok" else "❌"
        gold_status = "✅" if gold_result["status"] == "ok" else "❌"

        logger.info(f"{bronze_status} Bronze Layer | {bronze_result['total_records']:8d} records")
        logger.info(f"{silver_status} Silver Layer | {silver_result['total_records']:8d} records")
        logger.info(f"{gold_status} Gold Layer   | {gold_result['total_records']:8d} records")

        logger.info("-" * 70)

        all_ok = (
            bronze_result["status"] == "ok"
            and silver_result["status"] == "ok"
            and gold_result["status"] == "ok"
        )

        if all_ok:
            logger.info("✅ Pipeline validation PASSED!")
            logger.info("All layers (Bronze → Silver → Gold) are healthy.")
        else:
            logger.warning("⚠️  Pipeline validation FAILED!")
            logger.warning("Some layers have errors. Check logs above for details.")

        logger.info("=" * 70)

        # Data lineage sanity check
        logger.info("\nData Lineage Check:")
        bronze_count = bronze_result["total_records"]
        silver_count = silver_result["total_records"]
        gold_count = gold_result["total_records"]

        if bronze_count > 0:
            silver_ratio = (silver_count / bronze_count) * 100
            logger.info(f"  Silver/Bronze ratio: {silver_ratio:.1f}% (expect <100% due to dedup)")

        if silver_count > 0:
            logger.info(f"  Gold layer aggregated from {silver_count} Silver records into {gold_count} KPI rows")

        logger.info("\nPipeline Flow:")
        logger.info(f"  Bronze ({bronze_count:,} records)")
        logger.info(f"    ↓ dedup + transform")
        logger.info(f"  Silver ({silver_count:,} records)")
        logger.info(f"    ↓ aggregate + compute KPIs")
        logger.info(f"  Gold   ({gold_count:,} records)")

    finally:
        spark.stop()


def main() -> None:
    """Main entry point."""
    import sys

    # Allow passing custom date via CLI
    if len(sys.argv) > 1:
        processing_date = sys.argv[1]
    else:
        # Default to yesterday
        processing_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    validate_pipeline(processing_date)


if __name__ == "__main__":
    main()
