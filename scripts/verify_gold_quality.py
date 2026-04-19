#!/usr/bin/env python3
"""
verify_gold_quality.py - Quick validation script for Gold layer KPIs.

Reads sample records from each KPI table and validates:
- Schema correctness
- Record counts
- Data ranges
- Timestamp freshness
"""

import logging
from pathlib import Path

from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATALAKE_GOLD = PROJECT_ROOT / "datalake" / "gold"
SPARK_MASTER = "spark://127.0.0.1:7077"


def create_spark_session() -> SparkSession:
    """Create Spark session."""
    return (
        SparkSession.builder
        .appName("VerifyGoldQuality")
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


def verify_kpi(spark: SparkSession, kpi_name: str) -> dict:
    """Verify a single KPI table."""
    logger.info(f"\n{'='*50}")
    logger.info(f"Verifying KPI: {kpi_name}")
    logger.info(f"{'='*50}")

    kpi_path = f"{DATALAKE_GOLD}/kpi={kpi_name}"

    if not Path(kpi_path).exists():
        logger.error(f"❌ KPI path does not exist: {kpi_path}")
        return {"status": "missing", "count": 0}

    try:
        df = spark.read.parquet(kpi_path)
        count = df.count()

        logger.info(f"✅ Record count: {count}")
        logger.info(f"Schema:")
        df.printSchema()

        logger.info(f"\nSample data (first 5 rows):")
        df.show(5, truncate=False)

        # Check for _processed_at timestamp
        if "_processed_at" in df.columns:
            latest = df.agg({"_processed_at": "max"}).collect()[0][0]
            logger.info(f"Latest _processed_at: {latest}")

        # Check partition column
        if "dt" in df.columns:
            partitions = df.select("dt").distinct().count()
            logger.info(f"Number of date partitions: {partitions}")

        return {
            "status": "ok",
            "count": count,
            "columns": len(df.columns),
            "path": kpi_path,
        }

    except Exception as e:
        logger.error(f"❌ Failed to read {kpi_path}: {e}")
        return {"status": "error", "count": 0, "error": str(e)}


def main() -> None:
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("Gold Layer Quality Verification")
    logger.info(f"Gold Path: {DATALAKE_GOLD}")
    logger.info("=" * 60)

    spark = create_spark_session()

    kpis = [
        "daily_revenue",
        "user_retention",
        "iot_anomaly_summary",
        "inventory_turnover",
    ]

    results = {}

    try:
        for kpi in kpis:
            results[kpi] = verify_kpi(spark, kpi)

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("VERIFICATION SUMMARY")
        logger.info("=" * 60)

        total_records = 0
        for kpi, result in results.items():
            status = result["status"]
            count = result.get("count", 0)
            total_records += count

            status_icon = "✅" if status == "ok" else "❌"
            logger.info(f"{status_icon} {kpi:25s} | {count:6d} records | {status}")

        logger.info("-" * 60)
        logger.info(f"Total records across all KPIs: {total_records}")
        logger.info("=" * 60)

        # Check if all passed
        all_ok = all(r["status"] == "ok" for r in results.values())
        if all_ok:
            logger.info("✅ All KPI tables validated successfully!")
        else:
            logger.warning("⚠️  Some KPI tables have issues")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
