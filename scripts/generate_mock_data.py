#!/usr/bin/env python3
"""
generate_mock_data.py - Mock data generator for tandat-interview ETL platform.

Generates:
1. Kafka messages to 5 topics (events.user, events.txn, events.iot, logs.app, clicks.web)
2. Batch files to ingest/raw/ (orders, crm, inventory, payments)
3. Bronze NDJSON files to datalake/bronze/ for all 10 sources

Usage:
    python scripts/generate_mock_data.py
"""

import json
import logging
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
from faker import Faker
from kafka import KafkaProducer
from kafka.errors import KafkaError

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)

# Processing date = yesterday
PROCESSING_DATE_DT = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
PROCESSING_DATE = PROCESSING_DATE_DT.strftime("%Y-%m-%d")
PROCESSING_DATE_COMPACT = PROCESSING_DATE.replace("-", "")
PROCESSING_DATE_START = PROCESSING_DATE_DT
PROCESSING_DATE_END = PROCESSING_DATE_DT.replace(hour=23, minute=59, second=59)

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
INGEST_RAW = PROJECT_ROOT / "ingest" / "raw"
DATALAKE_BRONZE = PROJECT_ROOT / "datalake" / "bronze"

# Kafka config
KAFKA_BOOTSTRAP = "localhost:9092"

# Record counts
KAFKA_COUNTS = {
    "events.user": 200,
    "events.txn": 100,
    "events.iot": 150,
    "logs.app": 100,
    "clicks.web": 200,
}

BATCH_COUNTS = {
    "orders": 80,
    "crm": 50,
    "inventory": 40,
    "payments": 80,
}

# Mapping Kafka topics to Bronze sources
KAFKA_TO_SOURCE = {
    "events.user": "user_events",
    "events.txn": "transactions",
    "events.iot": "iot_sensors",
    "logs.app": "app_logs",
    "clicks.web": "clickstream",
}

# Batch folder to Bronze source
BATCH_TO_SOURCE = {
    "orders": "orders",
    "crm": "crm",
    "inventory": "inventory",
    "payments": "payments",
}

# Summary tracking
summary: dict[str, dict[str, int]] = {
    "kafka": {},
    "batch": {},
    "bronze": {},
}


# -----------------------------------------------------------------------------
# Data Generators
# -----------------------------------------------------------------------------


def gen_user_event() -> dict[str, Any]:
    """Generate a user event (clickstream, page_view, add_to_cart)."""
    event_types = ["page_view", "click", "add_to_cart", "remove_from_cart", "search"]
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": fake.uuid4()[:8],
        "session_id": fake.uuid4()[:12],
        "event_type": random.choice(event_types),
        "page_url": fake.uri_path(),
        "referrer": fake.uri() if random.random() > 0.3 else None,
        "device": random.choice(["desktop", "mobile", "tablet"]),
        "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        "country": fake.country_code(),
        "timestamp": fake.date_time_between(
            start_date=PROCESSING_DATE_START,
            end_date=PROCESSING_DATE_END,
        ).isoformat() + "Z",
    }


def gen_transaction() -> dict[str, Any]:
    """Generate a transaction event (purchase, refund)."""
    txn_type = random.choice(["purchase", "refund"])
    amount = round(random.uniform(10.0, 500.0), 2)
    if txn_type == "refund":
        amount = -abs(amount)
    return {
        "txn_id": str(uuid.uuid4()),
        "user_id": fake.uuid4()[:8],
        "txn_type": txn_type,
        "amount": amount,
        "currency": random.choice(["USD", "EUR", "VND"]),
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
        "status": random.choice(["completed", "pending", "failed"]),
        "merchant_id": f"M{random.randint(1000, 9999)}",
        "timestamp": fake.date_time_between(
            start_date=PROCESSING_DATE_START,
            end_date=PROCESSING_DATE_END,
        ).isoformat() + "Z",
    }


def gen_iot_sensor() -> dict[str, Any]:
    """Generate IoT sensor reading (temp, pressure, vibration)."""
    sensor_types = ["temperature", "pressure", "vibration", "humidity"]
    sensor_type = random.choice(sensor_types)

    value_ranges = {
        "temperature": (15.0, 85.0),
        "pressure": (980.0, 1050.0),
        "vibration": (0.0, 10.0),
        "humidity": (20.0, 95.0),
    }
    min_val, max_val = value_ranges[sensor_type]

    return {
        "reading_id": str(uuid.uuid4()),
        "sensor_id": f"SENSOR-{random.randint(1, 50):03d}",
        "sensor_type": sensor_type,
        "value": round(random.uniform(min_val, max_val), 2),
        "unit": {"temperature": "C", "pressure": "hPa", "vibration": "g", "humidity": "%"}[sensor_type],
        "location": random.choice(["factory_a", "factory_b", "warehouse_1", "warehouse_2"]),
        "timestamp": fake.date_time_between(
            start_date=PROCESSING_DATE_START,
            end_date=PROCESSING_DATE_END,
        ).isoformat() + "Z",
    }


def gen_app_log() -> dict[str, Any]:
    """Generate application log entry."""
    levels = ["INFO", "WARN", "ERROR", "DEBUG"]
    weights = [0.6, 0.2, 0.15, 0.05]
    level = random.choices(levels, weights=weights)[0]

    services = ["auth-service", "payment-gateway", "order-processor", "inventory-api", "notification-service"]

    messages = {
        "INFO": ["Request processed successfully", "User logged in", "Cache refreshed", "Health check passed"],
        "WARN": ["High latency detected", "Retry attempt", "Connection pool near limit", "Deprecated API called"],
        "ERROR": ["Database connection failed", "Payment declined", "Timeout exceeded", "Null pointer exception"],
        "DEBUG": ["Processing request", "Fetching user data", "Validating input", "Starting transaction"],
    }

    return {
        "log_id": str(uuid.uuid4()),
        "level": level,
        "service": random.choice(services),
        "message": random.choice(messages[level]),
        "trace_id": fake.uuid4()[:16],
        "span_id": fake.uuid4()[:8],
        "host": f"node-{random.randint(1, 10)}",
        "timestamp": fake.date_time_between(
            start_date=PROCESSING_DATE_START,
            end_date=PROCESSING_DATE_END,
        ).isoformat() + "Z",
    }


def gen_web_click() -> dict[str, Any]:
    """Generate nginx access log entry."""
    methods = ["GET", "POST", "PUT", "DELETE"]
    method_weights = [0.7, 0.2, 0.05, 0.05]

    paths = ["/api/users", "/api/products", "/api/orders", "/api/cart", "/static/js/app.js", "/static/css/style.css"]
    status_codes = [200, 201, 301, 400, 401, 403, 404, 500]
    status_weights = [0.7, 0.1, 0.02, 0.05, 0.03, 0.02, 0.05, 0.03]

    return {
        "request_id": str(uuid.uuid4()),
        "method": random.choices(methods, weights=method_weights)[0],
        "path": random.choice(paths),
        "status_code": random.choices(status_codes, weights=status_weights)[0],
        "latency_ms": random.randint(5, 2000),
        "bytes_sent": random.randint(100, 50000),
        "user_agent": fake.user_agent(),
        "ip_address": fake.ipv4(),
        "timestamp": fake.date_time_between(
            start_date=PROCESSING_DATE_START,
            end_date=PROCESSING_DATE_END,
        ).isoformat() + "Z",
    }


def gen_order() -> dict[str, Any]:
    """Generate order record for batch CSV."""
    return {
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product_id": f"PROD-{random.randint(100, 999)}",
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(10.0, 200.0), 2),
        "total_amount": 0.0,  # Will be calculated
        "discount_pct": random.choice([0, 5, 10, 15, 20]),
        "status": random.choice(["pending", "confirmed", "shipped", "delivered", "cancelled"]),
        "order_date": PROCESSING_DATE,
        "shipping_address": fake.address().replace("\n", ", "),
    }


def gen_crm_record() -> dict[str, Any]:
    """Generate CRM record for batch JSON."""
    return {
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.email(),
        "phone": fake.phone_number(),
        "segment": random.choice(["premium", "standard", "basic"]),
        "lifetime_value": round(random.uniform(100, 10000), 2),
        "registered_date": fake.date_between(start_date="-3y", end_date="-30d").isoformat(),
        "last_purchase_date": fake.date_between(start_date="-60d", end_date="today").isoformat(),
        "preferences": {
            "newsletter": random.choice([True, False]),
            "sms_alerts": random.choice([True, False]),
            "preferred_channel": random.choice(["email", "sms", "push"]),
        },
    }


def gen_inventory_record() -> dict[str, Any]:
    """Generate inventory record for batch CSV."""
    return {
        "sku": f"SKU-{random.randint(10000, 99999)}",
        "product_name": fake.catch_phrase(),
        "category": random.choice(["electronics", "clothing", "food", "furniture", "toys"]),
        "warehouse": random.choice(["WH-NORTH", "WH-SOUTH", "WH-EAST", "WH-WEST"]),
        "quantity_on_hand": random.randint(0, 500),
        "reorder_point": random.randint(10, 50),
        "unit_cost": round(random.uniform(5.0, 100.0), 2),
        "last_restock_date": fake.date_between(start_date="-30d", end_date="today").isoformat(),
        "snapshot_date": PROCESSING_DATE,
    }


def gen_payment_record() -> dict[str, Any]:
    """Generate payment record for batch JSON."""
    return {
        "payment_id": f"PAY-{random.randint(100000, 999999)}",
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "amount": round(random.uniform(20.0, 1000.0), 2),
        "currency": random.choice(["USD", "EUR", "VND"]),
        "payment_method": random.choice(["credit_card", "debit_card", "paypal", "bank_transfer", "cod"]),
        "payment_status": random.choice(["success", "pending", "failed", "refunded"]),
        "gateway": random.choice(["stripe", "paypal", "vnpay", "momo"]),
        "transaction_fee": round(random.uniform(0.5, 5.0), 2),
        "payment_date": PROCESSING_DATE,
        "metadata": {
            "ip": fake.ipv4(),
            "device": random.choice(["web", "ios", "android"]),
        },
    }


# Mapping generators
KAFKA_GENERATORS = {
    "events.user": gen_user_event,
    "events.txn": gen_transaction,
    "events.iot": gen_iot_sensor,
    "logs.app": gen_app_log,
    "clicks.web": gen_web_click,
}

BATCH_GENERATORS = {
    "orders": gen_order,
    "crm": gen_crm_record,
    "inventory": gen_inventory_record,
    "payments": gen_payment_record,
}


# -----------------------------------------------------------------------------
# Bronze Record Wrapper
# -----------------------------------------------------------------------------


def wrap_bronze_kafka(
    record: dict[str, Any],
    source: str,
    topic: str,
    partition: int,
    offset: int,
    flink_job: str,
) -> dict[str, Any]:
    """Wrap a record with Bronze metadata for Kafka sources."""
    return {
        **record,
        "_source": source,
        "_kafka_topic": topic,
        "_kafka_partition": partition,
        "_kafka_offset": offset,
        "_ingested_at": datetime.utcnow().isoformat() + "Z",
        "_flink_job": flink_job,
        "_raw": json.dumps(record),
    }


def wrap_bronze_batch(
    record: dict[str, Any],
    source: str,
    file_path: str,
    flink_job: str,
) -> dict[str, Any]:
    """Wrap a record with Bronze metadata for batch sources."""
    return {
        **record,
        "_source": source,
        "_file_path": file_path,
        "_ingested_at": datetime.utcnow().isoformat() + "Z",
        "_flink_job": flink_job,
        "_raw": json.dumps(record),
    }


# -----------------------------------------------------------------------------
# Kafka Producer
# -----------------------------------------------------------------------------


def produce_to_kafka() -> None:
    """Produce mock messages to all Kafka topics."""
    logger.info("=" * 60)
    logger.info("STEP 1: Producing to Kafka topics")
    logger.info("=" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
        )
    except KafkaError as e:
        logger.error(f"Failed to connect to Kafka at {KAFKA_BOOTSTRAP}: {e}")
        logger.warning("Skipping Kafka production. Make sure Kafka is running.")
        for topic in KAFKA_COUNTS:
            summary["kafka"][topic] = 0
        return

    for topic, count in KAFKA_COUNTS.items():
        logger.info(f"  Producing {count} records to topic: {topic}")
        generator = KAFKA_GENERATORS[topic]

        success_count = 0
        for _ in range(count):
            record = generator()
            try:
                future = producer.send(topic, value=record)
                future.get(timeout=10)
                success_count += 1
            except KafkaError as e:
                logger.warning(f"    Failed to send record to {topic}: {e}")

        summary["kafka"][topic] = success_count
        logger.info(f"    -> Sent {success_count}/{count} records")

    producer.flush()
    producer.close()
    logger.info("Kafka production completed.\n")


# -----------------------------------------------------------------------------
# Batch File Writers
# -----------------------------------------------------------------------------


def write_batch_files() -> None:
    """Write batch files to ingest/raw/."""
    logger.info("=" * 60)
    logger.info("STEP 2: Writing batch files to ingest/raw/")
    logger.info("=" * 60)

    # Orders CSV
    orders_dir = INGEST_RAW / "orders"
    orders_dir.mkdir(parents=True, exist_ok=True)
    orders_file = orders_dir / f"orders_{PROCESSING_DATE_COMPACT}.csv"

    orders_data = []
    for _ in range(BATCH_COUNTS["orders"]):
        order = gen_order()
        order["total_amount"] = round(order["quantity"] * order["unit_price"] * (1 - order["discount_pct"] / 100), 2)
        orders_data.append(order)

    pd.DataFrame(orders_data).to_csv(orders_file, index=False)
    summary["batch"]["orders"] = len(orders_data)
    logger.info(f"  Written {len(orders_data)} orders to {orders_file}")

    # CRM JSON
    crm_dir = INGEST_RAW / "crm"
    crm_dir.mkdir(parents=True, exist_ok=True)
    crm_file = crm_dir / f"crm_{PROCESSING_DATE_COMPACT}.json"

    crm_data = [gen_crm_record() for _ in range(BATCH_COUNTS["crm"])]
    with open(crm_file, "w") as f:
        json.dump(crm_data, f, indent=2)
    summary["batch"]["crm"] = len(crm_data)
    logger.info(f"  Written {len(crm_data)} CRM records to {crm_file}")

    # Inventory CSV
    inventory_dir = INGEST_RAW / "inventory"
    inventory_dir.mkdir(parents=True, exist_ok=True)
    inventory_file = inventory_dir / f"inventory_{PROCESSING_DATE_COMPACT}.csv"

    inventory_data = [gen_inventory_record() for _ in range(BATCH_COUNTS["inventory"])]
    pd.DataFrame(inventory_data).to_csv(inventory_file, index=False)
    summary["batch"]["inventory"] = len(inventory_data)
    logger.info(f"  Written {len(inventory_data)} inventory records to {inventory_file}")

    # Payments JSON
    payments_dir = INGEST_RAW / "payments"
    payments_dir.mkdir(parents=True, exist_ok=True)
    payments_file = payments_dir / f"payments_{PROCESSING_DATE_COMPACT}.json"

    payments_data = [gen_payment_record() for _ in range(BATCH_COUNTS["payments"])]
    with open(payments_file, "w") as f:
        json.dump(payments_data, f, indent=2)
    summary["batch"]["payments"] = len(payments_data)
    logger.info(f"  Written {len(payments_data)} payment records to {payments_file}")

    logger.info("Batch file writing completed.\n")


# -----------------------------------------------------------------------------
# Bronze Layer Writers
# -----------------------------------------------------------------------------


def write_bronze_layer() -> None:
    """Write Bronze NDJSON files for all 10 sources."""
    logger.info("=" * 60)
    logger.info("STEP 3: Writing Bronze NDJSON to datalake/bronze/")
    logger.info("=" * 60)

    # Generate random hours for partitioning
    hours = [f"{h:02d}" for h in range(24)]

    # Write Bronze for Kafka sources (5 sources)
    for topic, count in KAFKA_COUNTS.items():
        source = KAFKA_TO_SOURCE[topic]
        flink_job = f"StreamIngestJob_{source}"

        # Distribute records across multiple hours
        records_per_hour: dict[str, list[dict]] = {h: [] for h in hours}

        generator = KAFKA_GENERATORS[topic]
        for i in range(count):
            record = generator()
            hour = random.choice(hours)
            partition = random.randint(0, 2)
            offset = i * 10 + random.randint(0, 9)

            bronze_record = wrap_bronze_kafka(
                record=record,
                source=source,
                topic=topic,
                partition=partition,
                offset=offset,
                flink_job=flink_job,
            )
            records_per_hour[hour].append(bronze_record)

        # Write to Bronze partitions
        total_written = 0
        for hour, records in records_per_hour.items():
            if not records:
                continue

            bronze_path = DATALAKE_BRONZE / f"source={source}" / f"dt={PROCESSING_DATE}" / f"hour={hour}"
            bronze_path.mkdir(parents=True, exist_ok=True)
            bronze_file = bronze_path / "part-00000.ndjson"

            with open(bronze_file, "w") as f:
                for rec in records:
                    f.write(json.dumps(rec) + "\n")
            total_written += len(records)

        summary["bronze"][source] = total_written
        logger.info(f"  Written {total_written} Bronze records for source={source}")

    # Write Bronze for Batch sources (4 sources)
    batch_sources = [
        ("orders", f"orders_{PROCESSING_DATE_COMPACT}.csv", gen_order),
        ("crm", f"crm_{PROCESSING_DATE_COMPACT}.json", gen_crm_record),
        ("inventory", f"inventory_{PROCESSING_DATE_COMPACT}.csv", gen_inventory_record),
        ("payments", f"payments_{PROCESSING_DATE_COMPACT}.json", gen_payment_record),
    ]

    for batch_name, file_name, generator in batch_sources:
        source = BATCH_TO_SOURCE[batch_name]
        flink_job = f"BatchIngestJob_{source}"
        file_path = str(INGEST_RAW / batch_name / file_name)
        count = BATCH_COUNTS[batch_name]

        # Distribute across hours
        records_per_hour: dict[str, list[dict]] = {h: [] for h in hours}

        for _ in range(count):
            record = generator()
            if batch_name == "orders":
                record["total_amount"] = round(
                    record["quantity"] * record["unit_price"] * (1 - record["discount_pct"] / 100), 2
                )
            hour = random.choice(hours)

            bronze_record = wrap_bronze_batch(
                record=record,
                source=source,
                file_path=file_path,
                flink_job=flink_job,
            )
            records_per_hour[hour].append(bronze_record)

        # Write to Bronze partitions
        total_written = 0
        for hour, records in records_per_hour.items():
            if not records:
                continue

            bronze_path = DATALAKE_BRONZE / f"source={source}" / f"dt={PROCESSING_DATE}" / f"hour={hour}"
            bronze_path.mkdir(parents=True, exist_ok=True)
            bronze_file = bronze_path / "part-00000.ndjson"

            with open(bronze_file, "w") as f:
                for rec in records:
                    f.write(json.dumps(rec) + "\n")
            total_written += len(records)

        summary["bronze"][source] = total_written
        logger.info(f"  Written {total_written} Bronze records for source={source}")

    # Write Bronze for alerts source (simulated, no Kafka topic yet)
    source = "alerts"
    flink_job = "StreamIngestJob_alerts"
    alerts_count = 30  # Smaller count for alerts

    records_per_hour: dict[str, list[dict]] = {h: [] for h in hours}

    for i in range(alerts_count):
        alert = {
            "alert_id": str(uuid.uuid4()),
            "severity": random.choice(["low", "medium", "high", "critical"]),
            "alert_type": random.choice(["threshold_breach", "anomaly_detected", "system_down", "security_event"]),
            "source_system": random.choice(["monitoring", "security", "iot", "application"]),
            "message": fake.sentence(),
            "timestamp": fake.date_time_between(
                start_date=PROCESSING_DATE_START,
                end_date=PROCESSING_DATE_END,
            ).isoformat() + "Z",
        }
        hour = random.choice(hours)

        bronze_record = wrap_bronze_kafka(
            record=alert,
            source=source,
            topic="alerts",
            partition=random.randint(0, 1),
            offset=i * 10,
            flink_job=flink_job,
        )
        records_per_hour[hour].append(bronze_record)

    total_written = 0
    for hour, records in records_per_hour.items():
        if not records:
            continue

        bronze_path = DATALAKE_BRONZE / f"source={source}" / f"dt={PROCESSING_DATE}" / f"hour={hour}"
        bronze_path.mkdir(parents=True, exist_ok=True)
        bronze_file = bronze_path / "part-00000.ndjson"

        with open(bronze_file, "w") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")
        total_written += len(records)

    summary["bronze"][source] = total_written
    logger.info(f"  Written {total_written} Bronze records for source={source}")

    logger.info("Bronze layer writing completed.\n")


# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------


def print_summary() -> None:
    """Print summary table of all generated data."""
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)

    print("\n" + "=" * 70)
    print(f"{'MOCK DATA GENERATION SUMMARY':^70}")
    print(f"{'Processing Date: ' + PROCESSING_DATE:^70}")
    print("=" * 70)

    # Kafka Summary
    print("\n[KAFKA TOPICS]")
    print("-" * 50)
    print(f"{'Topic':<25} {'Records':>10}")
    print("-" * 50)
    kafka_total = 0
    for topic, count in summary["kafka"].items():
        print(f"{topic:<25} {count:>10}")
        kafka_total += count
    print("-" * 50)
    print(f"{'TOTAL':<25} {kafka_total:>10}")

    # Batch Summary
    print("\n[BATCH FILES - ingest/raw/]")
    print("-" * 50)
    print(f"{'Source':<25} {'Records':>10}")
    print("-" * 50)
    batch_total = 0
    for source, count in summary["batch"].items():
        print(f"{source:<25} {count:>10}")
        batch_total += count
    print("-" * 50)
    print(f"{'TOTAL':<25} {batch_total:>10}")

    # Bronze Summary
    print("\n[BRONZE LAYER - datalake/bronze/]")
    print("-" * 50)
    print(f"{'Source':<25} {'Records':>10}")
    print("-" * 50)
    bronze_total = 0
    for source in sorted(summary["bronze"].keys()):
        count = summary["bronze"][source]
        print(f"{source:<25} {count:>10}")
        bronze_total += count
    print("-" * 50)
    print(f"{'TOTAL':<25} {bronze_total:>10}")

    # Grand Total
    print("\n" + "=" * 70)
    grand_total = kafka_total + batch_total + bronze_total
    print(f"{'GRAND TOTAL RECORDS':<25} {grand_total:>10}")
    print("=" * 70 + "\n")


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------


def main() -> None:
    """Main entry point."""
    logger.info("=" * 60)
    logger.info("MOCK DATA GENERATOR - tandat-interview ETL Platform")
    logger.info(f"Processing Date: {PROCESSING_DATE}")
    logger.info(f"Project Root: {PROJECT_ROOT}")
    logger.info("=" * 60 + "\n")

    produce_to_kafka()
    write_batch_files()
    write_bronze_layer()
    print_summary()

    logger.info("Mock data generation completed successfully!")


if __name__ == "__main__":
    main()
