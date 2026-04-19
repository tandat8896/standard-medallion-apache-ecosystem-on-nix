#!/usr/bin/env python3
"""
flink_bronze_ingest.py - Flink streaming job to ingest from Kafka to Bronze layer."""

# IMPORTANT: Import compatibility patches BEFORE pyflink
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))
import pyflink_beam_compat  # noqa: F401

"""

Consumes 5 Kafka topics and writes NDJSON to datalake/bronze/ with partition scheme:
    source=<name>/dt=YYYY-MM-DD/hour=HH/part-NNNNN.ndjson

Usage:
    flink run -py scripts/flink_bronze_ingest.py
    flink run -p 2 -py scripts/flink_bronze_ingest.py  # with parallelism

Topics consumed:
    - events.user  -> source=user_events
    - events.txn   -> source=transactions
    - events.iot   -> source=iot_sensors
    - logs.app     -> source=app_logs
    - clicks.web   -> source=clickstream
"""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

from pyflink.common import Duration, Types, WatermarkStrategy, Configuration
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.datastream import RuntimeExecutionMode, StreamExecutionEnvironment
from pyflink.datastream.checkpoint_config import CheckpointingMode
from pyflink.datastream.connectors.file_system import (
    FileSink,
    OutputFileConfig,
    RollingPolicy,
)
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
)
logger = logging.getLogger(__name__)

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATALAKE_BRONZE = PROJECT_ROOT / "datalake" / "bronze"
DATALAKE_DEAD_LETTER = PROJECT_ROOT / "datalake" / "dead_letter"
CHECKPOINT_DIR = PROJECT_ROOT / "datalake" / "checkpoints" / "flink" / "bronze_ingest"

# Kafka config
KAFKA_BOOTSTRAP = "localhost:9092"
CONSUMER_GROUP = "flink-bronze-writer"

# Topic to source mapping
TOPIC_TO_SOURCE: dict[str, str] = {
    "events.user": "user_events",
    "events.txn": "transactions",
    "events.iot": "iot_sensors",
    "logs.app": "app_logs",
    "clicks.web": "clickstream",
}

FLINK_JOB_NAME = "StreamIngestJob_bronze"


# -----------------------------------------------------------------------------
# Bronze Record Transformer
# -----------------------------------------------------------------------------


class BronzeRecordTransformer:
    """Transform raw Kafka messages into Bronze records with metadata."""

    def __init__(self, source: str, topic: str, flink_job: str) -> None:
        self.source = source
        self.topic = topic
        self.flink_job = flink_job

    def transform(
        self, raw_value: str, partition: int, offset: int
    ) -> dict[str, Any] | None:
        """Parse and enrich raw message with Bronze metadata."""
        try:
            payload = json.loads(raw_value)
        except json.JSONDecodeError:
            return None

        return {
            **payload,
            "_source": self.source,
            "_kafka_topic": self.topic,
            "_kafka_partition": partition,
            "_kafka_offset": offset,
            "_ingested_at": datetime.utcnow().isoformat() + "Z",
            "_flink_job": self.flink_job,
            "_raw": raw_value,
        }


# -----------------------------------------------------------------------------
# Custom Bucket Assigner (simplified for PyFlink)
# -----------------------------------------------------------------------------


def get_bucket_path(record: dict[str, Any]) -> str:
    """Generate bucket path from record timestamp."""
    ts_str = record.get("timestamp") or record.get("_ingested_at", "")

    try:
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1]
        dt = datetime.fromisoformat(ts_str)
    except (ValueError, TypeError):
        dt = datetime.utcnow()

    source = record.get("_source", "unknown")
    return f"source={source}/dt={dt.strftime('%Y-%m-%d')}/hour={dt.strftime('%H')}"


# -----------------------------------------------------------------------------
# Process Function
# -----------------------------------------------------------------------------


def process_kafka_record(value: str, topic: str) -> str | None:
    """Process a single Kafka record and return NDJSON string."""
    source = TOPIC_TO_SOURCE.get(topic, "unknown")
    transformer = BronzeRecordTransformer(source, topic, FLINK_JOB_NAME)

    # Note: In real Flink, we'd get partition/offset from context
    # For simplicity, using placeholder values
    bronze_record = transformer.transform(value, partition=0, offset=0)

    if bronze_record is None:
        return None

    return json.dumps(bronze_record)


# -----------------------------------------------------------------------------
# Main Job
# -----------------------------------------------------------------------------


def create_kafka_source(env: StreamExecutionEnvironment, topic: str) -> KafkaSource:
    """Create a Kafka source for a specific topic."""
    return (
        KafkaSource.builder()
        .set_bootstrap_servers(KAFKA_BOOTSTRAP)
        .set_topics(topic)
        .set_group_id(CONSUMER_GROUP)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


def create_file_sink(source_name: str) -> FileSink:
    """Create a file sink for Bronze layer."""
    output_path = str(DATALAKE_BRONZE / f"source={source_name}")

    return (
        FileSink.for_row_format(
            output_path,
            Encoder.simple_string_encoder("UTF-8"),
        )
        .with_output_file_config(
            OutputFileConfig.builder()
            .with_part_prefix("part")
            .with_part_suffix(".ndjson")
            .build()
        )
        .with_rolling_policy(
            RollingPolicy.default_rolling_policy(
                part_size=128 * 1024 * 1024,  # 128MB
                rollover_interval=600000,  # 10 minutes in milliseconds
                inactivity_interval=300000,  # 5 minutes in milliseconds
            )
        )
        .build()
    )


def run_bronze_ingest() -> None:
    """Main entry point for the Flink Bronze ingest job."""
    logger.info("=" * 60)
    logger.info("Starting Flink Bronze Ingest Job")
    logger.info(f"Checkpoint dir: {CHECKPOINT_DIR}")
    logger.info(f"Output dir: {DATALAKE_BRONZE}")
    logger.info("=" * 60)

    # Create directories
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    DATALAKE_BRONZE.mkdir(parents=True, exist_ok=True)
    DATALAKE_DEAD_LETTER.mkdir(parents=True, exist_ok=True)

    # 1. Khởi tạo Configuration để "ép" nạp JAR ngay từ đầu (Fix lỗi Java 17)
    config = Configuration()
    # kafka_jar = "file:///home/tandat8896-nix/tandat-interview/ETL/infrastructure/flink-data/lib/kafka-clients-3.4.0.jar"
    
    # logger.info(f"Configuring pipeline with JAR: {kafka_jar}")
    
    config.set_string("classloader.resolve-order", "parent-last")
    # config.set_string("pipeline.jars", kafka_jar)
    # config.set_string("pipeline.classpaths", kafka_jar)

    # 2. Tạo Execution Environment với Configuration đã nạp JAR
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.set_parallelism(1)  # Changed from 2 to 1 to match available task slots

    # 3. Cấu hình Checkpointing - EXACTLY_ONCE semantics
    env.enable_checkpointing(60_000)  # 60 seconds
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config.set_min_pause_between_checkpoints(30_000)
    checkpoint_config.set_checkpoint_timeout(120_000)
    checkpoint_config.set_max_concurrent_checkpoints(1)
    # checkpoint_config.set_checkpoint_storage_dir(f"file://{CHECKPOINT_DIR}")

    # 4. Watermark strategy - bounded out-of-orderness 30 seconds
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(30)
    ).with_idleness(Duration.of_minutes(1))

    # 5. Xử lý từng Topic
    for topic, source_name in TOPIC_TO_SOURCE.items():
        logger.info(f"Setting up pipeline for topic: {topic} -> source={source_name}")

        # Create source (Lệnh này giờ sẽ thấy class Kafka nhờ nạp JAR qua config ở trên)
        kafka_source = create_kafka_source(env, topic)

        # Create stream
        stream = env.from_source(
            kafka_source,
            watermark_strategy,
            f"kafka-source-{topic}",
        )

        # Transform to Bronze format
        def make_processor(t: str):
            def processor(value: str) -> str:
                source = TOPIC_TO_SOURCE.get(t, "unknown")
                try:
                    payload = json.loads(value)
                    bronze_record = {
                        **payload,
                        "_source": source,
                        "_kafka_topic": t,
                        "_kafka_partition": 0,
                        "_kafka_offset": 0,
                        "_ingested_at": datetime.utcnow().isoformat() + "Z",
                        "_flink_job": FLINK_JOB_NAME,
                        "_raw": value,
                    }
                    return json.dumps(bronze_record)
                except json.JSONDecodeError:
                    return ""
            return processor

        bronze_stream = stream.map(
            make_processor(topic),
            output_type=Types.STRING(),
        ).filter(lambda x: x != "")

        # Create sink
        file_sink = create_file_sink(source_name)
        bronze_stream.sink_to(file_sink).name(f"bronze-sink-{source_name}")

    logger.info("Submitting job to Flink cluster...")
    env.execute(FLINK_JOB_NAME)
# -----------------------------------------------------------------------------
# Alternative: Simple Batch Mode for Testing
# -----------------------------------------------------------------------------


def run_batch_test() -> None:
    """Run in batch mode for testing without Kafka."""
    logger.info("Running in BATCH mode for testing...")

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    env.set_parallelism(1)

    # Create sample data
    sample_records = [
        {
            "event_id": "1",
            "user_id": "u1",
            "event_type": "click",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        },
        {
            "event_id": "2",
            "user_id": "u2",
            "event_type": "view",
            "timestamp": datetime.utcnow().isoformat() + "Z",
        },
    ]

    # Create stream from collection
    stream = env.from_collection(
        [json.dumps(r) for r in sample_records],
        type_info=Types.STRING(),
    )

    # Transform
    def add_bronze_meta(value: str) -> str:
        try:
            payload = json.loads(value)
            bronze_record = {
                **payload,
                "_source": "test",
                "_kafka_topic": "test.topic",
                "_kafka_partition": 0,
                "_kafka_offset": 0,
                "_ingested_at": datetime.utcnow().isoformat() + "Z",
                "_flink_job": "TestJob",
                "_raw": value,
            }
            return json.dumps(bronze_record)
        except json.JSONDecodeError:
            return ""

    bronze_stream = stream.map(add_bronze_meta, output_type=Types.STRING())

    # Print to stdout for testing
    bronze_stream.print()

    env.execute("TestBronzeIngest")


# -----------------------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------------------


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        run_batch_test()
    else:
        run_bronze_ingest()
