# Medallion Data Platform – Local ETL Lab

> **Production-ready** data platform chạy hoàn toàn **local trên NixOS** (WSL2)
> Không dùng Docker, không dùng Cloud – Pure Nix Flakes environment

---

## [STATUS] Current Status (2026-04-19)

### [OK] Phase 1: Bronze Layer (COMPLETED)
- **Flink 1.19.1** streaming ingest từ **5 Kafka topics** → Bronze NDJSON
- **~1.2GB** data ingested (date=2026-04-17)
- **10 Bronze sources**: 5 streaming + 5 batch
- Partition scheme: `source=<name>/dt=YYYY-MM-DD/hour=HH/`
- Dead Letter Queue: configured for corrupted records

### [OK] Phase 2: Silver Layer (COMPLETED)
- **4 Spark domains** transformed từ Bronze → Silver Parquet:
  - `domain=events` (56KB) - user_events + clickstream merged
  - `domain=transactions` (16KB) - transactions + payments merged
  - `domain=customers` (8.5KB) - CRM + orders aggregated
  - `domain=inventory` (7.6KB) - inventory + stock status calculated
- **Deduplication** by primary key (Window function)
- **Schema enforcement** + null filtering
- **Snappy compression** Parquet

### [OK] Phase 3: Gold Layer (COMPLETED)
- **4 KPI tables** aggregated từ Silver → Gold:
  - `kpi=daily_revenue` (3.1KB) - Revenue + conversion rate
  - `kpi=user_retention` (2.2KB) - Cohort retention analysis
  - `kpi=inventory_turnover` (4.0K) - Stock turnover by category
  - `kpi=iot_anomaly_summary` (0 records) - Placeholder for 3σ anomaly detection
- Total Gold: **~10KB**

### [TODO] Phase 4: Database Layer
- [ ] Setup PostgreSQL analytics database
- [ ] Load Gold KPIs → PostgreSQL tables
- [ ] Create views/materialized views
- [ ] Setup daily orchestration (Airflow/cron)

---

## Tech Stack

### Streaming & Batch
- **Apache Kafka 3.4.0** - 2 brokers, 5 topics streaming
- **Apache Flink 1.19.1** - Stream processing (Bronze ingest)
- **Apache Spark 3.5.5** - Batch transformations (Silver/Gold)

### Storage
- **Medallion Data Lake**:
  - Bronze: NDJSON (raw, append-only)
  - Silver: Parquet + Snappy (validated, deduped)
  - Gold: Parquet + Snappy (aggregated KPIs)

### Infrastructure
- **ZooKeeper** - Coordination (Kafka + Flink HA)
- **Kerberos KDC** - Authentication (realm: ETL.LOCAL)
- **PostgreSQL 15** - Analytics database
- **NixOS + Nix Flakes** - Reproducible dev environment

### Orchestration
- **just** (justfile) - Task runner cho service management
- `just start-all` - Start toàn bộ stack (KDC → ZK → Kafka → Flink)
- `just run-all-silver 2026-04-17` - Chạy 4 Silver transforms
- `just run-all-gold 2026-04-17` - Chạy 4 Gold KPIs

---

## Project Structure

```
tandat-interview/
├── flake.nix                    # Nix dependencies (pinned với flake.lock)
├── .envrc                       # direnv auto-activation
├── ETL/
│   ├── justfile                 # Tất cả commands (start/stop/run jobs)
│   ├── infrastructure/          # Service configs + data + logs
│   │   ├── .config/             # Kafka broker properties
│   │   ├── kafka-data/          # broker-1/, broker-2/, *.log
│   │   ├── flink-data/          # conf/, logs/, ha/
│   │   ├── spark-data/          # master/worker logs + work dir
│   │   ├── zookeeper-data/      # conf/, data/, logs/
│   │   ├── kerberos-data/       # conf/, data/, logs/
│   │   └── postgres-data/       # PostgreSQL cluster
│   └── tutorial/                # Markdown guides (kafka.md, flink.md, etc.)
├── scripts/                     # Flink + Spark Python jobs
│   ├── flink_bronze_ingest.py   # Streaming: Kafka → Bronze
│   ├── spark_bronze_to_silver_*.py  # 4 Silver transform jobs
│   └── spark_silver_to_gold_*.py    # 4 Gold KPI jobs
├── datalake/                    # Medallion Data Lake
│   ├── bronze/                  # NDJSON (10 sources)
│   ├── silver/                  # Parquet (4 domains)
│   ├── gold/                    # Parquet (4 KPIs)
│   └── checkpoints/             # Flink state
└── ingest/raw/                  # Batch drop zone (CSV/JSON)
```

---

## Quick Start

### 1. Enter Nix shell
```bash
cd ~/tandat-interview
nix develop  # or direnv will auto-load
```

### 2. Start infrastructure
```bash
cd ETL
just start-all      # Start KDC → ZK → Kafka → Flink
just status         # Verify all services running
```

### 3. Verify data flow
```bash
# Check Bronze (Flink streaming)
find ../datalake/bronze -name "*.ndjson" -mmin -60 | head -10

# Run Silver transforms
just run-all-silver 2026-04-17

# Run Gold KPIs
just run-all-gold 2026-04-17

# Verify Gold output
ls -lh ../datalake/gold/kpi=*/dt=2026-04-17/*.parquet
```

---

## Data Flow

```
[Streaming Sources]
  Web/POS/IoT/nginx → Kafka (localhost:9092)
       ↓ Flink StreamIngestJob (parallelism=1)
       ↓ FileSink + Watermark (30s out-of-orderness)
  Bronze NDJSON (~1.2GB)
    source=user_events/    353KB
    source=transactions/   148KB
    source=iot_sensors/    192KB
    source=app_logs/       143KB
    source=clickstream/    365KB

[Batch Sources]
  /ingest/raw/{orders,crm,inventory,payments}/*.csv
       ↓ Flink FileSource (PROCESS_ONCE)
  Bronze NDJSON
    source=orders/
    source=crm/
    source=inventory/
    source=payments/
    source=alerts/

─────────── Daily 01:00 ───────────

  Spark Bronze → Silver (4 domains)
    domain=events (56KB)        - user_events + clickstream
    domain=transactions (16KB)  - transactions + payments
    domain=customers (8.5KB)    - crm + orders aggregated
    domain=inventory (7.6KB)    - inventory + stock status

─────────── Daily 02:00 ───────────

  Spark Silver → Gold (4 KPIs)
    kpi=daily_revenue (3.1KB)           - Revenue + conversion
    kpi=user_retention (2.2KB)          - Cohort analysis
    kpi=inventory_turnover (4.0KB)      - Stock turnover
    kpi=iot_anomaly_summary (0 records) - 3σ anomaly detection
```

---

## Về Data Size - Tại sao chỉ có ~1.2GB?

Đây là **lab environment với sample data**, không phải production scale:

### Current State (Lab)
- **Bronze**: ~1.2GB (1 ngày streaming)
- **Silver**: ~88KB (4 domains)
- **Gold**: ~10KB (4 KPIs)

### Production Scale (Estimate)
Nếu chạy production với:
- **1M events/day** → Bronze ~50-100GB/day
- **30 days retention** → Bronze ~1.5-3TB
- **Silver** (deduped + compressed) → ~500GB-1TB
- **Gold** (aggregated) → ~10-50GB

### Lý do data nhỏ:
1. **Sample data** - Chỉ test với vài trăm records mỗi source
2. **Single day** - Chỉ có data cho 2026-04-17
3. **Local testing** - Không cần petabyte data để validate kiến trúc
4. **Parquet compression** - Snappy giảm size xuống ~10-20% so với NDJSON

### Để tăng data size:
```bash
# Produce more Kafka messages
just test-all-topics  # Send batch messages to all 5 topics

# Wait 15 minutes for Flink checkpoint
just verify-bronze    # Check .inprogress files

# Or: Load larger CSV batches to /ingest/raw/
```

---

## Design Decisions

### Tại sao dùng Flink cho Bronze?
- **Event-time processing** - Xử lý late data tốt hơn Spark Streaming
- **Low latency** - <1s end-to-end (Kafka → Parquet)
- **Exactly-once** - Checkpoint-based fault tolerance
- **Watermark** - Handle out-of-order events (30s bounded)

### Tại sao dùng Spark cho Silver/Gold?
- **Batch efficiency** - Tối ưu cho large joins + aggregations
- **AQE** (Adaptive Query Execution) - Auto-optimize shuffle
- **Parquet columnar** - Query performance tốt hơn NDJSON
- **DataFrame API** - Easier dedup, window functions, complex logic

### Tại sao không dùng Docker?
- **Nix Flakes** - Pin toàn bộ dependencies (JDK, Kafka, Flink, Spark, PostgreSQL)
- **Reproducible** - `flake.lock` = deterministic builds
- **Lighter** - Không overhead của container layers
- **Dev speed** - `direnv` auto-load shell khi `cd`

---

## Next Steps (Phase 4)

1. **Setup PostgreSQL**
   ```bash
   just start-postgres
   just create-databases  # airflow_metadata, etl_analytics
   ```

2. **Load Gold → PostgreSQL**
   - Create table schemas matching Gold Parquet
   - Spark JDBC write: `df.write.jdbc(...)`

3. **Orchestration**
   - Airflow DAGs for daily Silver/Gold jobs
   - Or: cron jobs with `just run-all-silver`

4. **Monitoring**
   - Grafana + Prometheus for metrics
   - Track: Kafka lag, Flink backpressure, Spark job duration

---

## Security

- **Kerberos** - realm `ETL.LOCAL` :8888
- **SASL/GSSAPI** - Kafka authentication
- **JAAS configs** - ZooKeeper + Flink principals
- **Keytabs** - Not committed (in `.gitignore`)

---

## Documentation

- `ETL/tutorial/kafka.md` - Kafka setup + topic management
- `ETL/tutorial/flink.md` - Flink job submission + checkpoint tuning
- `ETL/tutorial/spark.md` - PySpark patterns + Kafka JARs config
- `ETL/tutorial/krb5.md` - Kerberos principal setup
- `README-flink.md` - Flink-specific details

---

## Performance Notes

### Flink Bronze Ingest
- **Parallelism**: 1 (single task slot)
- **Checkpoint interval**: 60s
- **Rolling policy**: 10 min or 128MB
- **Throughput**: ~1K events/sec (local WSL2)

### Spark Silver/Gold
- **Shuffle partitions**: 8 (small cluster)
- **Executor memory**: 1GB
- **Compression**: Snappy (good balance speed/size)
- **Job duration**: ~15-30s per domain/KPI

---

## Contributing

Đây là personal learning project. Nếu muốn replicate:

1. Clone repo
2. `nix develop` (requires Nix + flakes enabled)
3. `cd ETL && just start-all`
4. Follow Quick Start guide

---

## License

MIT License - Free to use for learning/research

---

**Built using pure Nix + Apache stack**
