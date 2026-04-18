# 🏗️ Medallion Data Platform – End-to-End Architecture

## 📐 Kiến trúc logic (Medallion)

### 🥉 Bronze (Raw Layer)

* Ingest dữ liệu thô từ **10+ nguồn (Streaming & Batch)**
* Format: `ndjson`
* Giữ nguyên **schema gốc**
* Bổ sung metadata: `ingestion_ts`
* Mục tiêu: đảm bảo **data lineage + traceability**

---

### 🥈 Silver (Validated Layer)

* Xử lý bằng Spark:

  * Data cleaning
  * Deduplication
  * Schema enforcement (cast kiểu dữ liệu)
* Format lưu trữ: `Parquet`
* Tối ưu:

  * Columnar storage
  * Query performance

---

### 🥇 Gold (Analytics Layer)

* Precompute các bảng:

  * `Daily Revenue`
  * `User Retention`
* Use-case:

  * Dashboard BI
  * ML Training datasets
* Đặc điểm:

  * Denormalized
  * Query-ready

---

## 🛠️ Stack công nghệ

### 📥 Ingestion

* Kafka → Messaging backbone
* Flink → Real-time stream processing
* Spark → Batch processing

### 💾 Storage

* Medallion Data Lake:

  * Bronze: NDJSON
  * Silver/Gold: Parquet
* Delta Lake (Databricks / Cloud)

### 🔐 Security

* Kerberos KDC (Authentication)
* SASL/GSSAPI (Kafka security)

### 🧱 Infrastructure

* NixOS + Nix Flakes → reproducible environment
* WSL2 + Docker → local dev
* Terraform → infrastructure as code

### 🤖 MLOps

* Feast → Feature Store
* MLflow → experiment tracking
* KServe → model serving

---

## 📋 Checklist triển khai (Roadmap to Production)

### 1. 🧱 Infrastructure & Security

* [x] Setup môi trường với Nix Flakes
* [x] Cấu hình Kerberos KDC (Realm: `ETL.LOCAL`)
* [x] Tạo keytabs cho Kafka / Zookeeper / Flink
* [x] Setup Zookeeper High Availability (Leader Election)

---

### 2. 🚰 Ingestion & Storage

* [x] Tạo Kafka topics cho 10 nguồn dữ liệu
* [x] Triển khai Flink sink → `/datalake/bronze/`

  * Có event-time watermark
  * **JobID: ef8371b8620e2b7c52548d08e8a414e0**
  * 5 Kafka topics streaming: `events.user`, `events.txn`, `events.iot`, `logs.app`, `clicks.web`
  * Checkpoint dir: `/datalake/checkpoints/flink/bronze_ingest`
  * Output: `/datalake/bronze/` (partitioned by source/dt/hour)
* [ ] Setup Dead Letter Queue (DLQ) cho JSON lỗi
* [ ] Spark job:

  * Transform Bronze → Silver
  * Partition overwrite strategy

---

### 3. 🧠 Logic xử lý & ML

* [ ] Feature Engineering (Spark SQL trên Silver)
* [ ] Tích hợp Feast (Online + Offline features)
* [ ] Train model Fraud Detection:

  * XGBoost / LightGBM
* [ ] Log experiment vào MLflow

---

### 4. 📊 Operations & Monitoring

* [ ] Checkpointing:

  * Path: `/datalake/checkpoints/`
  * Đảm bảo fault tolerance
* [ ] Monitoring:

  * Grafana + Prometheus
  * Kafka throughput
  * Flink backpressure
* [ ] Orchestration:

  * Airflow hoặc ArgoCD

---

## 💡 Trade-off Notes (Design Decisions)

* **Flink (Bronze Layer)**:

  * Ưu tiên xử lý **late data**
  * Latency thấp (<1s)
  * Strong event-time semantics

* **Spark (Silver/Gold Layer)**:

  * Tối ưu hóa **shuffle (AQE)**
  * Xử lý tốt:

    * Large joins
    * Batch transformations
  * Phù hợp cho analytical workloads

---

## 🚀 Tổng kết

Kiến trúc này tách biệt rõ:

* **Data ingestion (Flink)**
* **Data processing (Spark)**
* **Data serving (Gold + ML)**

👉 Giúp hệ thống:

* Scalable
* Fault-tolerant
* Production-ready
* Phù hợp cả BI + ML workloads

---
