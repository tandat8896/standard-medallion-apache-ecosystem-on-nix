# Prompt dán thẳng vào Claude CLI
# Chạy từ: ~/tandat-interview/

Tôi cần tạo mock data cho toàn bộ hệ thống ETL lab theo đúng cấu trúc datalake đã thiết kế.

## Yêu cầu

Viết một script Python duy nhất `scripts/generate_mock_data.py` chạy từ project root
`~/tandat-interview/`, thực hiện đồng thời 3 việc:

---

### 1. Kafka — produce streaming events

Produce vào đúng 5 topics đang chạy tại `localhost:9092`:

| Topic | Records | Schema gợi ý |
|-------|---------|--------------|
| `events.user` | 200 records | event_id, user_id, event_type (page_view/add_to_cart/checkout), page, event_ts, session_id, ip |
| `events.txn` | 100 records | txn_id, user_id, session_id, amount (10-500), currency=VND, status (paid/pending/failed), txn_ts |
| `events.iot` | 150 records | device_id, sensor_type (temp/pressure/vibration), reading (float), unit, device_ts, location |
| `logs.app` | 100 records | log_id, level (ERROR/WARN/INFO), service, message, log_ts, trace_id |
| `clicks.web` | 200 records | click_id, url, status_code (200/301/404/500), latency_ms, user_agent, click_ts |

Dùng `kafka-python-ng` (đã có trong flake.nix).
Timestamps rải đều trong 24h của ngày hôm qua (để Spark job có thể đọc partition `dt=yesterday`).

---

### 2. Batch files — drop vào /ingest/raw/

Tạo đúng theo naming convention:

| Path | Format | Records |
|------|--------|---------|
| `ingest/raw/orders/orders_YYYYMMDD.csv` | CSV | 80 records: order_id, user_id, session_id, product_id, quantity, unit_price, total, order_ts, status |
| `ingest/raw/crm/crm_YYYYMMDD.json` | JSON (array) | 50 records: customer_id, name, email, phone, segment (VIP/Regular/New), created_at, last_purchase_ts |
| `ingest/raw/inventory/inventory_YYYYMMDD.csv` | CSV | 40 records: product_id, product_name, category, stock_qty, warehouse_id, updated_at |
| `ingest/raw/payments/payments_YYYYMMDD.json` | JSON (array) | 80 records: payment_id, order_id, method (card/cash/wallet), amount, status, payment_ts |

YYYYMMDD = ngày hôm qua.

---

### 3. Bronze layer — ghi trực tiếp NDJSON

Simulate Flink output bằng cách ghi trực tiếp vào Bronze theo đúng partition scheme:

```
datalake/bronze/source=<n>/dt=YYYY-MM-DD/hour=HH/part-00000.ndjson
```

Mỗi record PHẢI có các meta fields:
- `_source`: tên source
- `_kafka_topic`: topic name (streaming) hoặc `null` (batch)
- `_kafka_partition`: int (streaming) hoặc `null` (batch)  
- `_kafka_offset`: int (streaming) hoặc `null` (batch)
- `_file_path`: đường dẫn file (batch) hoặc `null` (streaming)
- `_ingested_at`: timestamp ISO8601 UTC
- `_flink_job`: `"MockGenerator-v1"`
- `_raw`: JSON string của payload gốc

Ghi cho tất cả 10 sources:
- `source=user_events` — từ events.user
- `source=transactions` — từ events.txn  
- `source=app_logs` — từ logs.app
- `source=iot_sensors` — từ events.iot
- `source=clickstream` — từ clicks.web
- `source=orders` — từ ingest/raw/orders/
- `source=crm` — từ ingest/raw/crm/
- `source=inventory` — từ ingest/raw/inventory/
- `source=payments` — từ ingest/raw/payments/
- `source=alerts` — tạo 30 records alert trực tiếp (alert_id, severity, message, alert_ts)

Records phân bổ theo giờ thực tế của timestamp — `hour=HH` phải match với `event_ts`.

---

## Constraints

- Chạy được bằng: `python scripts/generate_mock_data.py`
- Không dùng thư viện nào ngoài flake.nix: `kafka-python-ng`, `pandas`, `numpy`, `pathlib`
- Dùng `PROCESSING_DATE` = ngày hôm qua tính từ `datetime.now() - timedelta(days=1)`
- Log progress ra stdout: `[Kafka] Produced 200 records to events.user`, v.v.
- Tạo thư mục tự động nếu chưa có (kể cả `scripts/`)
- Cuối script in summary table: source → record count → output path
