# Kafka Setup (KRaft Mode - Multi Broker - Nix)

---

## 1. Xác định Kafka binary

```bash
which kafka-server-start.sh
```

Output:
```bash
/nix/store/r5k458c5b87cmd60qdyklybqr3jyd486-apache-kafka-2.13-4.1.0/bin/kafka-server-start.sh
```

---

## 2. Copy config về project

```bash
cd /nix/store/r5k458c5b87cmd60qdyklybqr3jyd486-apache-kafka-2.13-4.1.0/config

cp server.properties ~/tandat-interview/ETL

cd ~/tandat-interview/ETL/.config/

chmod 644 server.properties
```

---

## 3. Chỉnh sửa `server.properties`

### 3.1 Thay đổi storage path

```properties
log.dirs=/tmp/kraft-combined-logs
```

➡️ đổi thành:

```properties
log.dirs=/home/tandat8896-nix/tandat-interview/ETL/infrastructure/kafka-data/.kafka-logs
```

---

### 3.2 Fix controller quorum (QUAN TRỌNG)

```properties
controler.quorum.voters=1@localhost:9093
controller.quorum.bootstrap.servers=localhost:9093
```

➡️ đổi thành:

```properties
controller.quorum.voters=1@localhost:9093
```

---

## 4. Tạo nhiều broker config

```bash
cp server.properties server-1.properties
cp server.properties server-2.properties
```

---

## 5. Cấu hình chi tiết các node

| Thuộc tính (Attribute) | `server.properties` (Khuôn) | `server-1.properties` (Sếp + Lính) | `server-2.properties` (Lính) |
| :--- | :--- | :--- | :--- |
| **Vai trò (`process.roles`)** | `broker,controller` | `broker,controller` | `broker` |
| **Định danh (`node.id`)** | `1` | `1` | `2` |
| **Cổng (`listeners`)** | `PLAINTEXT://:9092,CONTROLLER://:9093` | `PLAINTEXT://:9092,CONTROLLER://:9093` | `PLAINTEXT://:9094` |
| **Quảng bá (`advertised.listeners`)** | `PLAINTEXT://localhost:9092` | `PLAINTEXT://localhost:9092` | `PLAINTEXT://localhost:9094` |
| **Bầu cử (`controller.quorum.voters`)** | `1@localhost:9093` | `1@localhost:9093` | `1@localhost:9093` |
| **Lưu trữ (`log.dirs`)** | `.../.kafka-logs` | `.../broker-1` | `.../broker-2` |
| **Phân mảnh (`num.partitions`)** | `2` | `2` | `2` |

---

## 6. Format storage (KRaft - MULTI NODE)

### 6.1 Format broker-1 (tạo cluster.id)

```bash
kafka-storage.sh format \
  -t $(kafka-storage.sh random-uuid) \
  -c infrastructure/.config/server-1.properties
```

Output:
```
Formatting metadata directory .../broker-1
```

---

### 6.2 Lấy `cluster.id`

```bash
cat infrastructure/kafka-data/broker-1/meta.properties
```

Ví dụ:
```properties
node.id=1
directory.id=JvAQ2ir-k0Z3-BW75V8Mwg
version=1
cluster.id=mT997lCmQF2tqYbMIO4QBg
```

---

### 6.3 Format broker-2 với CÙNG `cluster.id`

```bash
kafka-storage.sh format \
  -t mT997lCmQF2tqYbMIO4QBg \
  -c infrastructure/.config/server-2.properties
```

Output:
```
Formatting metadata directory .../broker-2
```

---

## ⚠️ Nguyên tắc QUAN TRỌNG

### 1. Tất cả broker phải dùng cùng `cluster.id`
👉 Đây là điều kiện để join cùng cluster

---

### 2. Mỗi broker phải có `log.dirs` riêng

Ví dụ:
```
broker-1 → kafka-data/broker-1
broker-2 → kafka-data/broker-2
```

---

### 3. Chỉ có 1 controller (trong setup này)

```
server-1 → controller + broker
server-2 → broker
```

---

### 4. Sai key là chết ngay

❌ Sai:
```properties
control.quorum.voters
controler.quorum.voters
```

✅ Đúng:
```properties
controller.quorum.voters
```

---

## 7. Ghi chú

- Setup đang dùng **KRaft mode (không Zookeeper)**
- Phù hợp cho:
  - local dev
  - ETL pipeline test
  - học distributed system cơ bản
- Không phù hợp production (vì chỉ có 1 controller, replication = 1)

---

## 8. Next step (khuyến nghị)

- Start `server-1` trước
kafka-server-start.sh infrastructure/.config/server-1.properties
- Start `server-2`
kafka-server-start.sh infrastructure/.config/server-2.properties
- Verify quorum:
```bash
kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
kafka-broker-api-versions.sh --bootstrap-server localhost:9092
```

- Test producer / consumer
kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status
ClusterId:              7cWf3DGGTeKhb_WttJNKag
LeaderId:               1
LeaderEpoch:            1
HighWatermark:          94
MaxFollowerLag:         0
MaxFollowerLagTimeMs:   0
CurrentVoters:          [{"id": 1, "endpoints": ["CONTROLLER://localhost:9093"]}]
CurrentObservers:       [{"id": 2, "directoryId": "bpVf6z5GaO5AcscYrlm68g"}]
