❯ which airflow
/nix/store/i66ppas7zfbcb3qngk5gcpkzc3ik59cy-python3.13-apache-airflow-3.1.7/bin/airflow


❯ cd /nix/store/i66ppas7zfbcb3qngk5gcpkzc3ik59cy-python3.13-apache-airflow-3.1.7

cd ~/tandat-interview/ETL

Không chỉnh trong Nix store (read-only), chỉ làm việc trong project

---

## flake.nix – add Airflow
airflow = pkgs.python3Packages.apache-airflow;

Inject Airflow CLI vào dev shell qua Nix

---

## AIRFLOW_HOME
export AIRFLOW_HOME="$PWD/ETL/infrastructure/airflow-data"

Chứa:
- metadata DB
- logs
- dags state

---

## Fernet key generate
python fernet-key-create.py

Dùng để encrypt:
- connections
- variables
- secrets

---

## Python env dependency
pythonEnv = pkgs.python3.withPackages (ps: [
  ps.cryptography
]);

Cần cho Fernet encryption

---

## airflow.cfg
fernet_key =

Không hardcode key trong file config

---

## .env override
AIRFLOW__CORE__FERNET_KEY=your_fernet_key

Format:
AIRFLOW__<SECTION>__<KEY>

---

## direnv + dotenv
.envrc use dotenv

Auto load env khi vào project

---

## nix dev shell
nix develop pkgs#infrastructure .

Bật full environment:
- airflow
- kafka
- python libs

---

## init airflow DB
airflow db migrate

Tạo metadata DB schema

---

## rotate fernet key
airflow fernet rotate key

Đổi encryption key (cẩn thận data cũ)

---

## JWT + API secrets
AIRFLOW__API_AUTH__JWT_SECRET=your_jwt_secret
AIRFLOW__API__SECRET_KEY=your_secret_key

---

## check config
airflow config get-value api_auth jwt_secret
airflow config get-value api secret_key

---

## Kafka start
just start-kafka

Broker 1:
kafka-server-start.sh server-1.properties > broker-1.log 2>&1 &

Broker 2:
kafka-server-start.sh server-2.properties > broker-2.log 2>&1 &

→ local 2-node Kafka cluster

---

## Airflow providers
airflow providers list

- common-compat
- common-io
- common-sql
- smtp
- standard

Missing: Kafka provider

---

## Kafka provider Nix package
airflow-kafka = pkgs.python3Packages.buildPythonPackage rec {
  pname = "apache-airflow-providers-apache-kafka";
  version = "1.6.0";

  src = pkgs.fetchurl {
    url = "https://files.pythonhosted.org/...tar.gz";
    sha256 = "sha256-BZpWjvyPoVrSw168eSRmJoaF5cNDCek+bE6Q2KRNi+0=...";
  };

  doCheck = false;

  nativeBuildInputs = [
    pkgs.python3Packages.flit-core
  ];

  propagatedBuildInputs = [
    pkgsUnstable.apache-airflow
    pkgs.python3Packages.confluent-kafka
  ];
};

---

## Architecture tổng

- Airflow: orchestration
- Kafka: messaging (2 brokers)
- Provider: bridge Airflow ↔ Kafka
- Nix: reproducible environment
- direnv/dotenv: env injection
- Fernet + JWT: security layer


config airflow with scamr-sha-256 authentication and connect airflow-database to postgres
tìm sqlalchemy
thay = 
sql_alchemy_conn = postgresql+psycopg2://airflow_admin@localhost:5432/postgres

mk lưu trong .env


vào db pgsql 
postgres=# CREATE DATABASE airflow_db;
CREATE DATABASE
postgres=# CREATE USER airflow_user WITH PASSWORD '123456';
CREATE ROLE
postgres=# GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT
postgres=#


postgres=# CREATE DATABASE airflow_db;
CREATE DATABASE
postgres=# CREATE USER airflow_user WITH PASSWORD '123456';
CREATE ROLE
postgres=# GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT
postgres=#
sữa sql lại và env như cũ 

repath/home/tandat8896-nix/tandat-interview/ETL/infrastructure/airflow-data/postgres/sockets
❯ realpath ./ETL/infrastructure/airflow-data/postgres/sockets        on  etl [✘!]
/home/tandat8896-nix/tandat-interview/ETL/infrastructure/airflow-data/postgres/sockets


xcom_objectstorage_path = 




❄️ tandat-interview/ETL/infrastructure
❯ kafka-topics.sh --create \                                                                                on  main [!?]
  --topic user-events \
  --partitions 2 \
  --replication-factor 1 \
  --bootstrap-server localhost:9092
Created topic user-events.

❄️ tandat-interview/ETL/infrastructure
❯ kafka-log-dirs.sh --bootstrap-server localhost:9092 \                                                     on  main [!?]
  --describe \
  --topic-list user-events
Querying brokers for log directories information
Received log directory information from brokers 1,2
{"brokers":[{"broker":1,"logDirs":[{"partitions":[{"partition":"user-events-0","size":0,"offsetLag":0,"isFuture":false}],"error":null,"logDir":"/home/tandat8896-nix/tandat-interview/ETL/infrastructure/kafka-data/broker-1"}]},{"broker":2,"logDirs":[{"partitions":[{"partition":"user-events-1","size":0,"offsetLag":0,"isFuture":false}],"error":null,"logDir":"/home/tandat8896-nix/tandat-interview/ETL/infrastructure/kafka-data/broker-2"}]}],"version":1}


❄️ tandat-interview/ETL/infrastructure
❯ kafka-console-producer.sh --bootstrap-server localhost:9092 \                                             on  main [!?]
  --topic user-events \
  --property "parse.key=true" \
  --property "key.separator=:"
>

 Test Thử Với Console Producer (Dữ Liệu Nhỏ)
bash# Terminal 1: Chạy Producer
kafka-console-producer.sh \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --property "key.separator=:" \
  --property "parse.key=true"
Sau đó type vào:
user_1:{"event":"click"}
user_1:{"event":"click"}
user_1:{"event":"click"}
user_2:{"event":"view"}
user_3:{"event":"click"}
user_1:{"event":"click"}
user_1:{"event":"click"}
user_1:{"event":"click"}
user_1:{"event":"click"}
user_1:{"event":"click"}
(Ctrl+C để thoát sau)

2️⃣ Check Partition Distribution
bashkafka-log-dirs.sh --bootstrap-server localhost:9092 \
  --describe \
  --topic-list user-events
Xem kết quả:

Partition 0 và Partition 1 có bao nhiêu messages?
user_1 có tập trung ở 1 partition không?


3️⃣ Check Consumer Lag
bash# Tạo consumer group để xem
kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group test-group \
  --describe
Xem:

LAG của mỗi partition
Partition nào processing chậm hơn?


4️⃣ Xem Chi Tiết Messages
bashkafka-console-consumer.sh \
  --topic user-events \
  --bootstrap-server localhost:9092 \
  --from-beginning \
  --property "print.key=true"
Xem:

Key của mỗi message là gì?
Messages từ user_1 đều vào 1 partition?


Sau khi hiểu rõ, mới chạy script produce 10,000 messages.



hoặc giám sát 
--> salting key để phân phối đều  hơn 
--> thêm timestamp vào key để tránh tập trung vào 1 partitions
--> thêm random salt vào key để phân phối đều hơ> [!NOTE]
> **Lưu ý**: Khi chạy các lệnh Kafka, hãy đảm bảo rằng Kafka cluster của bạn đang hoạt động và bạn đã tạo topic `user-events` trước đó.

