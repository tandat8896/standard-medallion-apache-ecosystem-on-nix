# Airflow + Kafka + Nix Dev Setup

## Airflow binary check
which airflow
/nix/store/.../python3.13-apache-airflow-3.1.7/bin/airflow

Airflow đang chạy từ Nix store → immutable environment, version pinned 3.1.7

---

## Project context
cd /nix/store/...
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
