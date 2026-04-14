which pg_ctl
/nix/store/34pghcna1jilziyr3zdz792gvfmr8xfy-postgresql-15.17/bin/pg_ctl

cd /nix/store/34pghcna1jilziyr3zdz792gvfmr8xfy-postgresql-15.17/bin/
--> initdb

which initdb
/nix/store/34pghcna1jilziyr3zdz792gvfmr8xfy-postgresql-15.17/bin/initdb
cd /nix/store/34pghcna1jilziyr3zdz792gvfmr8xfy-postgresql-15.17/bin
--> cd .. --> share 
man pg_hba.conf --> documentation 
mkdir -p ./airflow-data/postgres

initdb -D ./airflow-data/postgres --auth-local=scram-sha-256 --auth-host=scram-sha-256 -U airflow_admin -W


add ps.asyncpg in nix because airflow requires


mkdir ./ETL/infrastructure/airflow-data/postgres/sockets
which pg_ctl 

❯ pg_ctl -D ./ETL/infrastructure/airflow-data/postgres -l ./ETL/infrastructure/airflow-data/postgres/logfile start
waiting for server to start.... stopped waiting
pg_ctl: could not start server
Examine the log output.
❯ cat ./ETL/infrastructure/airflow-data/postgres/logfile    on  etl [✘]
2026-04-14 17:14:22.837 +07 [10104] LOG:  starting PostgreSQL 15.17 on x86_64-pc-linux-gnu, compiled by clang version 21.1.7, 64-bit
2026-04-14 17:14:22.838 +07 [10104] LOG:  listening on IPv4 address "127.0
.0.1", port 5432
2026-04-14 17:14:22.841 +07 [10104] FATAL:  could not create lock file "/run/postgresql/.
s.PGSQL.5432.lock": No such file or directory
2026-04-14 17:14:22.843 +07 [10104] LOG:  database system is shut down
unix_socket_directories = './sockets/'	# comma-separated list of directories


❄️ tandat-interview
❯ psql -U airflow_admin -d postgres -h ./ETL/infrastructure/airflow-data/postgres/sockets
psql: error: could not translate host name "./ETL/infrastructure/airflow-data/postgres/sockets" to address: Name or service not known

❄️ tandat-interview
❯ realpath ./ETL/infrastructure/airflow-data/postgres/sockets
/home/tandat8896-nix/tandat-interview/ETL/infrastructure/airflow-data/postgres/sockets

❄️ tandat-interview
❯ psql -U airflow_admin -d postgres -h /home/tandat8896-nix/tandat-interview/ETL/infrastructure/airflow-data/postgres/sockets
Password for user airflow_admin:
psql (15.17)
Type "help" for help.

postgres=# CREATE DATABASE airflow_db;
CREATE DATABASE
postgres=# CREATE USER airflow_user WITH PASSWORD '123456';
CREATE ROLE
postgres=# GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT
postgres=#


-- 1. Phải vào đúng phòng cái đã
\c airflow_db

-- 2. Cho phép dùng và tạo bảng trong vườn 'public'
GRANT USAGE, CREATE ON SCHEMA public TO airflow_user;

-- 3. Đảm bảo sau này app đẻ ra bảng nào thì user cũng có quyền đọc/ghi bảng đó
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO airflow_user;

-- 4. Cấp quyền cho các bảng ĐÃ tồn tại (nếu có)
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow_user;


airflow_db=# exit
                         List of relations
 Schema |                Name                | Type  |    Owner
--------+------------------------------------+-------+--------------
 public | alembic_version                    | table | airflow_user
 public | asset                              | table | airflow_user
 public | asset_active                       | table | airflow_user
 public | asset_alias                        | table | airflow_user
 public | asset_alias_asset                  | table | airflow_user
 public | asset_alias_asset_event            | table | airflow_user
 public | asset_dag_run_queue                | table | airflow_user
 public | asset_event                        | table | airflow_user
 public | asset_trigger                      | table | airflow_user
 public | backfill                           | table | airflow_user
 public | backfill_dag_run                   | table | airflow_user
 public | callback_request                   | table | airflow_user
 public | connection                         | table | airflow_user
 public | dag                                | table | airflow_user
 public | dag_bundle                         | table | airflow_user
 public | dag_bundle_team                    | table | airflow_user
 public | dag_code                           | table | airflow_user
 public | dag_favorite                       | table | airflow_user
:
