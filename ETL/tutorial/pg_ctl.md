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

