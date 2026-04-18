# Thiết lập nơi ghi dữ liệu (để không bị lỗi Read-only)
SPARK_WORKER_DIR=/home/tandat8896-nix/tandat-interview/ETL/infrastructure/spark-data/work
SPARK_LOG_DIR=/home/tandat8896-nix/tandat-interview/ETL/infrastructure/spark-data/logs

# Thiết lập mạng (để không bị xung đột với Airflow)
SPARK_MASTER_HOST=127.0.0.1
SPARK_MASTER_WEBUI_PORT=8181
SPARK_WORKER_WEBUI_PORT=8182
