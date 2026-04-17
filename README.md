# ETL Lab

## Setup
```bash
direnv reload
```

## Chạy theo thứ tự

```bash
just start-kafka
just start-spark
just shell
```

## Test streaming

Terminal 1 - start stream trong pyspark shell:
```python
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "user-events").option("startingOffsets", "latest").load()
query = df.selectExpr("CAST(value AS STRING)").writeStream.format("console").start()
```

Terminal 2 - gửi data:
```bash
just test
```

## Tắt
```bash
just stop-spark
just stop-kafka
```

