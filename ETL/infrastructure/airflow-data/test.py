# FILE: test.py
try:
    # ĐÂY LÀ CÁCH IMPORT CHUẨN CHO BẢN 1.6.0
    from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator
    
    print("✅ XONG! Đã tìm thấy ProduceToTopicOperator.")
    
    # Khởi tạo thử xem có mắng mỏ gì không
    task = ProduceToTopicOperator(
        task_id="test_kafka",
        kafka_config_id="kafka_default",
        topic="fraud_events",
        producer_payload={"key": "value"}
    )
    print("🚀 Khởi tạo Operator thành công rực rỡ!")

except Exception as e:
    print(f"❌ Vẫn còn biến: {e}")
