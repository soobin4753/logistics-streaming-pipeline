from kafka import KafkaConsumer
import json

# Kafka Consumer 생성
consumer = KafkaConsumer(
    "logistics-events", # 구독할 Kafka topic (Producer가 보낸 이벤트 스트림)
    bootstrap_servers="localhost:9094", # Kafka 브로커 주소 (외부 접근 포트)
    auto_offset_reset="earliest", # 처음 실행 시 어디서부터 읽을지 설정(처음부터 모든 데이터 읽기)
    group_id="test-group", # Consumer 그룹 ID(같은 group 내에서는 데이터 분산 처리됨)
    enable_auto_commit=True, # 읽은 메시지 offset 자동 저장(재시작 시 중복 처리 방지)
    value_deserializer=lambda x: json.loads(x.decode("utf-8")) # JSON 형태로 변환해서 사용
)

print("Consumer started")

try:
    for msg in consumer: # Kafka에서 메시지가 들어올 때마다 반복 실행
        try:
            event = msg.value # 역직렬화된 실제 이벤트 데이터

            event_type = event.get("event_type", "UNKNOWN") # 이벤트 상태 (created ~ delivered)
            order_id = event.get("order_id", "UNKNOWN") # 주문 ID

            print(
                f"[{event_type}] "
                f"order={order_id} "
                f"partition={msg.partition} "
                f"offset={msg.offset}"
            )
            # partition = 데이터가 저장된 Kafka shard
            # offset = 해당 partition에서의 순서

        except Exception as e:
            # 개별 메시지 파싱 오류 처리
            print("Message parse error:", e)

except KeyboardInterrupt:
    print("\n Consumer stopped") # Ctrl+C 종료 처리

finally:
    consumer.close() # Kafka 연결 종료