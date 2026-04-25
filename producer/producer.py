import json
import time
import random
import pandas as pd
from kafka import KafkaProducer
from event_generator import create_event

# CSV 데이터 로드
# -> Kaggle 물류 데이터를 기반으로 이벤트 생성의 입력으로 사용
df = pd.read_csv("data/dynamic_supply_chain_logistics_dataset.csv")

# Kafka Producer 생성
producer = KafkaProducer(
    bootstrap_servers='localhost:9094', # Kafka 브로커 주소 (외부 접근용 포트)
    value_serializer=lambda v: json.dumps(v).encode('utf-8') # Python dict → JSON → byte 변환 (Kafka 전송 형식)
)

TOPIC = "logistics-events" # 이벤트가 전송될 Kafka topic (데이터 스트림 채널)

try:
    for _, row in df.iterrows(): # CSV의 각 row를 순차적으로 처리 (batch → stream 변환 역할)

        # 각 row를 기반으로 "배송 이벤트" 생성
        event = create_event(row)

        # 이벤트 생성 실패 시 skip
        if event is None:
            continue

        # Kafka로 이벤트 전송
        producer.send(
            TOPIC,
            key=event["order_id"].encode("utf-8"), 
            # 동일 order_id 기준으로 같은 partition에 묶이도록 설정
            # → 순서 보장 (ordering guarantee)
            value=event
            # 실제 이벤트 데이터 (JSON)
        )

        print(f"[{event['event_type']}] order={event['order_id']}")

        # 실제 스트리밍처럼 보이게 랜덤 딜레이 추가
        time.sleep(random.uniform(0.2, 0.7))

except Exception as e:
    # 전체 producer 실행 중 오류 처리
    print("Producer error:", e)

finally:
    producer.flush() # Kafka 내부 buffer 강제 전송
    producer.close() # 연결 종료
    print("Producer closed")