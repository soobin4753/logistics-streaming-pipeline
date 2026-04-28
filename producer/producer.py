import json
import time
import random
import pandas as pd
from kafka import KafkaProducer
from event_generator import create_event


# CSV 데이터 로드
df = pd.read_csv("data/dynamic_supply_chain_logistics_dataset.csv")


# Kafka Producer 생성
producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "logistics-events"

try:
    for _, row in df.iterrows():

        # 이벤트 생성
        event = create_event(row)

        # 생성 실패 시 skip
        if event is None:
            continue

        
        # Kafka 전송 + metadata 확보
        future = producer.send(
            TOPIC,
            key=event["order_id"].encode("utf-8"),
            value=event
        )

        # Kafka ack 기다리고 partition / offset 받기
        metadata = future.get(timeout=10)

        
        # 디버깅 로그
        print(
            f"order={event['order_id']} "
            f"driver={event['driver_id']} "
            f"vehicle={event['vehicle_id']} "
            f"partition={metadata.partition} "
            f"offset={metadata.offset}"
        )

        
        # 스트리밍처럼 보이게 딜레이
        time.sleep(random.uniform(0.2, 0.7))

except Exception as e:
    print("Producer error:", e)

finally:
    # 남은 메시지 flush
    producer.flush()
    producer.close()
    print("Producer closed")