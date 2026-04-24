import json
import time
import random
import pandas as pd
from kafka import KafkaProducer
from event_generator import create_event

df = pd.read_csv("data/dynamic_supply_chain_logistics_dataset.csv")

producer = KafkaProducer(
    bootstrap_servers='localhost:9094',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC = "logistics-events"

try:
    for _, row in df.iterrows():

        event = create_event(row)

        if event is None:
            continue

        producer.send(
            TOPIC,
            key=event["order_id"].encode("utf-8"),
            value=event
        )

        print(f"[{event['event_type']}] order={event['order_id']}")

        time.sleep(random.uniform(0.2, 0.7))

except Exception as e:
    print("Producer error:", e)

finally:
    producer.flush()
    producer.close()
    print("Producer closed")