from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "logistics-events",
    bootstrap_servers="localhost:9094",
    auto_offset_reset="earliest",
    group_id="test-group",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

print("Consumer started")

try:
    for msg in consumer:
        try:
            event = msg.value

            event_type = event.get("event_type", "UNKNOWN")
            order_id = event.get("order_id", "UNKNOWN")

            print(
                f"[{event_type}] "
                f"order={order_id} "
                f"partition={msg.partition} "
                f"offset={msg.offset}"
            )

        except Exception as e:
            print("⚠️ Message parse error:", e)

except KeyboardInterrupt:
    print("\n Consumer stopped")

finally:
    consumer.close()