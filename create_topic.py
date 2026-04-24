from kafka.admin import KafkaAdminClient, NewTopic

admin = KafkaAdminClient(
    bootstrap_servers="localhost:9094"
)

topic = NewTopic(
    name="logistics-events",
    num_partitions=3,
    replication_factor=1
)

try:
    admin.create_topics([topic])
    print("Topic created")
except Exception as e:
    print("이미 존재하거나 오류:", e)