from kafka.admin import KafkaAdminClient, NewTopic

# Kafka 관리자 클라이언트 생성
admin = KafkaAdminClient(
    bootstrap_servers="localhost:9094"
)
# bootstrap_servers:
# → Kafka 브로커 접속 주소 (외부 접근용 포트 9094)

# Topic 생성 정의
topic = NewTopic(
    name="logistics-events", # Topic 이름
    num_partitions=3, # Partition 개수
    replication_factor=1 # 복제 개수
)

try:
    admin.create_topics([topic]) # Kafka에 topic 생성 요청
    print("Topic created")
except Exception as e:
    print("이미 존재하거나 오류:", e) # 이미 존재하거나 Kafka 연결 문제 발생 시 예외 처리