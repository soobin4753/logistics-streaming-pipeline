import random
import time
from faker import Faker
from datetime import timedelta
import pandas as pd

# 초기 설정
fake = Faker('ko_KR') # 주문 ID 생성용 더미 데이터 생성기
# 재현 가능한 데이터 생성 (결과 고정)
random.seed(42)
fake.seed_instance(42)

# 하나의 주문이 거치는 상태 흐름
EVENT_FLOW = ["created", "assigned", "pickup", "in_transit", "delivered"]

order_state = {} # 주문별 현재 진행 상태 저장
order_routes = {} # 주문별 이동 경로

TARGET_ACTIVE = 10 # 동시에 유지할 "활성 주문 수"

# 위치 생성
def fake_location(lat, lon, scale=0.05):
    # 실제 GPS 기반 + 랜덤 노이즈 추가
    # -> 현실감 있는 위치 시뮬레이션
    return {
        "lat": lat + random.uniform(-scale, scale),
        "lon": lon + random.uniform(-scale, scale)
    }

# 주문 생성
def assign_order(base_lat, base_lon):
    # 새로운 주문 생성
    order_id = fake.uuid4()
    # 초기 상태 (created 단계)
    order_state[order_id] = {
        "step": 0
    }

    # 출발지 / 도착지 생성 (배송 경로 시뮬레이션)
    order_routes[order_id] = {
        "origin": fake_location(base_lat, base_lon, 0.01),
        "destination": fake_location(base_lat, base_lon, 0.05)
    }

    return order_id

# 현재 살아있는 주문 조회
def get_active_orders():
    # 아직 delivered 안 된 주문만 필터링
    return [
        oid for oid, s in order_state.items()
        if s["step"] < len(EVENT_FLOW)
    ]

# 다음 이벤트 상태 생성
def get_next_event(order_id):
    state = order_state[order_id]

    if state["step"] >= len(EVENT_FLOW):
        # 이미 완료된 주문
        return None

    # 현재 step 기준 이벤트 결정
    event_type = EVENT_FLOW[state["step"]]
    # 상태 진행
    state["step"] += 1
    return event_type

# 위치 (이동 시뮬레이션)
def interpolate_location(origin, destination, progress):
    # origin → destination 사이 이동 위치 계산
    return {
        "lat": origin["lat"] + (destination["lat"] - origin["lat"]) * progress,
        "lon": origin["lon"] + (destination["lon"] - origin["lon"]) * progress
    }

# 이벤트 시간 생성
def generate_event_time(base_time, step):
    event_time = pd.to_datetime(base_time)

    # 단계별 시간 증가 (5~15분 랜덤)
    event_time += timedelta(minutes=step * random.randint(5, 15))

    # 10% 확률로 지연 발생 (현실성 추가)
    if random.random() < 0.1:
        event_time -= timedelta(minutes=random.randint(1, 10))

    return event_time.isoformat()


def create_event(row):
    # CSV의 GPS를 환경 입력으로 사용
    base_lat = row["vehicle_gps_latitude"]
    base_lon = row["vehicle_gps_longitude"]

    active_orders = get_active_orders()

    # 주문 유지 로직
    if len(active_orders) < TARGET_ACTIVE:
        order_id = assign_order(base_lat, base_lon) # 부족하면 신규 주문 생성
    else:
        order_id = random.choice(active_orders) # 기존 주문 재사용

    event_type = get_next_event(order_id)

    if event_type is None:
        return None # 이미 완료된 주문 제외

    route = order_routes[order_id]
    state = order_state[order_id]

    # 진행률 계산 (0~1)
    progress = (state["step"] - 1) / (len(EVENT_FLOW) - 1)

    # 차량 위치 계산
    vehicle_location = interpolate_location(
        route["origin"],
        route["destination"],
        progress
    )

    # 최종 이벤트 생성
    return {
        "event_id": f"{order_id}_{state['step']}_{int(time.time()*1000)}", # 유니크 이벤트 ID
        "event_time": generate_event_time(row["timestamp"], state["step"]), # 상태 + 시간 기반 이벤트 타임
        "event_type": event_type, # 상태 (created ~ delivered)

        "order_id": order_id,

        "origin": route["origin"],
        "destination": route["destination"],
        "vehicle_location": vehicle_location, # 현재 배송 위치

        "context": {
            "traffic": row["traffic_congestion_level"],
            "weather": row["weather_condition_severity"],
            "eta_variation": row["eta_variation_hours"]
        }
    }