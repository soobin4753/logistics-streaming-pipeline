import random
import time
from faker import Faker
from datetime import timedelta
import pandas as pd

fake = Faker('ko_KR')

# 재현성 유지
fake.seed_instance(42)
random.seed(42)

EVENT_FLOW = ["created", "assigned", "pickup", "in_transit", "delivered"]

order_state = {}
order_routes = {}

TARGET_ACTIVE = 10



# 위치 생성 (faker + 약간의 noise)
def fake_location(lat, lon, scale=0.05):
    return {
        "lat": lat + fake.pyfloat(min_value=-scale, max_value=scale),
        "lon": lon + fake.pyfloat(min_value=-scale, max_value=scale)
    }



# driver 생성 
def create_driver():
    experience = fake.random_int(min=1, max=10)  # 경력 (년)

    # 경력 기반 score
    driver_score = round(0.5 + (experience / 10) * 0.5, 2)

    # 피로도 (낮을수록 좋음)
    fatigue = round(fake.pyfloat(min_value=0, max_value=1), 2)

    return {
        "driver_id": f"DRV_{fake.random_int(1, 50)}",
        "driver_name": fake.name(),
        "experience_years": experience,
        "driver_score": driver_score,
        "fatigue": fatigue
    }



# vehicle 생성 (거리 기반)
def create_vehicle(origin, destination):
    distance = abs(origin["lat"] - destination["lat"]) + abs(origin["lon"] - destination["lon"])

    if distance < 0.02:
        vehicle_type = "bike"
    elif distance < 0.05:
        vehicle_type = "van"
    else:
        vehicle_type = "truck"

    return {
        "vehicle_id": f"VEH_{fake.random_int(1, 100)}",
        "vehicle_type": vehicle_type
    }



# 주문 생성
def assign_order(base_lat, base_lon):
    order_id = fake.uuid4()

    order_state[order_id] = {"step": 0}

    origin = fake_location(base_lat, base_lon, 0.01)
    destination = fake_location(base_lat, base_lon, 0.05)

    driver = create_driver()
    vehicle = create_vehicle(origin, destination)

    order_routes[order_id] = {
        "origin": origin,
        "destination": destination,
        "driver": driver,
        "vehicle": vehicle
    }

    return order_id



# 활성 주문 조회
def get_active_orders():
    return [
        oid for oid, s in order_state.items()
        if s["step"] < len(EVENT_FLOW)
    ]



# 이벤트 상태 전이
def get_next_event(order_id):
    state = order_state[order_id]

    if state["step"] >= len(EVENT_FLOW):
        return None

    event_type = EVENT_FLOW[state["step"]]
    state["step"] += 1

    return event_type



# 위치 이동
def interpolate_location(origin, destination, progress):
    return {
        "lat": origin["lat"] + (destination["lat"] - origin["lat"]) * progress,
        "lon": origin["lon"] + (destination["lon"] - origin["lon"]) * progress
    }



# 지연 요인 계산
def calculate_delay_factor(driver, row):
    delay = 0

    # 내부 요인
    if driver["driver_score"] < 0.7:
        delay += 0.2

    if driver["fatigue"] > 0.7:
        delay += 0.2

    # 외부 요인
    if row["traffic_congestion_level"] > 8:
        delay += 0.3

    if row["weather_condition_severity"] > 0.7:
        delay += 0.2

    return delay



# 이벤트 시간 생성
def generate_event_time(base_time, step, delay_factor):
    event_time = pd.to_datetime(base_time)

    base_delay = fake.random_int(min=5, max=15)
    delay_minutes = int(base_delay * (1 + delay_factor))

    event_time += timedelta(minutes=step * delay_minutes)

    return event_time.isoformat()



# 이벤트 생성 (메인)
def create_event(row):
    base_lat = row["vehicle_gps_latitude"]
    base_lon = row["vehicle_gps_longitude"]

    active_orders = get_active_orders()

    if len(active_orders) < TARGET_ACTIVE:
        order_id = assign_order(base_lat, base_lon)
    else:
        order_id = fake.random_element(active_orders)

    event_type = get_next_event(order_id)

    if event_type is None:
        return None

    route = order_routes[order_id]
    state = order_state[order_id]

    progress = (state["step"] - 1) / (len(EVENT_FLOW) - 1)

    vehicle_location = interpolate_location(
        route["origin"],
        route["destination"],
        progress
    )

    driver = route["driver"]
    vehicle = route["vehicle"]

    # 지연 요인 계산
    delay_factor = calculate_delay_factor(driver, row)

    return {
        "event_id": f"{order_id}_{state['step']}_{int(time.time()*1000)}",
        "event_time": generate_event_time(row["timestamp"], state["step"], delay_factor),
        "event_type": event_type,

        "order_id": order_id,

        # driver
        "driver_id": driver["driver_id"],
        "driver_name": driver["driver_name"],
        "experience_years": driver["experience_years"],
        "driver_score": driver["driver_score"],
        "fatigue": driver["fatigue"],

        # vehicle
        "vehicle_id": vehicle["vehicle_id"],
        "vehicle_type": vehicle["vehicle_type"],

        # 위치
        "origin": route["origin"],
        "destination": route["destination"],
        "vehicle_location": vehicle_location,

        # 외부 요인
        "context": {
            "traffic": row["traffic_congestion_level"],
            "weather": row["weather_condition_severity"],
            "eta_variation": row["eta_variation_hours"]
        },

        # 핵심 분석 변수
        "delay_risk": round(delay_factor, 2)
    }