import random
import time
from faker import Faker
from datetime import timedelta
import pandas as pd

fake = Faker('ko_KR')
random.seed(42)
fake.seed_instance(42)

EVENT_FLOW = ["created", "assigned", "pickup", "in_transit", "delivered"]

order_state = {}
order_routes = {}

TARGET_ACTIVE = 10 


def fake_location(lat, lon, scale=0.05):
    return {
        "lat": lat + random.uniform(-scale, scale),
        "lon": lon + random.uniform(-scale, scale)
    }


def assign_order(base_lat, base_lon):
    order_id = fake.uuid4()

    order_state[order_id] = {
        "step": 0
    }

    order_routes[order_id] = {
        "origin": fake_location(base_lat, base_lon, 0.01),
        "destination": fake_location(base_lat, base_lon, 0.05)
    }

    return order_id


def get_active_orders():
    return [
        oid for oid, s in order_state.items()
        if s["step"] < len(EVENT_FLOW)
    ]


def get_next_event(order_id):
    state = order_state[order_id]

    if state["step"] >= len(EVENT_FLOW):
        return None

    event_type = EVENT_FLOW[state["step"]]
    state["step"] += 1
    return event_type


def interpolate_location(origin, destination, progress):
    return {
        "lat": origin["lat"] + (destination["lat"] - origin["lat"]) * progress,
        "lon": origin["lon"] + (destination["lon"] - origin["lon"]) * progress
    }


def generate_event_time(base_time, step):
    event_time = pd.to_datetime(base_time)

    # 단계별 시간 증가
    event_time += timedelta(minutes=step * random.randint(5, 15))

    # 일부 늦게 도착
    if random.random() < 0.1:
        event_time -= timedelta(minutes=random.randint(1, 10))

    return event_time.isoformat()


def create_event(row):
    base_lat = row["vehicle_gps_latitude"]
    base_lon = row["vehicle_gps_longitude"]

    active_orders = get_active_orders()

    # 🔥 목표 개수 유지
    if len(active_orders) < TARGET_ACTIVE:
        order_id = assign_order(base_lat, base_lon)
    else:
        order_id = random.choice(active_orders)

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

    return {
        "event_id": f"{order_id}_{state['step']}_{int(time.time()*1000)}",
        "event_time": generate_event_time(row["timestamp"], state["step"]),
        "event_type": event_type,

        "order_id": order_id,

        "origin": route["origin"],
        "destination": route["destination"],
        "vehicle_location": vehicle_location,

        "context": {
            "traffic": row["traffic_congestion_level"],
            "weather": row["weather_condition_severity"],
            "eta_variation": row["eta_variation_hours"]
        }
    }