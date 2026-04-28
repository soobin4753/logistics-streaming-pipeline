import random
import time
import pandas as pd
from faker import Faker
from datetime import timedelta
import psycopg2
from dotenv import load_dotenv
import os



# INIT
load_dotenv()

fake = Faker('ko_KR')
fake.seed_instance(42)
random.seed(42)



# DB CONNECT
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    port=os.getenv("POSTGRES_PORT")
)

cur = conn.cursor()



# CONSTANTS
EVENT_FLOW = ["created", "assigned", "pickup", "in_transit", "delivered"]

order_state = {}
order_assignment = {}

TARGET_ACTIVE = 10



# LOAD ASSIGNMENTS
def load_assignments():
    cur.execute("""
        SELECT driver_id, vehicle_id
        FROM driver_vehicle_assignment
        WHERE status = 'ACTIVE'
    """)
    return cur.fetchall()



# GLOBAL CACHE (CRITICAL FIX)
ASSIGNMENTS_CACHE = load_assignments()

if not ASSIGNMENTS_CACHE:
    raise Exception("Assignment table is EMPTY. Run assignment seed first.")



# STATE MACHINE
def get_next_event(order_id):

    state = order_state[order_id]

    if state["step"] >= len(EVENT_FLOW):
        return None

    event_type = EVENT_FLOW[state["step"]]
    state["step"] += 1

    return event_type



# ORDER CREATION
def create_order():

    order_id = fake.uuid4()

    assignment = random.choice(ASSIGNMENTS_CACHE)

    order_assignment[order_id] = {
        "driver_id": assignment[0],
        "vehicle_id": assignment[1]
    }

    order_state[order_id] = {"step": 0}

    return order_id



# LOCATION SIMULATION
def interpolate_location(base_lat, base_lon, progress):

    return {
        "lat": base_lat + (0.05 * progress),
        "lon": base_lon + (0.05 * progress)
    }



# DELAY MODEL
def calculate_delay_factor(row):

    delay = 0

    if row["traffic_congestion_level"] > 8:
        delay += 0.3

    if row["weather_condition_severity"] > 0.7:
        delay += 0.2

    return delay


# DB UPDATE (STATE CHANGE)
def update_assignment(driver_id):

    cur.execute("""
        UPDATE driver_vehicle_assignment
        SET current_orders = current_orders + 1
        WHERE driver_id = %s
    """, (driver_id,))

    conn.commit()



# MAIN EVENT GENERATOR
def create_event(row):

    base_lat = row["vehicle_gps_latitude"]
    base_lon = row["vehicle_gps_longitude"]

    active_orders = list(order_state.keys())

    # order 생성 or reuse
    if len(active_orders) < TARGET_ACTIVE:
        order_id = create_order()
    else:
        order_id = random.choice(active_orders)

    state = order_state[order_id]

    event_type = get_next_event(order_id)

    if event_type is None:
        return None

    progress = state["step"] / len(EVENT_FLOW)

    assignment = order_assignment[order_id]

    vehicle_location = interpolate_location(
        base_lat,
        base_lon,
        progress
    )

    delay_factor = calculate_delay_factor(row)

    event_time = pd.to_datetime(row["timestamp"]) + timedelta(
        minutes=state["step"] * random.randint(5, 15)
    )


    # EVENT BUILD
    event = {
        "event_id": f"{order_id}_{state['step']}_{int(time.time()*1000)}",
        "event_time": event_time.isoformat(),

        "event_type": event_type,
        "order_id": order_id,

        # resource binding (핵심)
        "driver_id": assignment["driver_id"],
        "vehicle_id": assignment["vehicle_id"],

        # location
        "vehicle_location": vehicle_location,

        # context
        "context": {
            "traffic": row["traffic_congestion_level"],
            "weather": row["weather_condition_severity"],
            "eta_variation": row["eta_variation_hours"]
        },

        # risk
        "delay_risk": round(delay_factor, 2)
    }

    
    # STATE UPDATE
    update_assignment(assignment["driver_id"])

    return event



# CLEANUP
def close_connection():
    cur.close()
    conn.close()