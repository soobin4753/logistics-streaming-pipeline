from faker import Faker
import random
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta


# ENV
load_dotenv()

fake = Faker("ko_KR")
random.seed(42)
fake.seed_instance(42)


# DB CONNECT
print("DB 연결 시도 중...")

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    port=os.getenv("POSTGRES_PORT")
)

cur = conn.cursor()

print("DB 연결 성공")



# 1. LOAD MASTER DATA
def load_entities():
    cur.execute("SELECT driver_id, driver_score FROM driver")
    drivers = cur.fetchall()

    cur.execute("SELECT vehicle_id, vehicle_type FROM vehicle")
    vehicles = cur.fetchall()

    return drivers, vehicles



# 2. CAPACITY LOGIC
def calculate_capacity(driver_score, vehicle_type):

    base_capacity = int(driver_score * 30)

    if vehicle_type == "truck":
        base_capacity += 10
    elif vehicle_type == "van":
        base_capacity += 5
    else:
        base_capacity += 0

    return max(5, base_capacity)



# 3. ASSIGNMENT GENERATOR
def generate_assignments(drivers, vehicles, date):

    assignments = []

    used_vehicles = set()

    # driver 수 기준 1:1 매칭
    for driver_id, driver_score in drivers:

        vehicle_id, vehicle_type = random.choice(vehicles)

        # vehicle 중복 방지
        while vehicle_id in used_vehicles:
            vehicle_id, vehicle_type = random.choice(vehicles)

        used_vehicles.add(vehicle_id)

        # shift 정의
        shift_start = datetime.strptime(f"{date} 09:00:00", "%Y-%m-%d %H:%M:%S")
        shift_end = datetime.strptime(f"{date} 18:00:00", "%Y-%m-%d %H:%M:%S")

        # capacity 계산 (핵심)
        capacity = calculate_capacity(driver_score, vehicle_type)

        assignments.append((
            driver_id,
            vehicle_id,
            date,
            shift_start,
            shift_end,
            "ACTIVE",
            capacity,
            0  # current_orders
        ))

    return assignments



# 4. DB INSERT
def insert_assignments(assignments):

    query = """
        INSERT INTO driver_vehicle_assignment (
            driver_id,
            vehicle_id,
            assigned_date,
            shift_start,
            shift_end,
            status,
            max_orders,
            current_orders
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """

    execute_batch(cur, query, assignments)



# 5. MAIN
if __name__ == "__main__":

    print("driver / vehicle 로딩 중...")
    drivers, vehicles = load_entities()

    print(f"drivers: {len(drivers)}, vehicles: {len(vehicles)}")

    today = datetime.now().date().isoformat()

    print("assignment 생성 중...")

    assignments = generate_assignments(drivers, vehicles, today)

    print(f"{len(assignments)}개 assignment 생성 완료")

    insert_assignments(assignments)

    conn.commit()
    cur.close()
    conn.close()

    print("Assignments seeded successfully")