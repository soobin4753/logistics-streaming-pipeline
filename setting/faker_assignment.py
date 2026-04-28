import random
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
import pandas as pd

load_dotenv()


# DB 연결
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    port=os.getenv("POSTGRES_PORT")
)

cur = conn.cursor()



# driver / vehicle 조회
def load_ids():
    cur.execute("SELECT driver_id FROM driver")
    drivers = [row[0] for row in cur.fetchall()]

    cur.execute("SELECT vehicle_id FROM vehicle")
    vehicles = [row[0] for row in cur.fetchall()]

    if not drivers or not vehicles:
        raise Exception("❌ driver / vehicle 데이터 없음")

    return drivers, vehicles



# 날짜 추출 (CSV 기반)
def load_dates():
    df = pd.read_csv("data/dynamic_supply_chain_logistics_dataset.csv")

    dates = sorted(pd.to_datetime(df["timestamp"]).dt.date.unique())

    if len(dates) == 0:
        raise Exception("❌ 날짜 추출 실패")

    return dates



# 초기 매칭 (첫날)
def initial_matching(drivers, vehicles):
    d = drivers.copy()
    v = vehicles.copy()

    random.shuffle(d)
    random.shuffle(v)

    n = min(len(d), len(v))

    return {d[i]: v[i] for i in range(n)}  # dict



# 다음날 매칭 (현실성: 일부만 변경)
def mutate_matching(prev_map, drivers, vehicles, change_rate=0.2):
    new_map = prev_map.copy()

    driver_list = list(new_map.keys())
    change_count = int(len(driver_list) * change_rate)

    # 변경할 driver 선택
    to_change = random.sample(driver_list, change_count)

    # 현재 사용 중 차량 제외
    used_vehicles = set(new_map.values())
    available_vehicles = list(set(vehicles) - used_vehicles)

    random.shuffle(available_vehicles)

    for d in to_change:
        if available_vehicles:
            new_map[d] = available_vehicles.pop()

    return new_map



# assignment 생성
def generate_assignments():
    drivers, vehicles = load_ids()
    dates = load_dates()

    assignments = []

    # Day 1
    current_map = initial_matching(drivers, vehicles)

    for d in dates:
        # 저장
        for driver_id, vehicle_id in current_map.items():
            assignments.append((driver_id, vehicle_id, d))

        # 다음날 매칭 (80% 유지)
        current_map = mutate_matching(current_map, drivers, vehicles)

    return assignments



# INSERT
def insert_assignments(assignments):
    query = """
        INSERT INTO driver_vehicle_assignment (
            driver_id,
            vehicle_id,
            assigned_date
        )
        VALUES %s
        ON CONFLICT DO NOTHING
    """

    chunk_size = 10000

    for i in range(0, len(assignments), chunk_size):
        chunk = assignments[i:i + chunk_size]
        execute_values(cur, query, chunk)

        print(f"{i + len(chunk)} / {len(assignments)} inserted")



# 실행
if __name__ == "__main__":
    assignments = generate_assignments()

    print(f"{len(assignments)}개 assignment 생성")

    insert_assignments(assignments)

    conn.commit()
    cur.close()
    conn.close()

    print("Assignment seeded successfully")