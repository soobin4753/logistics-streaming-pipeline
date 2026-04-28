from dotenv import load_dotenv
import os
import random
import psycopg2
from psycopg2.extras import execute_batch


# ENV 로드
load_dotenv()


# DB 연결
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


# Vehicle 생성
def generate_vehicles(n=500):
    vehicles = []

    for i in range(n):
        vehicle = (
            f"VEH_{i}",  # vehicle_id
            random.choice(["bike", "van", "truck"])
        )
        vehicles.append(vehicle)

    return vehicles



# Insert
def insert_vehicles(vehicles):
    query = """
        INSERT INTO vehicle (
            vehicle_id,
            vehicle_type
        )
        VALUES (%s, %s)
        ON CONFLICT (vehicle_id) DO NOTHING
    """

    execute_batch(cur, query, vehicles)



# 실행
if __name__ == "__main__":
    vehicles = generate_vehicles(500)

    print(f"{len(vehicles)}대 생성 완료")

    insert_vehicles(vehicles)

    conn.commit()
    cur.close()
    conn.close()

    print("Vehicles seeded successfully")