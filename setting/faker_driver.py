from faker import Faker
import random
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
import os


# ENV 로드
load_dotenv()


# Faker 설정
fake = Faker("ko_KR")
random.seed(42)
fake.seed_instance(42)


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


# Driver 데이터 생성
def generate_drivers(n=300):
    drivers = []

    for i in range(n):
        experience = random.randint(1, 10)

        base = 0.6 + experience * 0.03   # 경험 영향 
        noise = random.uniform(-0.15, 0.15)  # 개인 편차

        score = round(min(max(base + noise, 0), 1), 2)

        driver = (
            f"DRV_{i}",                         # driver_id
            fake.name(),                       # driver_name
            experience,                        # experience_years
            score                               # driver_score
        )

        drivers.append(driver)

    return drivers



# DB Insert
def insert_drivers(drivers):
    query = """
        INSERT INTO driver (
            driver_id,
            driver_name,
            experience_years,
            driver_score
        )
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (driver_id) DO NOTHING
    """

    execute_batch(cur, query, drivers)



# 실행
if __name__ == "__main__":
    drivers = generate_drivers(300)

    print(f"{len(drivers)}명 생성 완료")

    insert_drivers(drivers)

    conn.commit()
    cur.close()
    conn.close()

    print("Drivers seeded successfully")