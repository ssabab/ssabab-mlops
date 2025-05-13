from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, month, year, when
from datetime import datetime
import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_db_connection():
    DB_NAME = 'postgres'
    DB_USER = 'ssafyuser'
    DB_PASSWORD = 'ssafy'
    DB_HOST = 'host.docker.internal'
    
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port='5432'
    )

def create_spark_session():
    return SparkSession.builder \
        .appName("Class Engagement Monthly") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    
    # 2024년 5월 데이터 사용
    target_year = 2024
    target_month = 5
    
    print(f"Fetching data for {target_year}-{target_month}")
    
    # psycopg2를 사용하여 데이터 읽기
    with get_db_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # 테이블이 있으면 삭제하고 다시 생성
            cur.execute("DROP TABLE IF EXISTS class_engagement_monthly")
            cur.execute("""
                CREATE TABLE class_engagement_monthly(
                    month date NOT NULL,
                    class text NOT NULL,
                    generation text NOT NULL,
                    review_count integer DEFAULT 0,
                    PRIMARY KEY(month,class,generation)
                )
            """)
            conn.commit()
            
            # food_review 데이터 읽기
            cur.execute("""
                SELECT * FROM food_review 
                WHERE EXTRACT(MONTH FROM timestamp) = %s 
                AND EXTRACT(YEAR FROM timestamp) = %s
            """, (target_month, target_year))
            food_reviews = cur.fetchall()
            print(f"Found {len(food_reviews)} food reviews")
            
            # account 데이터 읽기
            cur.execute("SELECT * FROM account")
            accounts = cur.fetchall()
            print(f"Found {len(accounts)} accounts")
    
    # DataFrame으로 변환
    food_review_df = spark.createDataFrame(food_reviews)
    account_df = spark.createDataFrame(accounts)
    
    # 리뷰 데이터와 계정 데이터 조인
    joined_df = food_review_df.join(account_df, "user_id")
    
    # 월간 리뷰 수 계산
    engagement_df = joined_df \
        .groupBy("class", "generation") \
        .agg(
            count("*").alias("review_count")
        )
    
    # 결과를 리스트로 변환
    results = engagement_df.collect()
    
    # psycopg2를 사용하여 결과 저장
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for row in results:
                cur.execute("""
                    INSERT INTO class_engagement_monthly 
                    (month, class, generation, review_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (month, class, generation) 
                    DO UPDATE SET
                        review_count = EXCLUDED.review_count
                """, (
                    f"{target_year}-{target_month}-01",
                    row['class'],
                    row['generation'],
                    row['review_count']
                ))
            conn.commit()

if __name__ == "__main__":
    main() 