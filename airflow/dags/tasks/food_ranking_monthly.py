from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, month, year, row_number, desc, asc, lit
from pyspark.sql.window import Window
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
        .appName("Food Ranking Monthly") \
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
            # food_review 데이터 읽기
            cur.execute("""
                SELECT * FROM food_review 
                WHERE EXTRACT(MONTH FROM timestamp) = %s 
                AND EXTRACT(YEAR FROM timestamp) = %s
            """, (target_month, target_year))
            food_reviews = cur.fetchall()
            print(f"Found {len(food_reviews)} food reviews")
            
            # food 데이터 읽기
            cur.execute("SELECT * FROM food")
            foods = cur.fetchall()
            print(f"Found {len(foods)} foods")
    
    # DataFrame으로 변환
    food_review_df = spark.createDataFrame(food_reviews)
    food_df = spark.createDataFrame(foods)
    
    # 리뷰 데이터와 음식 데이터 조인
    joined_df = food_review_df.join(food_df, "food_id")
    
    # 월간 리뷰 통계 계산
    monthly_reviews = joined_df \
        .groupBy("food_id", "food_name") \
        .agg(
            count("*").alias("review_count"),
            avg("food_score").alias("avg_rating")
        )
    
    print("Monthly reviews statistics:")
    monthly_reviews.show()
    
    # 평점 기준으로 정렬
    window_spec = Window.orderBy(desc("avg_rating"), desc("review_count"))
    window_spec_bottom = Window.orderBy(asc("avg_rating"), asc("review_count"))
    
    # 모든 음식에 대해 best와 worst 랭킹 계산
    ranked_foods = monthly_reviews \
        .withColumn("best_rank", row_number().over(window_spec)) \
        .withColumn("worst_rank", row_number().over(window_spec_bottom))
    
    # best와 worst 각각 TOP 5 선택
    best_foods = ranked_foods \
        .filter(col("best_rank") <= 5) \
        .withColumn("ranking_type", lit("best"))
    
    worst_foods = ranked_foods \
        .filter(col("worst_rank") <= 5) \
        .withColumn("ranking_type", lit("worst"))
    
    print("Best foods:")
    best_foods.show()
    print("Worst foods:")
    worst_foods.show()
    
    # 결과 합치기
    result_df = best_foods.unionAll(worst_foods)
    
    # 결과를 리스트로 변환
    results = result_df.collect()
    print(f"Total results to insert: {len(results)}")
    
    # psycopg2를 사용하여 결과 저장
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for row in results:
                print(f"Inserting row: {row}")
                try:
                    cur.execute("""
                        INSERT INTO food_ranking_monthly 
                        (month, food_id, food_name, avg_score, rank_type, rank)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (month, food_id, rank_type) 
                        DO UPDATE SET
                            food_name = EXCLUDED.food_name,
                            avg_score = EXCLUDED.avg_score,
                            rank = EXCLUDED.rank
                    """, (
                        f"{target_year}-{target_month}-01",
                        row['food_id'],
                        row['food_name'],
                        row['avg_rating'],
                        row['ranking_type'],
                        row['best_rank'] if row['ranking_type'] == 'best' else row['worst_rank']
                    ))
                    print("Row inserted successfully")
                except Exception as e:
                    print(f"Error inserting row: {e}")
            conn.commit()
            print("All rows committed to database")

if __name__ == "__main__":
    main() 