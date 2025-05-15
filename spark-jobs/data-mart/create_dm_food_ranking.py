from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, concat, lit, avg, row_number
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
from datetime import datetime
import sys
import os
from common.env_loader import load_env
from pyspark.sql.window import Window

load_env()

def create_spark_session():
    return SparkSession.builder \
        .appName("Food Ranking Monthly") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.2.jar") \
        .getOrCreate()

def get_db_connection():
    import psycopg2
    from psycopg2.extras import RealDictCursor
    return psycopg2.connect(
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        cursor_factory=RealDictCursor
    )

def main():
    # 커맨드 라인 인자로부터 월 받기
    if len(sys.argv) != 2:
        print("Usage: create_dm_food_ranking.py <month>")
        sys.exit(1)
    
    target_month = int(sys.argv[1])
    if not 1 <= target_month <= 12:
        print("Error: Month must be between 1 and 12")
        sys.exit(1)
    
    print(f"[INFO] Fetching data for month {target_month}")

    spark = create_spark_session()

    # PostgreSQL에서 데이터 읽기
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT fr.*, f.food_name 
                FROM food_review fr
                JOIN food f ON fr.food_id = f.food_id
                WHERE EXTRACT(MONTH FROM fr.create_at) = %s
            """, (target_month,))
            food_reviews = cur.fetchall()
    
    # 스키마 정의
    food_review_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("food_id", IntegerType(), False),
        StructField("food_score", IntegerType(), False),
        StructField("create_at", TimestampType(), False),
        StructField("food_name", StringType(), False)
    ])
    
    food_review_df = spark.createDataFrame(food_reviews, schema=food_review_schema)
    
    # 평균 점수 계산
    avg_scores_df = food_review_df.groupBy("food_id", "food_name") \
        .agg(avg("food_score").alias("avg_score"))
    
    # 전체 음식 수 확인
    total_foods = avg_scores_df.count()
    if total_foods < 10:
        print(f"Error: Not enough foods (total: {total_foods}, required: 10)")
        sys.exit(1)
    
    # 상위 5개 순위 계산
    best_ranked_df = avg_scores_df.orderBy(col("avg_score").desc()).limit(5)
    best_ranked_df = best_ranked_df.withColumn("rank", row_number().over(
        Window.orderBy(col("avg_score").desc())
    ))
    
    # 하위 5개 순위 계산
    worst_ranked_df = avg_scores_df.orderBy(col("avg_score").asc()).limit(5)
    worst_ranked_df = worst_ranked_df.withColumn("rank", row_number().over(
        Window.orderBy(col("avg_score").asc())
    ))
    
    best_results = best_ranked_df.collect()
    worst_results = worst_ranked_df.collect()

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 상위 5개 저장
            for row in best_results:
                cur.execute("""
                    INSERT INTO dm_food_ranking_monthly 
                    (month, food_id, food_name, avg_score, rank_type, rank)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (month, food_id, rank_type) 
                    DO UPDATE SET 
                        food_name = EXCLUDED.food_name,
                        avg_score = EXCLUDED.avg_score,
                        rank = EXCLUDED.rank
                """, (
                    target_month,
                    row['food_id'],
                    row['food_name'],
                    float(row['avg_score']),
                    'best',
                    row['rank']
                ))
            
            # 하위 5개 저장
            for row in worst_results:
                cur.execute("""
                    INSERT INTO dm_food_ranking_monthly 
                    (month, food_id, food_name, avg_score, rank_type, rank)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (month, food_id, rank_type) 
                    DO UPDATE SET 
                        food_name = EXCLUDED.food_name,
                        avg_score = EXCLUDED.avg_score,
                        rank = EXCLUDED.rank
                """, (
                    target_month,
                    row['food_id'],
                    row['food_name'],
                    float(row['avg_score']),
                    'worst',
                    row['rank']
                ))
            conn.commit()

if __name__ == "__main__":
    main()
