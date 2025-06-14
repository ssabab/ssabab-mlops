from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, concat, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
from datetime import datetime
import sys
import os
from utils.db import *
from common.env_loader import load_env

load_env()

def create_spark_session():
    return SparkSession.builder \
        .appName("Class Engagement Monthly") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.2.jar") \
        .getOrCreate()

def main():
    # 커맨드 라인 인자로부터 월 받기
    if len(sys.argv) != 2:
        print("Usage: create_dm_class_engagement.py <month>")
        sys.exit(1)
    
    target_month = int(sys.argv[1])
    if not 1 <= target_month <= 12:
        print("Error: Month must be between 1 and 12")
        sys.exit(1)
    
    print(f"[INFO] Fetching data for month {target_month}")

    spark = create_spark_session()

    # PostgreSQL에서 데이터 읽기
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT * FROM food_review 
                WHERE EXTRACT(MONTH FROM create_at) = %s
            """, (target_month,))
            food_reviews = cur.fetchall()

            cur.execute("""
                SELECT 
                    user_id, password, username, email, profile_image_url,
                    role, created_at, updated_at, active, ord_num,
                    gender, birth_year, class
                FROM account
            """)
            accounts = cur.fetchall()
    
    # 스키마 정의
    account_schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("password", StringType(), False),
        StructField("username", StringType(), False),
        StructField("email", StringType(), False),
        StructField("profile_image_url", StringType(), True),
        StructField("role", StringType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False),
        StructField("active", BooleanType(), False),
        StructField("ord_num", IntegerType(), False),
        StructField("gender", BooleanType(), False),
        StructField("birth_year", IntegerType(), False),
        StructField("class", IntegerType(), False)
    ])

    food_review_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("user_id", IntegerType(), False),
        StructField("food_id", IntegerType(), False),
        StructField("food_score", IntegerType(), False),
        StructField("create_at", TimestampType(), False)
    ])
    
    food_review_df = spark.createDataFrame(food_reviews, schema=food_review_schema)
    account_df = spark.createDataFrame(accounts, schema=account_schema)
    
    # ord_num을 generation으로 변환
    account_df = account_df.withColumn("generation", 
        concat(lit(""), col("ord_num"), lit("기"))
    )
    
    joined_df = food_review_df.join(account_df, "user_id")
    
    engagement_df = joined_df.groupBy("class", "generation") \
        .agg(count("*").alias("review_count"))
    
    results = engagement_df.collect()

    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            for row in results:
                cur.execute("""
                    INSERT INTO dm_class_engagement_monthly 
                    (month, class, generation, review_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (month, class, generation) 
                    DO UPDATE SET review_count = EXCLUDED.review_count
                """, (
                    target_month,  # 정수형으로 월만 저장
                    row['class'],
                    row['generation'],
                    row['review_count']
                ))
            conn.commit()

if __name__ == "__main__":
    main()
