from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from utils.db import get_db_connection 

def create_spark_session():
    return SparkSession.builder \
        .appName("Class Engagement Monthly") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    
    target_year = 2024
    target_month = 5
    print(f"[INFO] Fetching data for {target_year}-{target_month}")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            # 테이블 재생성 (DROP + CREATE)
            cur.execute("DROP TABLE IF EXISTS class_engagement_monthly")
            cur.execute("""
                CREATE TABLE class_engagement_monthly(
                    month date NOT NULL,
                    class text NOT NULL,
                    generation text NOT NULL,
                    review_count integer DEFAULT 0,
                    PRIMARY KEY(month, class, generation)
                )
            """)
            conn.commit()

            # 데이터 조회
            cur.execute("""
                SELECT * FROM food_review 
                WHERE EXTRACT(MONTH FROM timestamp) = %s 
                  AND EXTRACT(YEAR FROM timestamp) = %s
            """, (target_month, target_year))
            food_reviews = cur.fetchall()

            cur.execute("SELECT * FROM account")
            accounts = cur.fetchall()

    print(f"[INFO] Loaded {len(food_reviews)} food reviews")
    print(f"[INFO] Loaded {len(accounts)} accounts")

    # PySpark DataFrame 변환
    food_review_df = spark.createDataFrame(food_reviews)
    account_df = spark.createDataFrame(accounts)

    # user_id 기준 조인 후 월간 리뷰 집계
    joined_df = food_review_df.join(account_df, "user_id")

    engagement_df = joined_df.groupBy("class", "generation") \
        .agg(count("*").alias("review_count"))

    results = engagement_df.collect()

    print(f"[INFO] Writing {len(results)} rows to class_engagement_monthly")

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for row in results:
                cur.execute("""
                    INSERT INTO class_engagement_monthly 
                    (month, class, generation, review_count)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (month, class, generation) 
                    DO UPDATE SET review_count = EXCLUDED.review_count
                """, (
                    f"{target_year}-{target_month}-01",
                    row['class'],
                    row['generation'],
                    row['review_count']
                ))
            conn.commit()

    print("[INFO] Ingestion complete.")

if __name__ == "__main__":
    main()
