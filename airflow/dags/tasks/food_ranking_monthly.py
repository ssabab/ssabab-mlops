from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from datetime import datetime
from utils.db import get_db_connection

def create_spark_session():
    return SparkSession.builder \
        .appName("Class Engagement Monthly") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    
    target_year = 2024
    target_month = 5
    print(f"Fetching data for {target_year}-{target_month}")

    # PostgreSQL에서 데이터 읽기
    with get_db_connection() as conn:
        with conn.cursor() as cur:
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

            cur.execute("""
                SELECT * FROM food_review 
                WHERE EXTRACT(MONTH FROM timestamp) = %s 
                AND EXTRACT(YEAR FROM timestamp) = %s
            """, (target_month, target_year))
            food_reviews = cur.fetchall()

            cur.execute("SELECT * FROM account")
            accounts = cur.fetchall()
    
    food_review_df = spark.createDataFrame(food_reviews)
    account_df = spark.createDataFrame(accounts)
    
    joined_df = food_review_df.join(account_df, "user_id")
    
    engagement_df = joined_df.groupBy("class", "generation") \
        .agg(count("*").alias("review_count"))
    
    results = engagement_df.collect()

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

if __name__ == "__main__":
    main()
