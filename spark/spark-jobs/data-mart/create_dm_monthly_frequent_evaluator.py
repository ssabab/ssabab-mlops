from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, count, max as spark_max, row_number, expr, coalesce, lit
import sys
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

def main():
    if len(sys.argv) != 2:
        print("Usage: create_dm_monthly_frequent_evaluator.py <YYYY-MM>")
        sys.exit(1)
    target_month = sys.argv[1]

    spark = SparkSession.builder.appName("Monthly Frequent Evaluator").getOrCreate()
    mysql_url = get_mysql_jdbc_url()
    mysql_props = get_mysql_jdbc_properties()

    # food_review에서 해당 월 데이터 가져오기
    food_review_df = spark.read.jdbc(
        url=mysql_url,
        table="ssabab.food_review",
        properties=mysql_props
    ).filter(col("timestamp").substr(1, 7) == target_month)

    # menu 테이블에서 날짜 정보 가져오기
    menu_df = spark.read.jdbc(
        url=mysql_url,
        table="ssabab.menu",
        properties=mysql_props
    ).select(col("menu_id"), col("date"))

    # pre_vote와 menu를 조인하여 해당 월 데이터 가져오기
    pre_vote_df = spark.read.jdbc(
        url=mysql_url,
        table="ssabab.pre_vote",
        properties=mysql_props
    ).join(menu_df, "menu_id") \
     .filter(col("date").substr(1, 7) == target_month) \
     .select("user_id", "date")

    # account 테이블에서 사용자 정보 가져오기
    account_df = spark.read.jdbc(
        url=mysql_url,
        table="ssabab.account",
        properties=mysql_props
    ).select("user_id", col("username").alias("name"))

    # food_review에서 사용자별 평가 횟수와 최근 평가일 계산
    food_review_stats = food_review_df.groupBy("user_id") \
        .agg(
            count("*").alias("food_review_count"),
            spark_max("timestamp").alias("last_food_review")
        )

    # pre_vote에서 사용자별 평가 횟수와 최근 평가일 계산
    pre_vote_stats = pre_vote_df.groupBy("user_id") \
        .agg(
            count("*").alias("pre_vote_count"),
            spark_max("date").alias("last_pre_vote")
        )

    # 두 테이블을 조인하여 전체 평가 횟수 계산
    user_stats = food_review_stats.join(pre_vote_stats, "user_id", "full_outer") \
        .fillna(0, ["food_review_count", "pre_vote_count"]) \
        .withColumn("total_evaluates", 
                   coalesce(col("food_review_count"), lit(0)) + coalesce(col("pre_vote_count"), lit(0)))

    # 최근 평가일 계산 (food_review와 pre_vote 중 더 최근 것)
    user_stats = user_stats.withColumn(
        "last_evaluate",
        expr("greatest(coalesce(last_food_review, '1900-01-01'), coalesce(last_pre_vote, '1900-01-01'))")
    )

    # account와 조인하여 이름 정보 추가
    result_df = user_stats.join(account_df, "user_id") \
        .select("name", "total_evaluates", "last_evaluate") \
        .withColumnRenamed("total_evaluates", "evaluates")

    # 평가 횟수 기준으로 순위 매기기
    window = Window.orderBy(col("evaluates").desc())
    result_df = result_df.withColumn("rank", row_number().over(window)) \
        .withColumn("last_evaluate", col("last_evaluate").cast("timestamp")) \
        .select("rank", "name", "evaluates", "last_evaluate")

    # 결과 저장
    result_df.write.jdbc(
        url=mysql_url,
        table="ssabab_dm.monthly_frequent_evaluator",
        mode="overwrite",
        properties=mysql_props
    )

if __name__ == "__main__":
    main()
