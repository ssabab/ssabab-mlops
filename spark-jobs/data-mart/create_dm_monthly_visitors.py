from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, countDistinct, sum as spark_sum
import sys
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

def main():
    if len(sys.argv) != 2:
        print("Usage: create_dm_monthly_visitors.py <YYYY-MM>")
        sys.exit(1)
    target_month = sys.argv[1]

    spark = SparkSession.builder.appName("Monthly Visitors").getOrCreate()
    mysql_url = get_mysql_jdbc_url()
    mysql_props = get_mysql_jdbc_properties()

    # menu_review에서 해당 월에 리뷰를 남긴 user_id, 날짜 추출
    menu_review_df = spark.read.jdbc(
        url=mysql_url,
        table="menu_review",
        properties=mysql_props
    ).filter(col("timestamp").substr(1, 7) == target_month) \
     .select(to_date(col("timestamp")).alias("date"), col("user_id")).distinct()

    # pre_vote에서 해당 월에 투표한 user_id, 날짜 추출
    pre_vote_df = spark.read.jdbc(
        url=mysql_url,
        table="pre_vote",
        properties=mysql_props
    ).filter(col("date").substr(1, 7) == target_month) \
     .select(to_date(col("date")).alias("date"), col("user_id")).distinct()

    # 두 집합을 합치고 중복 제거 (일별 유니크)
    daily_users_df = menu_review_df.union(pre_vote_df).distinct()

    # 날짜별 유니크 유저 수 집계
    daily_count_df = daily_users_df.groupBy("date").agg(countDistinct("user_id").alias("daily_user_count"))

    # 월별 합계
    monthly_sum_df = daily_count_df.agg(spark_sum("daily_user_count").alias("monthly_user_sum"))

    # 결과 저장
    monthly_sum_df.write.jdbc(
        url=mysql_url,
        table="dm_monthly_visitors",
        mode="overwrite",
        properties=mysql_props
    )

if __name__ == "__main__":
    main()
