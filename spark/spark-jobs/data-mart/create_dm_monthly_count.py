from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, countDistinct, sum as spark_sum, lit, coalesce
import sys
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

def main():
    if len(sys.argv) != 2:
        print("Usage: create_dm_monthly_count.py <YYYY-MM>")
        sys.exit(1)
    target_month = sys.argv[1]

    spark = SparkSession.builder.appName("Monthly Count").getOrCreate()
    mysql_url = get_mysql_jdbc_url()
    mysql_props = get_mysql_jdbc_properties()

    # 현재 월 평가자 수 계산
    # menu_review에서 해당 월에 리뷰를 남긴 유니크 user_id 수
    menu_review_df = spark.read.jdbc(
        url=mysql_url,
        table="ssabab.menu_review",
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
     .filter(col("date").substr(1, 7) == target_month)

    # 현재 월 평가자 수 계산
    # menu_review에서 해당 월에 리뷰를 남긴 개수
    menu_review_count = menu_review_df.count()

    # pre_vote에서 해당 월에 투표한 개수
    pre_vote_count = pre_vote_df.count()

    # evaluator: 해당 월 pre_vote + menu_review 개수
    evaluator = menu_review_count + pre_vote_count

    # 누적 평가자 수 계산 (전체 기간)
    all_menu_review_df = spark.read.jdbc(
        url=mysql_url,
        table="ssabab.menu_review",
        properties=mysql_props
    )
    
    all_pre_vote_df = spark.read.jdbc(
        url=mysql_url,
        table="ssabab.pre_vote",
        properties=mysql_props
    )

    cumulative_evaluators = all_menu_review_df.select("user_id").union(
        all_pre_vote_df.select("user_id")
    ).distinct().count()

    # 이전 월 데이터가 있다면 차이값 및 누적 계산
    try:
        prev_month_data = spark.read.jdbc(
            url=mysql_url,
            table="ssabab_dm.monthly_count",
            properties=mysql_props
        ).limit(1).collect()  # 정렬 없이 한 줄만
        print("prev_month_data:", prev_month_data)
        if prev_month_data:
            prev_evaluator = prev_month_data[0]["evaluator"]
            prev_cumulative = prev_month_data[0]["cumulative"]
            cumulative = prev_cumulative + evaluator
            difference = cumulative - prev_cumulative
        else:
            difference = evaluator
            cumulative = evaluator
    except Exception as e:
        print("Exception:", e)  
        difference = evaluator
        cumulative = evaluator

    # 결과 데이터프레임 생성
    result_df = spark.createDataFrame([
        (evaluator, cumulative, difference)
    ], ["evaluator", "cumulative", "difference"])

    # 결과 저장
    result_df.write.jdbc(
        url=mysql_url,
        table="ssabab_dm.dm_monthly_count",
        mode="overwrite",
        properties=mysql_props
    )

if __name__ == "__main__":
    main()
