from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max, avg, stddev, variance, expr, round
import sys
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

def main():
    if len(sys.argv) != 2:
        print("Usage: create_dm_monthly_statistic.py <YYYY-MM>")
        sys.exit(1)
    target_month = sys.argv[1]

    spark = SparkSession.builder.appName("Monthly Statistic").getOrCreate()
    mysql_url = get_mysql_jdbc_url()
    mysql_props = get_mysql_jdbc_properties()

    # food_review에서 해당 월 데이터 가져오기
    food_review_df = spark.read.jdbc(
        url=mysql_url,
        table="ssabab.food_review",
        properties=mysql_props
    ).filter(col("timestamp").substr(1, 7) == target_month)

    # 통계 계산
    stats_df = food_review_df.agg(
        spark_min("food_score").alias("min"),
        spark_max("food_score").alias("max"),
        avg("food_score").alias("avg"),
        expr("percentile_approx(food_score, 0.25)").alias("q1"),
        expr("percentile_approx(food_score, 0.75)").alias("q3"),
        variance("food_score").alias("variance"),
        stddev("food_score").alias("stdev")
    )

    # 컬럼별로 타입 변환
    stats_df = stats_df \
        .withColumn("min", col("min").cast("int")) \
        .withColumn("max", col("max").cast("int")) \
        .withColumn("avg", round(col("avg"), 2).cast("decimal(3,2)")) \
        .withColumn("q1", col("q1").cast("int")) \
        .withColumn("q3", col("q3").cast("int")) \
        .withColumn("variance", round(col("variance"), 2).cast("decimal(3,2)")) \
        .withColumn("stdev", round(col("stdev"), 2).cast("decimal(3,2)"))

    # 결과 저장
    stats_df.write.jdbc(
        url=mysql_url,
        table="ssabab_dm.dm_monthly_statistic",
        mode="overwrite",
        properties=mysql_props
    )

if __name__ == "__main__":
    main()
