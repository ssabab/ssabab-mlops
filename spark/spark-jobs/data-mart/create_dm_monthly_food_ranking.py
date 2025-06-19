from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, avg, row_number, lit, count
from pyspark.sql.types import *
import sys
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

def main():
    if len(sys.argv) != 2:
        print("Usage: create_dm_monthly_food_ranking.py <YYYY-MM>")
        sys.exit(1)
    target_month = sys.argv[1]

    spark = SparkSession.builder.appName("Monthly Food Ranking").getOrCreate()
    mysql_url = get_mysql_jdbc_url()
    mysql_props = get_mysql_jdbc_properties()

    food_review_df = spark.read.jdbc(
        url=mysql_url,
        table="ssabab.food_review",
        properties=mysql_props
    ).filter(col("timestamp").substr(1, 7) == target_month)

    food_df = spark.read.jdbc(
        url=mysql_url,
        table="ssabab.food",
        properties=mysql_props
    )

    df = food_review_df.join(food_df, "food_id") \
        .select("food_id", "food_name", "food_score", "timestamp")

    avg_score_df = df.groupBy("food_id", "food_name") \
        .agg(
            avg("food_score").alias("avg_score"),
            count("*").alias("count")
        )

    window_best = Window.orderBy(col("avg_score").desc())
    window_worst = Window.orderBy(col("avg_score").asc())

    best_df = avg_score_df.withColumn("rank", row_number().over(window_best)) \
        .filter(col("rank") <= 5) \
        .withColumn("rank_type", lit("best"))

    worst_df = avg_score_df.withColumn("rank", row_number().over(window_worst)) \
        .filter(col("rank") <= 5) \
        .withColumn("rank_type", lit("worst"))

    result_df = best_df.unionByName(worst_df) \
        .select(
            "food_id",
            "food_name",
            col("avg_score").cast("decimal(3,2)").alias("avg_score"),
            "rank_type",
            "rank",
            "count"
        )

    result_df.write.jdbc(
        url=mysql_url,
        table="ssabab_dm.monthly_food_ranking",
        mode="overwrite",
        properties=mysql_props
    )

if __name__ == "__main__":
    main()
