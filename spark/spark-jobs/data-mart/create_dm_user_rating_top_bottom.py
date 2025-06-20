from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import col, row_number, asc, desc, first
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties


spark = SparkSession.builder \
    .appName("create_dm_user_rating_top_bottom") \
    .getOrCreate()


mysql_url = get_mysql_jdbc_url()
mysql_properties = get_mysql_jdbc_properties()

fact_df = spark.read.jdbc(
    url=mysql_url, 
    table="fact_user_ratings", 
    properties=mysql_properties
)

food_df = spark.read.jdbc(
    url=mysql_url, 
    table="dim_food", 
    properties=mysql_properties
)

ratings = fact_df.join(food_df, on="food_id").select("user_id", "food_name", col("food_score").alias("score"))


window_best = Window.partitionBy("user_id").orderBy(desc("score"), asc("food_name"))
best_df = ratings.withColumn("rank", row_number().over(window_best)) \
    .filter(col("rank") <= 5)

window_worst = Window.partitionBy("user_id").orderBy(asc("score"), asc("food_name"))
worst_df = ratings.withColumn("rank", row_number().over(window_worst)) \
    .filter(col("rank") <= 5)


def pivot_top_bottom(df, prefix):
    df = df.withColumn("food_col", col("rank").cast("string"))
    pivot_values = [str(i) for i in range(1, 6)]

    agg_exprs = []
    for i in range(1, 6):
        agg_exprs.append(first("food_name").alias(f"{prefix}_food{i}_name"))
        agg_exprs.append(first("score").alias(f"{prefix}_food{i}_score"))

    return df.select("user_id", "food_col", "food_name", "score") \
        .groupBy("user_id") \
        .pivot("food_col", pivot_values) \
        .agg(*agg_exprs)

best_pivot = pivot_top_bottom(best_df, "best")
worst_pivot = pivot_top_bottom(worst_df, "worst")

user_rating_top_bottom_df = best_pivot.join(worst_pivot, on="user_id", how="outer")

user_rating_top_bottom_df.write.jdbc(
    url=mysql_url,
    table="dm_user_rating_top_bottom",
    mode="overwrite",
    properties=mysql_properties
)
