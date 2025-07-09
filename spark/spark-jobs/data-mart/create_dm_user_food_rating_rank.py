from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col, lit, avg, first
from pyspark.sql.window import Window
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder.appName("create_dm_user_food_rating_rank").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

ratings_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.fact_user_food_feedback",
    properties=mysql_props
)

food_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.dim_menu_food_combined",
    properties=mysql_props
)

food_df_dedup = food_df.groupBy("food_id").agg(first("food_name").alias("food_name"))

avg_score_df = ratings_df.groupBy("user_id", "food_id") \
    .agg(avg("food_score").alias("avg_score"))

rating_with_food_df = avg_score_df.join(food_df_dedup, on="food_id", how="left")


best_window = Window.partitionBy("user_id").orderBy(col("avg_score").desc())
best_df = rating_with_food_df.withColumn("rank_order", row_number().over(best_window)) \
    .filter(col("rank_order") <= 5) \
    .withColumn("score_type", lit("best")) \
    .select("user_id", "food_name", col("avg_score").alias("food_score"), "rank_order", "score_type")


worst_window = Window.partitionBy("user_id").orderBy(col("avg_score").asc())
worst_df = rating_with_food_df.withColumn("rank_order", row_number().over(worst_window)) \
    .filter(col("rank_order") <= 5) \
    .withColumn("score_type", lit("worst")) \
    .select("user_id", "food_name", col("avg_score").alias("food_score"), "rank_order", "score_type")

dm_user_food_rating_rank_df = best_df.unionByName(worst_df)

dm_user_food_rating_rank_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_food_rating_rank",
    mode="overwrite",
    properties=mysql_props
)
