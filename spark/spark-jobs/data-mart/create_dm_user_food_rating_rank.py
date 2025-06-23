from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, col
from pyspark.sql.window import Window
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder \
    .appName("create_dm_user_food_rating_rank") \
        .getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

ratings_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.fact_user_ratings",
    properties=mysql_props
)

food_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.dim_food",
    properties=mysql_props
)

rating_with_food_df = ratings_df.join(food_df, on="food_id", how="inner")

best_window = Window.partitionBy("user_id").orderBy(col("food_score").desc())
best_df = rating_with_food_df.withColumn("rank_order", row_number().over(best_window)) \
    .filter(col("rank_order") <= 5) \
    .withColumn("score_type", col("score_type").cast("string")).withColumn("score_type", lit("best")) \
    .select("user_id", "food_name", "food_score", "rank_order", "score_type")

worst_window = Window.partitionBy("user_id").orderBy(col("food_score").asc())
worst_df = rating_with_food_df.withColumn("rank_order", row_number().over(worst_window)) \
    .filter(col("rank_order") <= 5) \
    .withColumn("score_type", col("score_type").cast("string")).withColumn("score_type", lit("worst")) \
    .select("user_id", "food_name", "food_score", "rank_order", "score_type")

dm_user_food_rating_rank_df = best_df.unionByName(worst_df)

dm_user_food_rating_rank_df.write.jdbc(
    url=mysql_url,
    table="dm_user_food_rating_rank",
    mode="overwrite",
    properties=mysql_props
)
