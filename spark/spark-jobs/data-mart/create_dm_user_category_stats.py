from pyspark.sql import SparkSession
from pyspark.sql.functions import count as count_, col, round as round_, sum as sum_
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder.appName("create_dm_user_category_stats").getOrCreate()

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

category_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.dim_category",
    properties=mysql_props
)

ratings_with_category_df = ratings_df.join(food_df, on="food_id", how="inner") \
    .join(category_df, on="category_id", how="inner") \
    .select("user_id", "category_name")

category_count_df = ratings_with_category_df.groupBy("user_id", "category_name") \
    .agg(count_("*").alias("count"))

total_count_df = category_count_df.groupBy("user_id") \
    .agg(sum_("count").alias("total"))

category_stats_df = category_count_df.join(total_count_df, on="user_id", how="inner") \
    .withColumn("ratio", round_(col("count") / col("total"), 4)) \
    .select(
        col("user_id"),
        col("category_name").alias("category"),
        col("count"),
        col("ratio")
    )

category_stats_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_category_stats",
    mode="overwrite",
    properties=mysql_props
)
