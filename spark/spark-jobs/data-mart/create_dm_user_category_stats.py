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

menu_food_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.dim_menu_food_combined",
    properties=mysql_props
)

ratings_with_category_df = ratings_df.join(menu_food_df, on="food_id", how="inner") \
    .select("user_id", "category_name")

category_stats_df = ratings_with_category_df.groupBy("user_id", "category_name") \
    .agg(count_("*").alias("count")) \
    .select(
        col("user_id"),
        col("category_name").alias("category"),
        col("count")
    )

category_stats_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_category_stats",
    mode="overwrite",
    properties=mysql_props
)