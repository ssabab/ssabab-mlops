from pyspark.sql import SparkSession
from pyspark.sql.functions import count as count_, col, first
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder.appName("create_dm_user_tag_stats").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

ratings_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.fact_user_food_feedback",
    properties=mysql_props
)

menu_food_df_raw = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.dim_menu_food_combined",
    properties=mysql_props
)

menu_food_df = menu_food_df_raw.groupBy("food_id").agg(first("tag_name").alias("tag_name"))

ratings_with_tags_df = ratings_df.join(menu_food_df, on="food_id", how="inner") \
    .select("user_id", "tag_name")


tag_stats_df = ratings_with_tags_df.groupBy("user_id", "tag_name") \
    .agg(count_("*").alias("count")) \
    .select(
        col("user_id"),
        col("tag_name").alias("tag"),
        col("count")
    )

tag_stats_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_tag_stats",
    mode="overwrite",
    properties=mysql_props
)
