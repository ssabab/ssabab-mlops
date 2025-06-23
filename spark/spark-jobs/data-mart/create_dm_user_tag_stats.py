from pyspark.sql import SparkSession
from pyspark.sql.functions import count as count_, col, round as round_, sum as sum_
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder.appName("create_dm_user_tag_stats").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

fact_user_tags_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.fact_user_tags",
    properties=mysql_props
)

dim_tag_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.dim_tag",
    properties=mysql_props
)

user_tags_with_name_df = fact_user_tags_df.join(
    dim_tag_df,
    on="tag_id",
    how="inner"
).select("user_id", "tag_name")

tag_count_df = user_tags_with_name_df.groupBy("user_id", "tag_name") \
    .agg(count_("*").alias("count"))

total_count_df = tag_count_df.groupBy("user_id") \
    .agg(sum_("count").alias("total"))

tag_stats_df = tag_count_df.join(total_count_df, on="user_id", how="inner") \
    .withColumn("ratio", round_(col("count") / col("total"), 4)) \
    .select(
        col("user_id"),
        col("tag_name").alias("tag"),
        col("count"),
        col("ratio")
    )

tag_stats_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_tag_stats",
    mode="overwrite",
    properties=mysql_props
)
