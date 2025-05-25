import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, collect_list, struct, to_json
from pyspark.sql.types import *
from common.env_loader import load_env

load_env()

mysql_url = f"jdbc:mysql://{os.getenv('MYSQL_HOST')}:3306/{os.getenv('MYSQL_DATABASE')}"
mysql_properties = {
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    "driver": "com.mysql.cj.jdbc.Driver"
}


spark = SparkSession.builder \
    .appName("create_dm_user_category_tag_preference") \
    .getOrCreate()


dim_user_df = spark.read.jdbc(
    url=mysql_url,
    table="dim_user",
    properties=mysql_properties
)

fact_user_scores_df = spark.read.jdbc(
    url=mysql_url,
    table="fact_user_ratings",
    properties=mysql_properties
)

dim_food_df = spark.read.jdbc(
    url=mysql_url,
    table="dim_food",
    properties=mysql_properties
)


joined_df = fact_user_scores_df.join(dim_food_df, on="food_id") \
    .select("user_id", "category", "tag")


category_df = joined_df.groupBy("user_id", "category") \
    .agg(count("*").alias("count")) \
    .withColumn("category_json_struct", struct(col("category"), col("count")))

category_json_df = category_df.groupBy("user_id") \
    .agg(collect_list("category_json_struct").alias("category_json"))


tag_df = joined_df.groupBy("user_id", "tag") \
    .agg(count("*").alias("count")) \
    .withColumn("tag_json_struct", struct(col("tag"), col("count")))

tag_json_df = tag_df.groupBy("user_id") \
    .agg(collect_list("tag_json_struct").alias("tag_json"))


final_df = category_json_df.join(tag_json_df, on="user_id", how="outer")

final_df.write.jdbc(
    url=mysql_url,
    table="dm_user_category_tag_preference",
    mode="overwrite",
    properties=mysql_properties
)
