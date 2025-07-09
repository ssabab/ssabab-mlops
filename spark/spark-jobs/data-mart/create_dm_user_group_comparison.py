from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, countDistinct, col, floor, lit, first
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder.appName("create_dm_user_group_comparison").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

ratings_df = spark.read.jdbc(mysql_url, "ssabab_dw.fact_user_food_feedback", properties=mysql_props)
food_df_raw = spark.read.jdbc(mysql_url, "ssabab_dw.dim_menu_food_combined", properties=mysql_props)
user_df = spark.read.jdbc(mysql_url, "ssabab_dw.dim_user", properties=mysql_props)
food_df = food_df_raw.groupBy("food_id").agg(first("category_name").alias("category_name"))

joined_df = ratings_df.join(food_df, "food_id").join(user_df, "user_id")
joined_df = joined_df.withColumn("age_group", floor((2025 - col("birth_year")) / 10))

user_stats_df = joined_df.groupBy("user_id").agg(
    avg("food_score").alias("user_avg_score"),
    countDistinct("category_name").alias("user_diversity_score")
)

overall_stats = joined_df.groupBy().agg(
    avg("food_score").alias("group_avg_score"),
    countDistinct("category_name").alias("group_diversity_score")
).withColumn("group_type", lit("all")).withColumn("group_value", lit("all"))

gender_stats = joined_df.groupBy("gender").agg(
    avg("food_score").alias("group_avg_score"),
    countDistinct("category_name").alias("group_diversity_score")
).withColumn("group_type", lit("gender")).withColumnRenamed("gender", "group_value")

age_stats = joined_df.groupBy("age_group").agg(
    avg("food_score").alias("group_avg_score"),
    countDistinct("category_name").alias("group_diversity_score")
).withColumn("group_type", lit("age")).withColumnRenamed("age_group", "group_value")

user_info = user_df.withColumn("age_group", floor((2025 - col("birth_year")) / 10)) \
                   .select("user_id", "gender", "age_group")

dm_all = user_stats_df.crossJoin(
    overall_stats.select("group_type", "group_value", "group_avg_score", "group_diversity_score")
).select(
    "user_id", "group_type", "group_value", "user_avg_score", "group_avg_score",
    "user_diversity_score", "group_diversity_score"
)

dm_gender = user_stats_df.join(user_info, "user_id").join(
    gender_stats,
    (col("gender") == col("group_value")) & (col("group_type") == lit("gender"))
).select(
    "user_id", "group_type", "group_value", "user_avg_score", "group_avg_score",
    "user_diversity_score", "group_diversity_score"
)

dm_age = user_stats_df.join(user_info, "user_id").join(
    age_stats,
    (col("age_group") == col("group_value")) & (col("group_type") == lit("age"))
).select(
    "user_id", "group_type", "group_value", "user_avg_score", "group_avg_score",
    "user_diversity_score", "group_diversity_score"
)

dm_user_diversity_df = dm_all.unionByName(dm_gender).unionByName(dm_age)

dm_user_diversity_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_group_comparison",
    mode="overwrite",
    properties=mysql_props
)
