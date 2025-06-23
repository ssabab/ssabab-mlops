from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder.appName("create_dm_user_summary").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_properties = get_mysql_jdbc_properties()

fact_user_ratings_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.fact_user_ratings",
    properties=mysql_properties
)

fact_user_pre_votes_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.fact_user_votings",
    properties=mysql_properties
)

rating_summary_df = fact_user_ratings_df.groupBy("user_id").agg(
    avg("food_score").alias("avg_score"),
    count("food_score").alias("total_reviews")
)

pre_vote_summary_df = fact_user_pre_votes_df.groupBy("user_id").agg(
    count("*").alias("pre_vote_count")
)

dm_user_summary_df = rating_summary_df.join(
    pre_vote_summary_df, on="user_id", how="left"
).fillna(0)

dm_user_summary_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_summary",
    mode="overwrite",
    properties=mysql_properties
)
