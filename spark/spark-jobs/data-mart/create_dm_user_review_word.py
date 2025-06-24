from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, count as count_
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder.appName("create_dm_user_review_word").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

comment_df = spark.read.jdbc(mysql_url, "ssabab_dw.fact_user_comments", properties=mysql_props)

review_words_df = comment_df.select(
    col("user_id"),
    explode(split(lower(col("menu_comment")), "\\s+")).alias("word")
)

word_filtered_df = review_words_df.filter(col("word") != "")

dm_user_review_word_df = word_filtered_df.groupBy("user_id", "word").agg(
    count_("word").alias("count")
)

dm_user_review_word_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_review_word",
    mode="overwrite",
    properties=mysql_props
)
