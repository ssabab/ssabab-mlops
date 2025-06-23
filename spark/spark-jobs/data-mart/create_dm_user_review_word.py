from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, count as count_, avg as avg_, udf
from pyspark.sql.types import FloatType
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties
from model.sentiment_model import predict_sentiment

spark = SparkSession.builder.appName("create_dm_user_review_word").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

comment_df = spark.read.jdbc(mysql_url, "ssabab_dw.fact_user_comments", properties=mysql_props)

review_words_df = comment_df.select(
    col("user_id"),
    explode(split(lower(col("menu_comment")), "\\s+")).alias("word")
)

word_filtered_df = review_words_df.filter(col("word") != "")

predict_udf = udf(lambda word: float(predict_sentiment(word)), FloatType())

review_with_sentiment_df = word_filtered_df.withColumn("sentiment_score", predict_udf(col("word")))

dm_user_review_word_df = review_with_sentiment_df.groupBy("user_id", "word").agg(
    count_("word").alias("count"),
    avg_("sentiment_score").alias("sentiment_score")
)

dm_user_review_word_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_review_word",
    mode="overwrite",
    properties=mysql_props
)
