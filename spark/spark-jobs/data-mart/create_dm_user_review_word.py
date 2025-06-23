from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, count as count_, avg as avg_
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder.appName("create_dm_user_review_word").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

review_df = spark.read.jdbc(mysql_url, "review", properties=mysql_props)

review_words_df = review_df.select(
    col("user_id"),
    explode(split(lower(col("content")), "\\s+")).alias("word")
)

word_filtered_df = review_words_df.filter(col("word") != "")

# 가상의 감성 점수 테이블 (PySpark DataFrame으로 로딩한다고 가정)
# sentiment_dict = {"맛있다": 1.0, "별로다": -1.0, ...}
# 실제에선 Hive 테이블이든 외부 분석 결과든 Spark DataFrame으로 로드해야 함
sentiment_data = [("맛있다", 1.0), ("별로다", -1.0), ("보통", 0.0)]
sentiment_df = spark.createDataFrame(sentiment_data, ["word", "sentiment_score"])

review_with_sentiment_df = word_filtered_df.join(sentiment_df, on="word", how="inner")

dm_user_review_word_df = review_with_sentiment_df.groupBy("user_id", "word") \
    .agg(
        count_("word").alias("count"),
        avg_("sentiment_score").alias("sentiment_score")
    )

dm_user_review_word_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_review_word",
    mode="overwrite",
    properties=mysql_props
)
