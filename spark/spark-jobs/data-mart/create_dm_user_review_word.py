from pyspark.sql import SparkSession
from pyspark.sql.functions import count as count_
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder.appName("create_dm_user_category_stats").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

ratings_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.fact_user_ratings",
    properties=mysql_props
)

comments_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.fact_user_comments",
    properties=mysql_props
)

cleaned_df = comments_df.withColumn("cleaned_comment", lower(regexp_replace(col("menu_comment"), "[^ㄱ-ㅎ가-힣a-zA-Z0-9\\s]", "")))

words_df = cleaned_df.withColumn("word", explode(split(col("cleaned_comment"), "\\s+"))) \
                     .filter(col("word") != "")

stopwords = ["너무", "정말", "진짜", "매우", "그리고", "더", "더욱", "그냥", "또", "이건"]
words_filtered_df = words_df.filter(~col("word").isin(stopwords))

word_count_df = words_filtered_df.groupBy("user_id", "word").agg(count_("*").alias("count"))

word_count_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_review_word",
    mode="overwrite",
    properties=mysql_props
)