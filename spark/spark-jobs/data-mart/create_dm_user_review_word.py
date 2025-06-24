from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as sum_
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder.appName("create_dm_user_review_word").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

raw_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.raw_user_review_word",
    properties=mysql_props
)

agg_df = raw_df.groupBy("user_id", "word").agg(
    sum_("count").alias("count")
)

agg_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_review_word",
    mode="overwrite",
    properties=mysql_props
)
