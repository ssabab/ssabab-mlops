from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

spark = SparkSession.builder.appName("create_dm_user_insight").getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_props = get_mysql_jdbc_properties()

raw_df = spark.read.jdbc(
    url=mysql_url,
    table="ssabab_dw.raw_user_insight",
    properties=mysql_props
)

window_spec = Window.partitionBy("user_id").orderBy(col("insight_date").desc())
latest_df = raw_df.withColumn("row_num", row_number().over(window_spec)) \
                  .filter(col("row_num") == 1) \
                  .select("user_id", "insight")

latest_df.write.jdbc(
    url=mysql_url,
    table="ssabab_dm.dm_user_insight",
    mode="overwrite", 
    properties=mysql_props
)
