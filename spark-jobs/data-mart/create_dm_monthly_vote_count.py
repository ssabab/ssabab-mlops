from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, concat, lit
from pyspark.sql.types import *
from datetime import datetime
import sys
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

def main():
    if len(sys.argv) != 2:
        print("Usage: create_dm_monthly_vote_count.py <YYYY-MM>")
        sys.exit(1)
    target_month = sys.argv[1]

    spark = SparkSession.builder.appName("Monthly Vote Count").getOrCreate()
    mysql_url = get_mysql_jdbc_url()
    mysql_props = get_mysql_jdbc_properties()

    food_review_df = spark.read.jdbc(
        url=mysql_url,
        table="food_review",
        properties=mysql_props
    ).filter(col("create_at").substr(1, 7) == target_month)

    account_df = spark.read.jdbc(
        url=mysql_url,
        table="account",
        properties=mysql_props
    )

    joined_df = food_review_df.join(account_df, "user_id")
    joined_df = joined_df.withColumn("generation", concat(col("ord_num"), lit("기")))
    joined_df = joined_df.withColumn("gender", col("gender").cast("string"))
    joined_df = joined_df.withColumn("age", lit(datetime.now().year) - col("birth_year"))

    result_df = joined_df.groupBy("class", "generation", "gender", "age") \
        .agg(count("*").alias("review_count"))

    result_df.write.jdbc(
        url=mysql_url,
        table="dm_monthly_vote_count",
        mode="overwrite",
        properties=mysql_props
    )

if __name__ == "__main__":
    main()
