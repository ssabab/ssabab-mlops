from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, BooleanType
from datetime import datetime
import sys
import os
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties

def create_spark_session():
    return SparkSession.builder \
        .appName("Monthly Visitors") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.7.2.jar") \
        .getOrCreate()

def main():
    if len(sys.argv) != 2:
        print("Usage: create_dm_monthly_visitors.py <YYYY-MM>")
        sys.exit(1)
    target_month = sys.argv[1]

    spark = SparkSession.builder.appName("Monthly Visitors").getOrCreate()
    mysql_url = get_mysql_jdbc_url()
    mysql_props = get_mysql_jdbc_properties()

    visitor_log_df = spark.read.jdbc(
        url=mysql_url,
        table="visitor_log",
        properties=mysql_props
    ).filter(col("visit_time").substr(1, 7) == target_month)

    result_df = visitor_log_df.agg(countDistinct("user_id").alias("user_count"))

    result_df.write.jdbc(
        url=mysql_url,
        table="dm_monthly_visitors",
        mode="overwrite",
        properties=mysql_props
    )

if __name__ == "__main__":
    main()
