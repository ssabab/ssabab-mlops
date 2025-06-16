import numpy as np
import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import lit, col
from utils.db import get_mysql_jdbc_url, get_mysql_jdbc_properties


spark = SparkSession.builder \
    .appName("create_dm_user_rating_iqr") \
    .getOrCreate()

mysql_url = get_mysql_jdbc_url()
mysql_properties = get_mysql_jdbc_properties()

dim_user_df = spark.read.jdbc(
    url=mysql_url,
    table="dim_user",
    properties=mysql_properties
)

fact_user_scores_df = spark.read.jdbc(
    url=mysql_url,
    table="fact_user_ratings",
    properties=mysql_properties
)


q1, q2, q3 = fact_user_scores_df.approxQuantile("food_score", [0.25, 0.5, 0.75], 0)
iqr = q3 - q1
total_iqr_schema = StructType([
    StructField("iqr_type", StringType(), False),
    StructField("iqr_value", StringType(), False),
    StructField("q1", FloatType(), False),
    StructField("q2", FloatType(), False),
    StructField("q3", FloatType(), False)
])
total_iqr_df = spark.createDataFrame([["total", "total", q1 - 1.5 * iqr, q2, q3 + 1.5 * iqr]], total_iqr_schema)


fact_with_user_info_df = fact_user_scores_df.join(dim_user_df, "user_id", "inner")


def get_iqr(pdf, group_col):
    q1, q2, q3 = np.percentile(pdf["food_score"], [25, 50, 75])
    iqr = q3 - q1
    group_val = pdf[group_col].iloc[0]
    return pd.DataFrame([[group_val, q1 - 1.5 * iqr, q2, q3 + 1.5 * iqr]], columns=[group_col, "q1", "q2", "q3"])

birth_date_iqr_df = fact_with_user_info_df.select("birth_date", "food_score") \
    .groupby("birth_date") \
    .applyInPandas(lambda pdf: get_iqr(pdf, "birth_date"),
                   schema="birth_date string, q1 float, q2 float, q3 float") \
    .withColumn("iqr_type", lit("birth")) \
    .withColumnRenamed("birth_date", "iqr_value") \
    .select("iqr_type", "iqr_value", "q1", "q2", "q3")


gender_iqr_df = fact_with_user_info_df.select("gender", "food_score") \
    .groupby("gender") \
    .applyInPandas(lambda pdf: get_iqr(pdf, "gender"),
                   schema="gender string, q1 float, q2 float, q3 float") \
    .withColumn("iqr_type", lit("gender")) \
    .withColumnRenamed("gender", "iqr_value") \
    .select("iqr_type", "iqr_value", "q1", "q2", "q3")

dm_iqrs_df = total_iqr_df.unionByName(birth_date_iqr_df).unionByName(gender_iqr_df)

dm_iqrs_df.write.jdbc(
    url=mysql_url,
    table="dm_iqrs",
    mode="overwrite",
    properties=mysql_properties
)


dm_user_rating_df = fact_user_scores_df.groupBy("user_id") \
    .agg({"food_score": "avg"}) \
    .withColumnRenamed("avg(food_score)", "score")

dm_user_rating_df.write.jdbc(
    url=mysql_url,
    table="dm_user_rating",
    mode="overwrite",
    properties=mysql_properties
)
