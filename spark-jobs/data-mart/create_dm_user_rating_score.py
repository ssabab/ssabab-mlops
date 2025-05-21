import pandas as pd
import numpy as np
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import *


spark = SparkSession.builder \
    .appName("create_dm_user_score_score") \
    .getOrCreate()


dim_user_schema = StructType([
    StructField("group_id", IntegerType(), False),
    StructField("user_id", IntegerType(), False),
    StructField("ord_num", StringType(), False),
    StructField("class", IntegerType(), False),
    StructField("birth_year", IntegerType(), False),
    StructField("gender", StringType(), False)
])

fact_user_scores_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("group_id", IntegerType(), False),
    StructField("food_id", IntegerType(), False),
    StructField("score", FloatType(), False),
    StructField("created_date", TimestampType(), False),
    StructField("updated_date", TimestampType(), False)
])


dim_user_data = [
    (1, 1, 13, 1, 2000, 'M'),
    (2, 2, 13, 1, 1999, 'F'),
    (3, 3, 14, 2, 1998, 'M'),
    (4, 4, 14, 2, 2001, 'F'),
    (5, 5, 13, 1, 2001, 'M')
]

fact_user_scores_data = [
    (1, 1, 1, 4.5, datetime.strptime("2023-01-01", "%Y-%m-%d"), datetime.strptime("2023-01-02", "%Y-%m-%d")),
    (2, 2, 2, 3.5, datetime.strptime("2023-01-01", "%Y-%m-%d"), datetime.strptime("2023-01-02", "%Y-%m-%d")),
    (3, 3, 3, 4.0, datetime.strptime("2023-01-01", "%Y-%m-%d"), datetime.strptime("2023-01-03", "%Y-%m-%d")),
    (4, 4, 4, 4.8, datetime.strptime("2023-01-01", "%Y-%m-%d"), datetime.strptime("2023-01-04", "%Y-%m-%d")),
    (5, 5, 5, 4.2, datetime.strptime("2023-01-01", "%Y-%m-%d"), datetime.strptime("2023-01-05", "%Y-%m-%d"))
]

dim_user_df = spark.createDataFrame(dim_user_data, dim_user_schema)
fact_user_scores_df = spark.createDataFrame(fact_user_scores_data, fact_user_scores_schema)


def get_iqr_schema(group_col=None):
    fields = [
        StructField("q1", FloatType()),
        StructField("q2", FloatType()),
        StructField("q3", FloatType())
    ]
    if group_col:
        fields.insert(0, StructField(group_col, StringType()))
    return StructType(fields)
        
def get_iqr(pdf, group_col):
    q1, q2, q3 = np.percentile(pdf["score"], [25, 50, 75])
    iqr = q3 - q1
    if group_col:
        group_val = pdf[group_col].iloc[0]
        return pd.DataFrame([[group_val, q1 - 1.5 * iqr, q2, q3 + 1.5 * iqr]], columns=[group_col, "q1", "q2", "q3"])
    else:
        return pd.DataFrame([[q1 - 1.5 * iqr, q2, q3 + 1.5 * iqr]], columns=["q1", "q2", "q3"])


q1, q2, q3 = fact_user_scores_df.approxQuantile("score", [0.25, 0.5, 0.75], 0)
iqr = q3 - q1
total_iqr_schema = StructType([
    StructField("q1", FloatType(), False),
    StructField("q2", FloatType(), False),
    StructField("q3", FloatType(), False)
])
total_iqr_df = spark.createDataFrame([[q1 - 1.5 * iqr, q2, q3 + 1.5 * iqr]], total_iqr_schema)

fact_with_user_info_df = fact_user_scores_df.join(dim_user_df, "user_id", "inner")

ord_iqr_df = fact_with_user_info_df.select("ord_num", "score") \
    .groupby("ord_num") \
    .applyInPandas(lambda pdf: get_iqr(pdf, group_col="ord_num"), schema=get_iqr_schema(group_col="ord_num"))
    
gender_iqr_df = fact_with_user_info_df.select("gender", "score") \
    .groupby("gender") \
    .applyInPandas(lambda pdf: get_iqr(pdf, group_col="gender"), schema=get_iqr_schema(group_col="gender"))


dm_user_score_score = fact_user_scores_df.select("user_id", "score") \
    .withColumn("total_iqr_json",lit(total_iqr_df.toJSON().collect())) \
    .withColumn("ord_iqr_json",lit(ord_iqr_df.toJSON().collect())) \
    .withColumn("gender_iqr_json", lit(gender_iqr_df.toJSON().collect()))
    
dm_user_score_score.show()

