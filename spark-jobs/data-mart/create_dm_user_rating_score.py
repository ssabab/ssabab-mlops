import pandas as pd
import numpy as np
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *


spark = SparkSession.builder \
    .appName("create_dm_user_rating_score") \
    .getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("ord_num", IntegerType(), False),
    StructField("birth_year", IntegerType(), False),
    StructField("gender", BooleanType(), False),
    StructField("food_id", IntegerType(), False),
    StructField("food_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("tag", StringType(), False),
    StructField("food_score", FloatType(), False),
    StructField("menu_date", DateType(), False),
])

mock_data = [
    (1, 13, 2000, False, 1, "김치찌개", "한식", "국", 4.0, datetime.strptime("2023-01-01","%Y-%m-%d")),
    (2, 13, 1999, True, 2, "파스타", "양식", "면", 3.0, datetime.strptime("2023-01-01","%Y-%m-%d")),
    (3, 14, 1998, False, 2, "파스타", "양식", "면", 4.5, datetime.strptime("2023-01-01","%Y-%m-%d")),
    (4, 14, 2001, True, 1, "김치찌개", "한식", "국", 4.5, datetime.strptime("2023-01-01","%Y-%m-%d")),
    (5, 13, 2001, True, 2, "파스타", "양식", "면", 4.0, datetime.strptime("2023-01-01","%Y-%m-%d")),
]    

df = spark.createDataFrame(mock_data, schema)

def get_iqr_schema(group_col=None):
    fields = [
        StructField("q1", FloatType()),
        StructField("q2", FloatType()),
        StructField("q3", FloatType())
    ]
    if group_col:
        fields.insert(0, StructField(group_col, df.schema[group_col].dataType))
    return StructType(fields)
        
def get_iqr(pdf, group_col):
    q1, q2, q3 = np.percentile(pdf["food_score"], [25, 50, 75])
    iqr = q3 - q1
    if group_col:
        group_val = pdf[group_col].iloc[0]
        return pd.DataFrame([[group_val, q1 - 1.5 * iqr, q2, q3 + 1.5 * iqr]], columns=[group_col, "q1", "q2", "q3"])
    else:
        return pd.DataFrame([[q1 - 1.5 * iqr, q2, q3 + 1.5 * iqr]], columns=["q1", "q2", "q3"])


total_iqr_df = df.select("food_score") \
    .applyInPandas(lambda pdf: get_iqr(pdf, group_col=None), schema=get_iqr_schema(group_col=None))

ord_iqr_df = df.select("ord_num", "food_score") \
    .groupby("ord_num") \
    .applyInPandas(lambda pdf: get_iqr(pdf, group_col="ord_num"), schema=get_iqr_schema(group_col="ord_num"))

gender_iqr_df = df.select("gender", "food_score") \
    .groupby("gender") \
    .applyInPandas(lambda pdf: get_iqr(pdf, group_col="gender"), schema=get_iqr_schema(group_col="gender"))
    
