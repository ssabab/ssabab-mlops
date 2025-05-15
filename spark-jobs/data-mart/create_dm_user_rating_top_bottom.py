from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("create_dm_user_rating_top_bottom") \
    .getOrCreate()
    
