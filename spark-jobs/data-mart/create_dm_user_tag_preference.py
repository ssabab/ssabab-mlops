from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("create_dm_user_tag_preference") \
    .getOrCreate()
    
