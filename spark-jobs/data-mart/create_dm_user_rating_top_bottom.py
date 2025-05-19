from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("create_dm_user_rating_top_bottom") \
    .getOrCreate()
