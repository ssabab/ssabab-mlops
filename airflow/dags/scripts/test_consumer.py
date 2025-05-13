from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("KafkaConsumeTest").getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "test-topic") \
    .option("startingOffsets", "earliest") \
    .load()

df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("console") \
  .outputMode("append") \
  .start() \
  .awaitTermination()