from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

def process_kafka_messages():
    spark = SparkSession.builder \
        .appName("KafkaSparkProcessor") \
        .getOrCreate()

    # Kafka에서 데이터 읽기
    df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "test_1") \
        .option("startingOffsets", "earliest") \
        .load()

    # JSON 스키마 정의
    schema = StructType() \
        .add("message", StringType())

    # JSON 파싱 및 처리
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # 결과 출력
    parsed_df.show(truncate=False)

    # 결과를 파일로 저장
    parsed_df.write \
        .mode("overwrite") \
        .json("/opt/airflow/output/processed_messages")

    spark.stop()

if __name__ == "__main__":
    process_kafka_messages() 