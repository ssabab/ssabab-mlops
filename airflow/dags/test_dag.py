from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from kafka import KafkaProducer

def produce_message():
    producer = KafkaProducer(bootstrap_servers="kafka:9092")
    producer.send("test-topic", b"hello from Airflow DAG")
    producer.flush()
    producer.close()

with DAG(
    dag_id="kafka_produce_consume_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    kafka_producer = PythonOperator(
        task_id="produce_to_kafka",
        python_callable=produce_message
    )

    kafka_consumer = SparkSubmitOperator(
        task_id="consume_from_kafka",
        application="/opt/airflow/dags/scripts/test_consumer.py",
        conn_id="spark_default",
        verbose=True,
        packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        driver_memory="1g",
        executor_memory="1g",
        total_executor_cores=1
    )

    kafka_producer >> kafka_consumer
