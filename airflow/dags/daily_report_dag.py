import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from common.env_loader import load_env

load_env()

SPARK_PATH = os.getenv("SPARK_PATH")
JDBC_JAR_PATH = os.getenv("JDBC_JAR_PATH")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="daily_user_report_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="매일 자정 사용자 분석용 데이터 마트 생성 DAG",
    tags=["dm", "user", "daily"],
) as dag:

    start = DummyOperator(task_id="start")

    dm_user_tag_preference = SparkSubmitOperator(
        task_id="create_dm_user_category_tag_preference",
        application=f"{SPARK_PATH}/data-mart/create_dm_user_category_tag_preference.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
        application_args=["2025-05-14"],
    )

    dm_user_rating_top_bottom = SparkSubmitOperator(
        task_id="create_dm_user_rating_top_bottom",
        application=f"{SPARK_PATH}/data-mart/create_dm_user_rating_top_bottom.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
        application_args=["2025-05-14"],
    )

    dm_user_rating_score = SparkSubmitOperator(
        task_id="create_dm_user_rating_iqr",
        application=f"{SPARK_PATH}/data-mart/create_dm_user_rating_iqr.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
        application_args=["2025-05-14"],
    )

    end = DummyOperator(task_id="end")

    start >> [dm_user_tag_preference, dm_user_rating_top_bottom, dm_user_rating_score] >> end
