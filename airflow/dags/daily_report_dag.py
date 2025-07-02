import os
from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from common.env_loader import load_env

load_env()

SPARK_PATH = os.getenv("SPARK_PATH")
JDBC_JAR_PATH = os.getenv("JDBC_JAR_PATH")

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 5, 1)
}
with DAG(
    dag_id="daily_user_report_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="매일 자정 사용자 분석용 데이터 마트 생성 DAG",
    tags=["dm", "user", "daily"],
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    dm_user_summary = SparkSubmitOperator(
        task_id="create_dm_user_summary",
        application=f"{SPARK_PATH}/data-mart/create_dm_user_summary.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
    )

    dm_user_food_rating_rank = SparkSubmitOperator(
        task_id="create_dm_user_food_rating_rank",
        application=f"{SPARK_PATH}/data-mart/create_dm_user_food_rating_rank.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
    )

    dm_user_category_stats = SparkSubmitOperator(
        task_id="create_dm_user_category_stats",
        application=f"{SPARK_PATH}/data-mart/create_dm_user_category_stats.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
    )

    dm_user_tag_stats = SparkSubmitOperator(
        task_id="create_dm_user_tag_stats",
        application=f"{SPARK_PATH}/data-mart/create_dm_user_tag_stats.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
    )

    dm_user_group_comparison = SparkSubmitOperator(
        task_id="create_dm_user_group_comparison",
        application=f"{SPARK_PATH}/data-mart/create_dm_user_group_comparison.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
    )

    dm_user_review_word = SparkSubmitOperator(
        task_id="create_dm_user_review_word",
        application=f"{SPARK_PATH}/data-mart/create_dm_user_review_word.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
    )

    dm_user_insight = SparkSubmitOperator(
        task_id="create_dm_user_insight",
        application=f"{SPARK_PATH}/data-mart/create_dm_user_insight.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
    )

    (
        start >>
        [
            dm_user_summary,
            dm_user_food_rating_rank,
            dm_user_category_stats,
            dm_user_tag_stats,
            dm_user_group_comparison,
            dm_user_review_word,
            dm_user_insight
        ] >> 
        end
    )
    