import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from common.env_loader import load_env

load_env()

SPARK_PATH = os.getenv("SPARK_PATH")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_previous_month():
    # 현재 날짜의 전월을 계산
    today = datetime.now()
    if today.month == 1:
        return 12  # 1월이면 전년 12월
    return today.month - 1

with DAG(
    'monthly_total_report_dag',
    default_args=default_args,
    description='Monthly total report ETL pipeline for class engagement and food ranking',
    schedule_interval='0 0 9 * *',  # 매월 1일 00:00에 실행
    start_date=days_ago(1),
    catchup=False,
    tags=['dw', 'dm', 'monthly', 'total_report'],
) as dag:
    # 시작 태스크
    start = DummyOperator(task_id="start")
    
    # Spark 작업 정의
    class_engagement_job = SparkSubmitOperator(
        task_id='class_engagement_monthly',
        application=f"{SPARK_PATH}/data-mart/create_dm_class_engagement.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        # application_args=[str(get_previous_month())],  # 전월을 파라미터로 전달
        application_args=["5"],  # for test
    )

    food_ranking_job = SparkSubmitOperator(
        task_id='food_ranking_monthly',
        application=f"{SPARK_PATH}/data-mart/create_dm_food_ranking.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        # application_args=[str(get_previous_month())],  # 전월을 파라미터로 전달
        application_args=["5"],  # for test
    )
    
    # 종료 태스크
    end = DummyOperator(task_id="end")

    # 작업 의존성 설정
    start >> [class_engagement_job, food_ranking_job] >> end 