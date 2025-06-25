from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from tasks.extract_comment import *

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}
with DAG(
    dag_id="daily_user_keyword_extraction_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="사용자 리뷰에서 명사 추출 및 요약 통계 DAG",
    tags=["keyword", "user", "review"],
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    raw_task = extract_raw_user_review_word()

    start >> raw_task >> end
