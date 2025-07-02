from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from tasks.extract_comment import *

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1)
}
with DAG(
    dag_id="daily_user_keyword_extraction_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="사용자 리뷰에서 키워드 추출 및 요약 통계 DAG",
    tags=["keyword", "user", "review"],
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    df_task = fetch_review_df()
    parsed_task = parse_nouns(df_task)
    insert_task = insert_review_keywords(parsed_task)

    start >> df_task >> parsed_task >> insert_task >> end
