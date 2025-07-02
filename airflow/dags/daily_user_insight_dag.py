from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime
from tasks.generate_insight import *

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1)
}
with DAG(
    dag_id="daily_user_insight_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="LLM 기반 사용자 인사이트 생성 DAG",
    tags=["llm", "user", "insight"]
) as dag:
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    user_ids = fetch_user_ids()
    insights = generate_user_insight_task(user_ids)
    raw_insert = insert_raw_user_insight(insights)

    start >> user_ids >> insights >> raw_insert >> end
