from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from tasks.load_user_data import load_user_data
from tasks.generate_insight import generate_insight_chunk
from tasks.save_to_dw import save_insight_to_dw

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="daily_user_insight_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="LLM 기반 사용자 인사이트 생성 DAG",
    tags=["insight", "llm", "dw"],
) as dag:

    start = DummyOperator(task_id="start")

    with TaskGroup("generate_insight_chunks") as chunk_group:
        for chunk_id in range(10):
            PythonOperator(
                task_id=f"generate_chunk_{chunk_id}",
                python_callable=generate_insight_chunk,
                op_kwargs={"chunk_id": chunk_id}
            )

    save_result_task = PythonOperator(
        task_id="save_to_dw",
        python_callable=save_insight_to_dw
    )

    start >> chunk_group >> save_result_task
