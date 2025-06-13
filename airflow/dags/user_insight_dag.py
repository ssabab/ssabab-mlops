from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

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
    tags=["llm", "insight", "user"]
) as dag:

    start = DummyOperator(task_id="start")

    fetch_user_ids = PythonOperator(
        task_id="fetch_user_ids",
        python_callable=fetch_user_ids_task
    )

    load_user_data = PythonOperator(
        task_id="load_user_data",
        python_callable=load_user_data_task
    )

    from airflow.operators.python import ShortCircuitOperator

    def has_users(**context):
        return bool(context['ti'].xcom_pull(task_ids='fetch_user_ids', key='user_ids'))

    check_user_exists = ShortCircuitOperator(
        task_id='check_user_exists',
        python_callable=has_users
    )

    user_ids = get_all_user_ids()
    generate_insights_group = dynamic_user_tasks(dag, user_ids)

    end = DummyOperator(task_id="end")

    start >> fetch_user_ids >> check_user_exists >> load_user_data >> generate_insights_group >> end

