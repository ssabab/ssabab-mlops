from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="daily_dw_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="데이터 웨어하우스 생성 dag",
    tags=["dw", "user", "daily"],
) as dag:
    
    start = DummyOperator(task_id="start")
    
    # TODO: DW Task 구현
    
    # Trigger daily user report dag
    dm_user = TriggerDagRunOperator(
        task_id="trigger_daily_report_dag",
        trigger_dag_id="daily_user_report_dag",
        wait_for_completion=True
    )

    start >> dm_user
    