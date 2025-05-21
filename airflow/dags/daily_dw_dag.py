from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from tasks.create_dw_task import *

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
    description="AWS RDS → fact(SNAPSHOT) 기반 DW 적재 DAG",
    tags=["dw", "snapshot", "raw", "fact"],
) as dag:

    start = DummyOperator(task_id="start")

    create_schema = create_tables_from_sql_files()

    # TaskGroup 1: raw → fact with snapshot_date
    with TaskGroup("transform_raw_to_dim", tooltip="raw 테이블로부터 dimension 테이블 생성") as dim_group:
        insert_dim_food_data()
        insert_dim_user_group_data()
        insert_dim_user_data()

    dim_done = BashOperator(
        task_id="dim_insert_complete",
        bash_command='echo "Dimension 테이블 적재 완료"',
    )
    
    
    # TaskGroup 2: raw → dim with snapshot_date
    with TaskGroup("transform_raw_to_fact", tooltip="raw 테이블로부터 fact 테이블 생성 및 snapshot 기록") as fact_group:
        insert_fact_user_ratings_data()

    fact_done = BashOperator(
        task_id="fact_insert_complete",
        bash_command='echo "Fact 테이블 적재 완료"',
    )

    # Trigger downstream DAG
    trigger_report_dag = TriggerDagRunOperator(
        task_id="trigger_daily_user_report",
        trigger_dag_id="daily_user_dm_report_dag",
        wait_for_completion=True,
    )

    # Task dependency
    start >> create_schema >> dim_group >> dim_done >> fact_group >> fact_done >> trigger_report_dag
