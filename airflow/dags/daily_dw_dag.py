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
    description="AWS RDS → fact 기반 DW 적재 DAG",
    tags=["dw", "snapshot", "raw", "fact"],
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    create_schema = create_tables_from_sql_files()

    with TaskGroup("transform_raw_to_dim", tooltip="raw → dimension 테이블") as dim_group:
        insert_dim_user()
        insert_dim_menu_food_combined()

    dim_done = BashOperator(
        task_id="dim_insert_complete",
        bash_command='echo "Dimension 테이블 적재 완료"',
    )

    with TaskGroup("transform_raw_to_fact", tooltip="raw → fact 테이블") as fact_group:
        insert_fact_user_ratings()
        insert_fact_user_votings()
        insert_fact_user_comments()

    fact_done = BashOperator(
        task_id="fact_insert_complete",
        bash_command='echo "Fact 테이블 적재 완료"',
    )

    trigger_keyword_dag = TriggerDagRunOperator(
        task_id="trigger_keyword_extraction_dag",
        trigger_dag_id="daily_user_keyword_extraction_dag",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60
    )

    trigger_insight_dag = TriggerDagRunOperator(
        task_id="trigger_user_insight_dag",
        trigger_dag_id="daily_user_insight_dag",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=60
    )

    trigger_report_dag = TriggerDagRunOperator(
        task_id="trigger_daily_user_report_dag",
        trigger_dag_id="daily_user_report_dag",
        wait_for_completion=True,
    )

    start >> create_schema >> dim_group >> dim_done >> fact_group >> fact_done >> [trigger_keyword_dag, trigger_insight_dag] >> trigger_report_dag >> end
