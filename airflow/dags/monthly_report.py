from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from tasks.class_engagement_monthly import main as class_engagement_main
from tasks.food_ranking_monthly import main as food_ranking_main

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'monthly_report',
    default_args=default_args,
    description='DW to DM(monthly) ETL pipeline',
    schedule_interval='0 0 9 * *',  # 매월 1일 00:00에 실행
    start_date=days_ago(1),
    catchup=False,
    tags=['dw', 'dm', 'monthly'],
) as dag:
    # Python 작업 정의
    class_engagement_job = PythonOperator(
        task_id='class_engagement_monthly',
        python_callable=class_engagement_main,
    )

    food_ranking_job = PythonOperator(
        task_id='food_ranking_monthly',
        python_callable=food_ranking_main,
    )

    # 작업 의존성 설정
    [class_engagement_job, food_ranking_job] 