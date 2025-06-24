import os
from datetime import timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago    
from common.env_loader import load_env
from tasks.create_fm_dataset import generate_fm_input

load_env()

SPARK_PATH = os.getenv("SPARK_PATH")
JDBC_JAR_PATH = os.getenv("JDBC_JAR_PATH")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'mlops_fm_data_dag',
    default_args=default_args,
    description='MLOps for food menu comparison',
    schedule_interval='0 21 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['mlops', 'food_menu_comparison'],
) as dag:
    start = DummyOperator(task_id="start")

    food_menu_comparison_job = SparkSubmitOperator(
        task_id='food_menu_comparison',
        application=f"{SPARK_PATH}/data-mart/create_dm_mlops_data.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
        application_args=["{{ ds }}"],
    )

    generate_fm_task = generate_fm_input.override(task_id='generate_fm_input_data')("{{ ds }}")

    end = DummyOperator(task_id="end")

    start >> food_menu_comparison_job >> generate_fm_task >> end