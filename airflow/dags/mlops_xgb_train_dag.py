import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from tasks.train_xgb_model import *
from common.env_loader import load_env

load_env()

SPARK_PATH = os.getenv("SPARK_PATH")
JDBC_JAR_PATH = os.getenv("JDBC_JAR_PATH")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="mlops_xgb_train_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    description="XGBoost 기반 A/B 메뉴 추천 DAG",
    tags=["recommendation", "xgboost", "mlflow"]
) as dag:

    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    check_menu = check_menu_count() 

    create_xgb_train_data = SparkSubmitOperator(
        task_id="create_xgb_train_data",
        application=f"{SPARK_PATH}/data-mart/create_xgb_train_data.py",
        conn_id="spark_default",
        conf={"spark.executor.memory": "2g"},
        jars=JDBC_JAR_PATH,
    )

    generate_train_data = generate_xgb_train_csv()
    train_model = train_xgb_model()

    validate_model = DummyOperator(task_id="validate_model")

    start >> check_menu >> create_xgb_train_data >> generate_train_data >> train_model >> validate_model >> end
