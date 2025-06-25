from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from tasks.generate_csv import generate_csv_from_dm
from tasks.train_xgb_model import train_xgb

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 6, 1),
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

    export_csv = PythonOperator(
        task_id="generate_csv_from_dm",
        python_callable=generate_csv_from_dm,
        op_kwargs={"ds": "{{ ds }}"}
    )

    train_model = PythonOperator(
        task_id="train_xgb_model",
        python_callable=train_xgb,
        op_kwargs={"ds": "{{ ds }}"}
    )

    predict = PythonOperator(
        task_id="predict_ab_menu",
        python_callable=predict_ab_menu,
        op_kwargs={"ds": "{{ ds }}"}
    )

    start >> export_csv >> train_model >> predict >> end
