from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import mlflow


def train_fm_model(ds, **kwargs):
    data_path = f"/opt/airflow/data/train_{ds}.libfm"
    model_path = f"/opt/airflow/models/fm_model_{ds}.bin"
    pred_path = f"/opt/airflow/data/pred_{ds}.txt"

    dim = "1,1,8"
    iter_ = 50
    lr = 0.01
    method = "sgd"

    # MLflow 설정
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment("fm_recommender")

    with mlflow.start_run(run_name=f"train_{ds}") as run:
        run_id = run.info.run_id

        mlflow.log_param("dim", dim)
        mlflow.log_param("iter", iter_)
        mlflow.log_param("learn_rate", lr)
        mlflow.log_param("method", method)
        mlflow.log_param("date", ds)

        # libFM 학습 실행
        subprocess.run([
            "libfm",
            "-task", "c",
            "-train", data_path,
            "-out", pred_path,
            "-model", model_path,
            "-dim", dim,
            "-iter", str(iter_),
            "-method", method,
            "-learn_rate", str(lr)
        ], check=True)

        mlflow.log_artifact(data_path, artifact_path="data")
        mlflow.log_artifact(pred_path, artifact_path="pred")
        mlflow.log_artifact(model_path, artifact_path="model")

        # run_id XCom으로 전달
        kwargs['ti'].xcom_push(key='run_id', value=run_id)


def evaluate_fm_model(ds, **kwargs):
    run_id = kwargs['ti'].xcom_pull(key='run_id', task_ids='train_fm')
    pred_path = f"/opt/airflow/data/pred_{ds}.txt"
    label_path = f"/opt/airflow/data/true_{ds}.txt"

    import numpy as np
    from sklearn.metrics import mean_squared_error, precision_score, recall_score

    y_pred = np.loadtxt(pred_path)
    y_true = np.loadtxt(label_path)
    y_bin = (y_pred >= 0.5).astype(int)

    rmse = mean_squared_error(y_true, y_pred, squared=False)
    precision = precision_score(y_true, y_bin, zero_division=0)
    recall = recall_score(y_true, y_bin, zero_division=0)

    print(f"RMSE: {rmse:.4f}, Precision: {precision:.4f}, Recall: {recall:.4f}")

    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.start_run(run_id=run_id)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("precision", precision)
    mlflow.log_metric("recall", recall)
    mlflow.end_run()


with DAG(
    dag_id="mlops_fm_train_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["fm", "mlflow"]
) as dag:

    train = PythonOperator(
        task_id="train_fm",
        python_callable=train_fm_model,
        provide_context=True
    )

    evaluate = PythonOperator(
        task_id="evaluate_fm",
        python_callable=evaluate_fm_model,
        provide_context=True
    )

    train >> evaluate