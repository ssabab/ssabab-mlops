import os
import yaml
import joblib
import pandas as pd
import numpy as np
import sqlalchemy
import mlflow
import mlflow.xgboost
import xgboost as xgb
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException
from utils.db import get_mysql_connection, get_mysql_sqlalchemy_url
from common.env_loader import load_env

load_env()

XGB_MODEL_NAME = os.getenv("MODEL__XGB__NAME")
XGB_CONFIG_PATH = os.getenv("MODEL__XGB__CONFIG_PATH")


@task
def generate_xgb_train_csv():
    context = get_current_context()
    execution_date = context["execution_date"].format("YYYY-MM-DD")

    filename = f"{XGB_MODEL_NAME}_train_{execution_date}.csv"
    csv_path = f"/tmp/{filename}"

    engine = sqlalchemy.create_engine(get_mysql_sqlalchemy_url())
    query = f"""
        SELECT *
        FROM ssabab_dw.dm_xgb_train_data
        WHERE train_date = '{execution_date}'
    """
    df = pd.read_sql(query, engine)
    df.to_csv(csv_path, index=False)

    return csv_path


@task
def check_menu_count():
    context = get_current_context()
    execution_date = context["execution_date"].format("YYYY-MM-DD")

    conn = get_mysql_connection()
    cursor = conn.cursor()
    
    query = """
        SELECT COUNT(DISTINCT menu_id)
        FROM ssabab_dw.dim_menu_food_combined
        WHERE menu_date = %s
    """
    cursor.execute(query, (execution_date,))
    count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()

    if count < 2:
        raise AirflowSkipException(f"Menu doesn't exist")


@task(queue="gpu")
def train_xgb_model(csv_path):
    context = get_current_context()
    execution_date = context["execution_date"].format("YYYY-MM-DD")

    df = pd.read_csv(csv_path)
    X = df.drop(columns=["label", "user_id", "train_date"])
    y = df["label"]

    with open(XGB_CONFIG_PATH) as f:
        params = yaml.safe_load(f)

    model = xgb.XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        **params
    )

    # MLflow 실험 그룹 지정
    mlflow.set_experiment(f"{XGB_MODEL_NAME}_recommender")
    
    with mlflow.start_run(run_name=f"train_{execution_date}") as run:
        # 실험에 대한 태그 등록 (모델명, 학습 날짜, 실행 유형)
        mlflow.set_tags({
            "model": XGB_MODEL_NAME,
            "train_date": execution_date,
            "run_type": "daily_batch"
        })

        # 모델 파일명을 run_id 기준으로 유일하게 생성
        run_id = run.info.run_id[:8] 
        model_filename = f"{XGB_MODEL_NAME}_model_{execution_date}_{run_id}.bin"
        model_path = f"/tmp/{model_filename}"
        joblib.dump(model, model_path)

        mlflow.log_params(params) # 하이퍼파라미터 전체 기록
        mlflow.log_param("train_date", execution_date) # 추가 단일 파라미터 기록 (학습 날짜)
        mlflow.xgboost.autolog() # XGBoost 모델 구조, 학습 과정 자동 로깅 활성화

        model.fit(X, y)

        y_pred_prob = model.predict_proba(X)[:, 1]
        y_pred = (y_pred_prob >= 0.5).astype(int)

        # 평가 지표 기록
        mlflow.log_metrics({
            "accuracy": accuracy_score(y, y_pred),
            "precision": precision_score(y, y_pred, zero_division=0),
            "recall": recall_score(y, y_pred, zero_division=0),
            "f1_score": f1_score(y, y_pred, zero_division=0),
        })

        mlflow.log_artifact(csv_path, artifact_path="data") # 학습에 사용된 CSV 데이터 저장
        mlflow.log_artifact(model_path, artifact_path="model") # 저장된 모델 파일 등록
        mlflow.log_artifact(XGB_CONFIG_PATH, artifact_path="config") # 사용한 하이퍼파라미터 설정 파일 기록
