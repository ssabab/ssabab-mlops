import os
import yaml
import joblib
import pandas as pd
import sqlalchemy
import mlflow
import mlflow.xgboost
import xgboost as xgb
from airflow.decorators import task
from airflow.operators.python import get_current_context
from utils.db import get_mysql_sqlalchemy_url
from common.env_loader import load_env

load_env()

XGB_MODEL_NAME = os.getenv("MODEL__XGB__NAME")
XGB_CONFIG_PATH = os.getenv("MODEL__XGB__CONFIG_PATH")
XGB_ARTIFACT_DATA_DIR = os.getenv("MODEL__XGB__ARTIFACT_DATA_DIR")
XGB_ARTIFACT_MODEL_DIR = os.getenv("MODEL__XGB__ARTIFACT_MODEL_DIR")

def get_csv_path(execution_date: str) -> str:
    return os.path.join(XGB_ARTIFACT_DATA_DIR, f"{XGB_MODEL_NAME}_train_{execution_date}.csv")

def get_model_path(execution_date: str) -> str:
    return os.path.join(XGB_ARTIFACT_MODEL_DIR, f"{XGB_MODEL_NAME}_model_{execution_date}.bin")


@task
def generate_xgb_train_csv():
    context = get_current_context()
    execution_date = context["execution_date"].format("YYYY-MM-DD")

    csv_path = get_csv_path(execution_date)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)

    engine = sqlalchemy.create_engine(get_mysql_sqlalchemy_url())
    query = f"""
        SELECT *
        FROM ssabab_dw.dm_xgb_train_data
        WHERE train_date = '{execution_date}'
    """
    df = pd.read_sql(query, engine)
    df.to_csv(csv_path, index=False)


@task
def train_xgb_model():
    context = get_current_context()
    execution_date = context["execution_date"].format("YYYY-MM-DD")

    csv_path = get_csv_path(execution_date)
    model_path = get_model_path(execution_date)
    config_path = XGB_CONFIG_PATH
    model_name = XGB_MODEL_NAME

    df = pd.read_csv(csv_path)
    X = df.drop(columns=["label", "user_id", "train_date"])
    y = df["label"]

    with open(config_path) as f:
        params = yaml.safe_load(f)

    model = xgb.XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        **params
    )

    mlflow.set_experiment(f"{model_name}_recommender")

    with mlflow.start_run(run_name=f"train_{execution_date}"):
        mlflow.xgboost.autolog()
        model.fit(X, y)

        joblib.dump(model, model_path)
        mlflow.log_artifact(csv_path, artifact_path="data")
        mlflow.log_artifact(model_path, artifact_path="model")
        mlflow.log_artifact(config_path, artifact_path="config")
        