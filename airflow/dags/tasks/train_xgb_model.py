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

model_name = "xgb"
csv_path = f"/opt/airflow/artifacts/data/train_{execution_date}.csv"
model_path = f"/opt/airflow/artifacts/model/{model_name}_model_{execution_date}.bin"
config_path = "/opt/airflow/configs/xgb_config.yaml"

@task
def generate_xgb_train_csv():
    context = get_current_context()
    execution_date = context["execution_date"].format("YYYY-MM-DD")
    
    csv_dir = "/opt/airflow/artifacts/data"
    os.makedirs(csv_dir, exist_ok=True)
    csv_path = os.path.join(csv_dir, f"xgb_train_{execution_date}.csv")

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
