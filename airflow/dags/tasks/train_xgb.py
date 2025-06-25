def train_xgb(ds):
    import pandas as pd
    import sqlalchemy
    import mlflow
    import mlflow.xgboost
    import xgboost as xgb
    import joblib
    import os
    import yaml

    model_name = "xgb"
    csv_path = f"/opt/airflow/artifacts/data/train_{ds}.csv"
    model_path = f"/opt/airflow/artifacts/model/{model_name}_model_{ds}.bin"
    config_path = "/opt/airflow/configs/xgb_config.yaml"


    engine = sqlalchemy.create_engine(get_mysql_sqlalchemy_url())
    query = f"SELECT * FROM ssabab_dw.dm_xgb_train_data WHERE ds = '{ds}'"
    df = pd.read_sql(query, engine)
    df.to_csv(csv_path, index=False)

    X = df.drop(columns=["label", "user_id", "ds"])
    y = df["label"]

    with open(config_path) as f:
        params = yaml.safe_load(f)

    model = xgb.XGBClassifier(
        objective="binary:logistic",
        eval_metric="logloss",
        **params
    )

    mlflow.set_experiment(f"{model_name}_recommender")

    with mlflow.start_run(run_name=f"train_{ds}"):
        mlflow.xgboost.autolog()
        model.fit(X, y)

        joblib.dump(model, model_path)
        mlflow.log_artifact(csv_path, artifact_path="data")
        mlflow.log_artifact(model_path, artifact_path="model")
        mlflow.log_artifact(config_path, artifact_path="config")
