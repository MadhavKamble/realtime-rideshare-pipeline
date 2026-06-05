from __future__ import annotations

import os
from pathlib import Path

import mlflow
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor

from ml.evaluate_model import evaluate_regression
from ml.feature_engineering import build_features


def train_surge_model(frame: pd.DataFrame, experiment_name: str | None = None):
    features, target = build_features(frame)
    x_train, x_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=42)

    model = XGBRegressor(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
    )

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "./data/mlflow")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment_name or os.getenv("MLFLOW_EXPERIMENT_NAME", "surge_price_prediction"))

    with mlflow.start_run() as run:
        model.fit(x_train, y_train)
        predictions = model.predict(x_test)
        metrics = evaluate_regression(y_test, predictions)
        mlflow.log_metrics(metrics)
        mlflow.xgboost.log_model(
            model,
            artifact_path="model",
            registered_model_name="surge_price_model",
        )
        metrics["run_id"] = run.info.run_id

    return model, metrics
