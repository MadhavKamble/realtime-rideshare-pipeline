from __future__ import annotations

import os

import mlflow
import pandas as pd
from sklearn.model_selection import train_test_split
from xgboost import XGBRegressor

from common.logging_config import get_logger
from ml.evaluate_model import evaluate_regression
from ml.feature_engineering import build_features
from ml.nyc_taxi_loader import load_to_silver

logger = get_logger(__name__)

EXPERIMENT_NAME = "surge_price_model_nyc"
REGISTERED_MODEL_NAME = "surge_price_model_nyc"


def _make_model() -> XGBRegressor:
    return XGBRegressor(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        subsample=0.8,
        colsample_bytree=0.8,
        random_state=42,
    )


def train_surge_model_nyc(frame: pd.DataFrame):
    frame = frame.sort_values("event_timestamp").reset_index(drop=True)
    features, target = build_features(frame)

    # Time-based split: sort chronologically, train on the first 80%, test on
    # the last 20%. Random split leaks future data into training — time-based
    # split simulates real deployment conditions, where the model only ever
    # sees data up to "now" and must predict forward.
    split_idx = int(len(features) * 0.8)
    x_train_time, x_test_time = features.iloc[:split_idx], features.iloc[split_idx:]
    y_train_time, y_test_time = target.iloc[:split_idx], target.iloc[split_idx:]

    # Random split kept only as a side-by-side comparison — this model is
    # never registered, it exists purely to show why the split strategy matters.
    x_train_rand, x_test_rand, y_train_rand, y_test_rand = train_test_split(
        features, target, test_size=0.2, random_state=42
    )

    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "./data/mlflow")
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(EXPERIMENT_NAME)

    with mlflow.start_run() as run:
        time_model = _make_model()
        time_model.fit(x_train_time, y_train_time)
        time_metrics = evaluate_regression(y_test_time, time_model.predict(x_test_time))

        random_model = _make_model()
        random_model.fit(x_train_rand, y_train_rand)
        random_metrics = evaluate_regression(y_test_rand, random_model.predict(x_test_rand))

        mlflow.log_metrics(time_metrics)
        mlflow.log_metric("mae_random_split_comparison", random_metrics["mae"])
        mlflow.xgboost.log_model(
            time_model,
            artifact_path="model",
            registered_model_name=REGISTERED_MODEL_NAME,
        )
        time_metrics["run_id"] = run.info.run_id

    print(f"MAE (random split, not registered): {random_metrics['mae']:.4f}")
    print(f"MAE (time-based split, registered):  {time_metrics['mae']:.4f}")

    return time_model, time_metrics


def main() -> None:
    frame = load_to_silver()
    logger.info("Loaded %d NYC rows for training", len(frame))
    model, metrics = train_surge_model_nyc(frame)
    print(f"Model trained and registered as '{REGISTERED_MODEL_NAME}'. MAE={metrics['mae']:.4f}")


if __name__ == "__main__":
    main()
