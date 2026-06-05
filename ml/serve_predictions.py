from __future__ import annotations

import os
from typing import Iterator

import mlflow
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

from ml.feature_engineering import build_features


def _load_production_model():
    """Load the latest Production model from the MLflow registry."""
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "./data/mlflow")
    mlflow.set_tracking_uri(tracking_uri)
    model_uri = "models:/surge_price_model/Production"
    return mlflow.xgboost.load_model(model_uri)


def make_surge_predictor_udf(model_uri: str | None = None):
    """Return a Pandas UDF that predicts surge_multiplier for a partition of rows.

    The UDF is created as an iterator-of-series variant so the model is loaded
    once per partition rather than once per row.
    """
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "./data/mlflow")
    resolved_uri = model_uri or f"models:/surge_price_model/Production"

    @F.pandas_udf(DoubleType())
    def _predict(
        city_zone: pd.Series,
        vehicle_type: pd.Series,
        weather: pd.Series,
        status: pd.Series,
        distance_km: pd.Series,
        fare_base_inr: pd.Series,
        event_hour: pd.Series,
    ) -> pd.Series:
        mlflow.set_tracking_uri(tracking_uri)
        model = mlflow.xgboost.load_model(resolved_uri)

        frame = pd.DataFrame(
            {
                "city_zone": city_zone,
                "vehicle_type": vehicle_type,
                "weather": weather,
                "status": status,
                "distance_km": distance_km,
                "fare_base_inr": fare_base_inr,
                "event_hour": event_hour,
            }
        )
        features, _ = build_features(frame)
        predictions = model.predict(features)
        return pd.Series(predictions.clip(1.0, 3.5))

    return _predict


def add_surge_predictions(df: DataFrame, model_uri: str | None = None) -> DataFrame:
    """Append a predicted_surge_multiplier column to df using the registered model.

    Expects df to already contain event_hour as an integer column.
    """
    predict_udf = make_surge_predictor_udf(model_uri)
    return df.withColumn(
        "predicted_surge_multiplier",
        predict_udf(
            F.col("city_zone"),
            F.col("vehicle_type"),
            F.col("weather"),
            F.col("status"),
            F.col("distance_km"),
            F.col("fare_base_inr"),
            F.col("event_hour"),
        ),
    )
