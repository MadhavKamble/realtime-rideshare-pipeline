from __future__ import annotations

import pandas as pd

CATEGORICAL_COLUMNS = ["city_zone", "vehicle_type", "weather", "status"]
NUMERIC_COLUMNS = ["distance_km", "fare_base_inr", "event_hour"]
TARGET_COLUMN = "surge_multiplier"


def build_features(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    data = frame.copy()
    if "event_timestamp" in data.columns and "event_hour" not in data.columns:
        data["event_timestamp"] = pd.to_datetime(data["event_timestamp"], utc=True, errors="coerce")
        data["event_hour"] = data["event_timestamp"].dt.hour.fillna(0).astype(int)

    for column in CATEGORICAL_COLUMNS:
        if column not in data.columns:
            data[column] = "unknown"

    for column in NUMERIC_COLUMNS:
        if column not in data.columns:
            data[column] = 0.0

    data = data.fillna({column: "unknown" for column in CATEGORICAL_COLUMNS})
    features = pd.get_dummies(data[CATEGORICAL_COLUMNS + NUMERIC_COLUMNS], columns=CATEGORICAL_COLUMNS, drop_first=False)
    target = data[TARGET_COLUMN] if TARGET_COLUMN in data.columns else pd.Series([1.0] * len(data), name=TARGET_COLUMN)
    return features, target
