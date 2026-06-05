import pandas as pd

from ml.feature_engineering import build_features


def test_build_features_creates_expected_columns():
    frame = pd.DataFrame(
        {
            "event_timestamp": ["2024-01-01T08:00:00Z"],
            "city_zone": ["cbd"],
            "vehicle_type": ["cab_economy"],
            "weather": ["clear"],
            "status": ["completed"],
            "distance_km": [8.4],
            "fare_base_inr": [336.0],
            "surge_multiplier": [1.8],
        }
    )

    features, target = build_features(frame)
    assert not features.empty
    assert target.iloc[0] == 1.8
    assert any(column.startswith("city_zone_") for column in features.columns)
