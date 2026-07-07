from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import requests

from common.logging_config import get_logger
from storage.delta_config import GOLD_ZONE_DEMAND_HISTORICAL_NYC_TABLE, SILVER_RIDES_HISTORICAL_NYC_TABLE

logger = get_logger(__name__)

NYC_TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
NYC_TAXI_DATA_DIR = Path(os.getenv("NYC_TAXI_DATA_DIR", "./data/nyc_taxi"))
NYC_TAXI_PARQUET_PATH = NYC_TAXI_DATA_DIR / "yellow_tripdata_2023-01.parquet"

# NYC PULocationID buckets mapped to synthetic zone labels. These are a distinct
# taxonomy from the simulator's named zones (airport, cbd, mall, railway_station,
# residential) — kept in a separate historical table so the two never mix in the
# same city_zone column (see SILVER_RIDES_HISTORICAL_NYC_TABLE).
_ZONE_BINS = [0, 50, 100, 150, 200, float("inf")]
_ZONE_LABELS = ["zone_A", "zone_B", "zone_C", "zone_D", "zone_E"]

_PAYMENT_METHOD_MAP = {1: "card", 2: "cash"}

MILES_TO_KM = 1.60934
USD_TO_INR = 83.0


def download_trip_data() -> Path:
    NYC_TAXI_DATA_DIR.mkdir(parents=True, exist_ok=True)

    if NYC_TAXI_PARQUET_PATH.exists():
        logger.info("NYC trip data already downloaded at %s, skipping download", NYC_TAXI_PARQUET_PATH)
        return NYC_TAXI_PARQUET_PATH

    logger.info("Downloading NYC trip data from %s", NYC_TAXI_URL)
    with requests.get(NYC_TAXI_URL, stream=True, timeout=60) as response:
        response.raise_for_status()
        with open(NYC_TAXI_PARQUET_PATH, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)

    logger.info("Downloaded NYC trip data to %s", NYC_TAXI_PARQUET_PATH)
    return NYC_TAXI_PARQUET_PATH


def _map_zone(location_id: pd.Series) -> pd.Series:
    return pd.cut(location_id, bins=_ZONE_BINS, labels=_ZONE_LABELS, right=True).astype(str)


def _map_payment_method(payment_type: pd.Series) -> pd.Series:
    return payment_type.map(_PAYMENT_METHOD_MAP).fillna("other")


def _scale_surge_multiplier(tip_ratio: pd.Series) -> pd.Series:
    min_ratio, max_ratio = tip_ratio.min(), tip_ratio.max()
    if max_ratio == min_ratio:
        return pd.Series(1.0, index=tip_ratio.index)
    normalized = (tip_ratio - min_ratio) / (max_ratio - min_ratio)
    return 1.0 + normalized * 2.5


def load_to_silver(spark_or_pandas: str = "pandas") -> pd.DataFrame:
    """Map raw NYC TLC trip data to the Silver rides schema.

    Only "pandas" is supported today — the Airflow container that runs the
    historical batch load and NYC model training has no PySpark installed
    (see requirements-airflow.txt), so a Spark path has no current caller.
    """
    if spark_or_pandas != "pandas":
        raise NotImplementedError("Only spark_or_pandas='pandas' is supported")

    parquet_path = download_trip_data()
    raw = pd.read_parquet(parquet_path)

    mapped = pd.DataFrame()
    mapped["ride_id"] = "nyc-" + raw.index.astype(str)
    mapped["event_timestamp"] = pd.to_datetime(raw["tpep_pickup_datetime"], errors="coerce")
    mapped["event_date"] = mapped["event_timestamp"].dt.date.astype(str)
    mapped["event_hour"] = mapped["event_timestamp"].dt.hour
    mapped["city_zone"] = _map_zone(raw["PULocationID"])
    mapped["distance_km"] = (raw["trip_distance"] * MILES_TO_KM).round(2)
    mapped["fare_base_inr"] = (raw["fare_amount"] * USD_TO_INR).clip(lower=0).round(2)

    tip_ratio = raw["tip_amount"] / (raw["fare_amount"] + 0.01)
    mapped["surge_multiplier"] = _scale_surge_multiplier(tip_ratio).round(2)

    mapped["payment_method"] = _map_payment_method(raw["payment_type"])
    mapped["vehicle_type"] = "yellow_taxi"
    mapped["is_completed"] = True
    mapped["status"] = "completed"
    mapped["gross_fare_inr"] = (mapped["fare_base_inr"] * mapped["surge_multiplier"]).round(2)

    return mapped.dropna(subset=["event_timestamp"]).reset_index(drop=True)


def _build_historical_gold(silver_df: pd.DataFrame) -> pd.DataFrame:
    gold = (
        silver_df.groupby(["event_date", "event_hour", "city_zone"], as_index=False)
        .agg(
            ride_count=("ride_id", "count"),
            completed_rides=("is_completed", "sum"),
            cancelled_rides=("status", lambda s: (s == "cancelled").sum()),
            gross_revenue_inr=("gross_fare_inr", "sum"),
            avg_surge_multiplier=("surge_multiplier", "mean"),
        )
    )
    gold["completed_rides"] = gold["completed_rides"].astype(int)
    gold["cancelled_rides"] = gold["cancelled_rides"].astype(int)
    gold["gross_revenue_inr"] = gold["gross_revenue_inr"].round(2)
    gold["avg_surge_multiplier"] = gold["avg_surge_multiplier"].round(2)
    return gold


def run_batch_load() -> None:
    """One-time historical load: Silver rides_historical_nyc + Gold zone_demand_historical_nyc."""
    from deltalake import write_deltalake

    silver_df = load_to_silver()
    write_deltalake(SILVER_RIDES_HISTORICAL_NYC_TABLE, silver_df, mode="append")
    logger.info("Loaded %d rows into %s", len(silver_df), SILVER_RIDES_HISTORICAL_NYC_TABLE)

    gold_df = _build_historical_gold(silver_df)
    write_deltalake(GOLD_ZONE_DEMAND_HISTORICAL_NYC_TABLE, gold_df, mode="overwrite")
    logger.info("Built %d Gold rows into %s", len(gold_df), GOLD_ZONE_DEMAND_HISTORICAL_NYC_TABLE)


if __name__ == "__main__":
    run_batch_load()
