from __future__ import annotations

import os

import pandas as pd
from pyspark.sql import SparkSession
from storage.spark_session import build_delta_spark

from storage.delta_config import DELTA_GOLD_PATH


def _spark() -> SparkSession:
    return build_delta_spark("DashboardDeltaReader")


def read_gold_heatmap() -> pd.DataFrame:
    spark = _spark()
    path = os.path.join(DELTA_GOLD_PATH, "zone_demand")
    if not os.path.exists(path):
        return pd.DataFrame(columns=["lon_grid", "lat_grid", "ride_count"])
    return spark.read.format("delta").load(path).toPandas()
