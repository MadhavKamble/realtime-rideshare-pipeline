from __future__ import annotations

from pyspark.sql import SparkSession
from storage.spark_session import build_delta_spark

from processing.batch.delta_utils import upsert_delta_table
from processing.batch.bronze_to_silver import transform_bronze_to_silver
from storage.delta_config import BRONZE_RIDES_TABLE, SILVER_RIDES_TABLE


def _spark() -> SparkSession:
    return build_delta_spark("delta-demo")


def demo_upsert():
    spark = _spark()
    # read bronze — in a real run this would be the Delta bronze path. Here we show the API.
    bronze_df = spark.read.format("delta").load(BRONZE_RIDES_TABLE) if spark._jvm is not None else None

    if bronze_df is None:
        print("No Bronze Delta data available in this environment. This demo shows the API only.")
        return

    silver_df = transform_bronze_to_silver(bronze_df)
    upsert_delta_table(spark, silver_df, SILVER_RIDES_TABLE, key_columns=["ride_id"])


if __name__ == "__main__":
    demo_upsert()
