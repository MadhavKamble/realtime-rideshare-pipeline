from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def transform_bronze_to_silver(bronze_df: DataFrame) -> DataFrame:
    return (
        bronze_df
        .dropDuplicates(["ride_id"])
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("event_hour", F.hour("event_ts"))
        .withColumn("is_completed", F.col("status") == F.lit("completed"))
        .withColumn("gross_fare_inr", F.round(F.col("fare_base_inr") * F.col("surge_multiplier"), 2))
    )
