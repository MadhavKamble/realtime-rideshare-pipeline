from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_hourly_kpis(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df
        .groupBy("event_date", "event_hour", "city_zone")
        .agg(
            F.count("ride_id").alias("ride_count"),
            F.sum(F.when(F.col("is_completed"), 1).otherwise(0)).alias("completed_rides"),
            F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled_rides"),
            F.round(F.sum("gross_fare_inr"), 2).alias("gross_revenue_inr"),
            F.round(F.avg("surge_multiplier"), 2).alias("avg_surge_multiplier"),
        )
    )
