"""Standalone Gold refresh — runs Silver→Gold aggregation and writes to Delta.

Use this to populate the Gold table without Airflow:
    docker exec <spark-container> python3 -m processing.batch.run_gold_standalone
"""
from __future__ import annotations

import sys

from storage.spark_session import build_delta_spark
from storage.delta_config import SILVER_RIDES_TABLE, GOLD_ZONE_DEMAND_TABLE
from processing.batch.silver_to_gold import build_hourly_kpis
from processing.batch.delta_utils import upsert_delta_table


def main() -> None:
    print("Building Spark session...")
    spark = build_delta_spark("GoldStandaloneRefresh")

    print(f"Reading Silver from: {SILVER_RIDES_TABLE}")
    silver_df = spark.read.format("delta").load(SILVER_RIDES_TABLE)
    row_count = silver_df.count()
    print(f"Silver rows available: {row_count:,}")

    if row_count == 0:
        print("Silver table is empty — nothing to aggregate. Exiting.")
        sys.exit(1)

    print("Building hourly KPIs...")
    gold_df = build_hourly_kpis(silver_df)
    gold_count = gold_df.count()
    print(f"Gold rows to write: {gold_count:,}")

    print(f"Upserting to Gold: {GOLD_ZONE_DEMAND_TABLE}")
    try:
        upsert_delta_table(
            spark,
            gold_df,
            GOLD_ZONE_DEMAND_TABLE,
            key_columns=["event_date", "event_hour", "city_zone"],
        )
    except Exception as exc:
        print(f"Gold MERGE failed, table left unchanged: {exc}")
        sys.exit(1)

    print(f"Done. Gold zone_demand table now has {gold_count:,} rows.")


if __name__ == "__main__":
    main()
