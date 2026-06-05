from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}


def check_null_ride_ids(**context):
    from deltalake import DeltaTable
    from storage.delta_config import SILVER_RIDES_TABLE

    df = DeltaTable(SILVER_RIDES_TABLE).to_pandas()
    null_count = df["ride_id"].isna().sum()

    if null_count > 0:
        raise ValueError(f"Found {null_count} NULL ride_ids in Silver rides table!")
    print(f"✓ Null check passed: 0 NULL ride_ids across {len(df):,} rows")


def check_gold_row_counts(**context):
    from deltalake import DeltaTable
    from storage.delta_config import GOLD_ZONE_DEMAND_TABLE

    df = DeltaTable(GOLD_ZONE_DEMAND_TABLE).to_pandas()
    if len(df) == 0:
        raise ValueError("Gold zone_demand table is empty!")
    print(f"✓ Gold zone_demand check passed: {len(df)} rows across {df['city_zone'].nunique()} zones")


def check_timestamps_in_range(**context):
    import pandas as pd
    from datetime import datetime, timedelta, timezone

    from deltalake import DeltaTable
    from storage.delta_config import SILVER_RIDES_TABLE

    df = DeltaTable(SILVER_RIDES_TABLE).to_pandas()
    cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
    df["event_ts"] = pd.to_datetime(df["event_ts"], utc=True, errors="coerce")
    old_count = (df["event_ts"] < cutoff).sum()
    total = len(df)
    old_pct = old_count / total if total > 0 else 0

    if old_pct > 0.10:
        print(f"⚠ Warning: {old_count}/{total} ({old_pct:.1%}) events are older than 2 hours")
    else:
        print(f"✓ Timestamp check passed: {total - old_count}/{total} events are recent")


def check_surge_multiplier_bounds(**context):
    from deltalake import DeltaTable
    from storage.delta_config import SILVER_RIDES_TABLE

    df = DeltaTable(SILVER_RIDES_TABLE).to_pandas()
    out_of_bounds = ((df["surge_multiplier"] < 1.0) | (df["surge_multiplier"] > 3.5)).sum()

    if out_of_bounds > 0:
        raise ValueError(f"Found {out_of_bounds} rides with surge_multiplier outside [1.0, 3.5]!")
    print(f"✓ Surge multiplier check passed: all {len(df):,} rows within [1.0, 3.5]")


with DAG(
    dag_id="data_quality_dag",
    default_args=default_args,
    description="Data quality checks on Silver and Gold tables — runs every 30 minutes",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["quality", "validation", "30min"],
) as dag:
    start = EmptyOperator(task_id="start")
    check_nulls = PythonOperator(task_id="check_null_ride_ids", python_callable=check_null_ride_ids)
    check_gold = PythonOperator(task_id="check_gold_row_counts", python_callable=check_gold_row_counts)
    check_ts = PythonOperator(task_id="check_timestamps_in_range", python_callable=check_timestamps_in_range)
    check_surge = PythonOperator(task_id="check_surge_multiplier_bounds", python_callable=check_surge_multiplier_bounds)
    end = EmptyOperator(task_id="end")

    start >> [check_nulls, check_gold, check_ts, check_surge] >> end
