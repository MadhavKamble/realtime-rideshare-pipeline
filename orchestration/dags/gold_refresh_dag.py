from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=45),
}


def _run_gold_refresh(**context):
    """Read Silver Delta table with deltalake, aggregate to Gold KPIs, write back."""
    from deltalake import DeltaTable, write_deltalake
    from storage.delta_config import SILVER_RIDES_TABLE, GOLD_ZONE_DEMAND_TABLE

    silver_df = DeltaTable(SILVER_RIDES_TABLE).to_pandas()
    print(f"✓ Loaded {len(silver_df):,} Silver rows from {SILVER_RIDES_TABLE}")

    silver_df["gross_fare_inr"] = pd.to_numeric(silver_df["gross_fare_inr"], errors="coerce").fillna(0.0)
    silver_df["surge_multiplier"] = pd.to_numeric(silver_df["surge_multiplier"], errors="coerce").fillna(1.0)
    silver_df["event_hour"] = pd.to_numeric(silver_df["event_hour"], errors="coerce").fillna(0).astype(int)
    silver_df["is_completed"] = silver_df["is_completed"].astype(bool)
    silver_df["event_date"] = silver_df["event_date"].astype(str)

    gold_df = (
        silver_df.groupby(["event_date", "event_hour", "city_zone"], as_index=False)
        .agg(
            ride_count=("ride_id", "count"),
            completed_rides=("is_completed", "sum"),
            cancelled_rides=("status", lambda s: (s == "cancelled").sum()),
            gross_revenue_inr=("gross_fare_inr", "sum"),
            avg_surge_multiplier=("surge_multiplier", "mean"),
        )
    )
    gold_df["completed_rides"] = gold_df["completed_rides"].astype(int)
    gold_df["cancelled_rides"] = gold_df["cancelled_rides"].astype(int)
    gold_df["gross_revenue_inr"] = gold_df["gross_revenue_inr"].round(2)
    gold_df["avg_surge_multiplier"] = gold_df["avg_surge_multiplier"].round(2)

    write_deltalake(GOLD_ZONE_DEMAND_TABLE, gold_df, mode="overwrite")
    print(f"✓ Gold refresh complete: {len(gold_df)} rows in zone_demand (ds={context['ds']})")


def _validate_gold_counts(**context):
    """Verify Gold zone_demand table is not empty."""
    from deltalake import DeltaTable
    from storage.delta_config import GOLD_ZONE_DEMAND_TABLE

    gold_df = DeltaTable(GOLD_ZONE_DEMAND_TABLE).to_pandas()
    count = len(gold_df)

    if count == 0:
        raise ValueError("Gold zone_demand table is empty after refresh!")

    print(f"✓ Gold validation passed: {count} rows in zone_demand")
    print(gold_df.groupby("city_zone")["gross_revenue_inr"].sum().sort_values(ascending=False).to_string())


def _alert_on_anomaly(**context):
    """Check for business logic anomalies — peak hours should have more rides than off-peak."""
    from deltalake import DeltaTable
    from storage.delta_config import GOLD_ZONE_DEMAND_TABLE

    gold_df = DeltaTable(GOLD_ZONE_DEMAND_TABLE).to_pandas()

    peak = gold_df[gold_df["event_hour"].between(8, 21)]["ride_count"].mean() or 0
    off_peak = gold_df[~gold_df["event_hour"].between(8, 21)]["ride_count"].mean() or 0

    if peak < off_peak * 0.5:
        print(f"⚠ Anomaly: peak avg ({peak:.0f}) < off-peak avg ({off_peak:.0f})")
    else:
        print(f"✓ No anomalies: peak avg ({peak:.0f}) >= off-peak avg ({off_peak:.0f})")


with DAG(
    dag_id="gold_refresh_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 * * * *",
    catchup=False,
    tags=["gold", "kpi", "hourly"],
    default_args=default_args,
) as dag:
    start = EmptyOperator(task_id="start")
    silver_to_gold = PythonOperator(task_id="silver_to_gold", python_callable=_run_gold_refresh)
    validate = PythonOperator(task_id="validate_gold_counts", python_callable=_validate_gold_counts)
    alert = PythonOperator(task_id="alert_on_anomaly", python_callable=_alert_on_anomaly)
    end = EmptyOperator(task_id="end")

    start >> silver_to_gold >> validate >> alert >> end
