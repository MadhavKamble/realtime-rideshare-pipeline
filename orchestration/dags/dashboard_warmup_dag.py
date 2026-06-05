from __future__ import annotations

from datetime import datetime, timedelta
import json

import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from deltalake import DeltaTable

from storage.delta_config import GOLD_ZONE_DEMAND_TABLE, SILVER_RIDES_TABLE
from storage.redis_client import get_client

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="dashboard_warmup_dag",
    default_args=default_args,
    description="Warm up Redis cache with aggregated metrics for the dashboard — runs every 15 minutes",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["cache", "redis", "dashboard", "15min"],
) as dag:

    start = EmptyOperator(task_id="start")

    def _load_delta_table(table_path: str) -> pd.DataFrame:
        return DeltaTable(table_path).to_pandas()

    def compute_total_revenue(**context):
        """Sum up total revenue from Gold zone demand and cache it."""
        gold_df = _load_delta_table(GOLD_ZONE_DEMAND_TABLE)
        total_revenue = float(gold_df["gross_revenue_inr"].fillna(0).sum())

        client = get_client()
        client.setex("dashboard:total_revenue", 900, json.dumps({"amount": total_revenue}))
        print(f"✓ Cached total revenue: ₹{total_revenue:.2f}")

    def compute_avg_surge(**context):
        """Calculate average surge multiplier across all zones."""
        silver_df = _load_delta_table(SILVER_RIDES_TABLE)
        avg_surge = float(silver_df["surge_multiplier"].fillna(1.0).mean())

        client = get_client()
        client.setex("dashboard:avg_surge", 900, json.dumps({"surge": avg_surge}))
        print(f"✓ Cached avg surge: {avg_surge:.2f}x")

    def compute_rides_by_zone(**context):
        """Aggregate ride counts by zone and cache."""
        silver_df = _load_delta_table(SILVER_RIDES_TABLE)
        zone_counts = silver_df.groupby("city_zone")["ride_id"].count()

        client = get_client()
        for zone_name, ride_count in zone_counts.items():
            client.setex(
                f"dashboard:zone:{zone_name}:count",
                900,
                json.dumps({"count": int(ride_count)}),
            )

        print(f"✓ Cached ride counts for {len(zone_counts)} zones")

    def compute_driver_utilisation(**context):
        """Compute driver utilisation metrics."""
        silver_df = _load_delta_table(SILVER_RIDES_TABLE)
        total_rides = int(len(silver_df))
        completed_rides = int((silver_df["status"] == "completed").sum())
        utilisation = (completed_rides / total_rides * 100.0) if total_rides > 0 else 0.0

        client = get_client()
        client.setex(
            "dashboard:driver_utilisation",
            900,
            json.dumps({"utilisation_percent": float(utilisation), "total_rides": total_rides}),
        )
        print(f"✓ Cached driver utilisation: {utilisation:.1f}%")

    revenue_task = PythonOperator(
        task_id="compute_total_revenue",
        python_callable=compute_total_revenue,
    )
    surge_task = PythonOperator(
        task_id="compute_avg_surge",
        python_callable=compute_avg_surge,
    )
    zones_task = PythonOperator(
        task_id="compute_rides_by_zone",
        python_callable=compute_rides_by_zone,
    )
    util_task = PythonOperator(
        task_id="compute_driver_utilisation",
        python_callable=compute_driver_utilisation,
    )
    end = EmptyOperator(task_id="end")

    start >> [revenue_task, surge_task, zones_task, util_task] >> end
