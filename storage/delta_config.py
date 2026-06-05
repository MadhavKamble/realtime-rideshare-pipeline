from __future__ import annotations

import os

DELTA_BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH", "./data/delta/bronze")
DELTA_SILVER_PATH = os.getenv("DELTA_SILVER_PATH", "./data/delta/silver")
DELTA_GOLD_PATH = os.getenv("DELTA_GOLD_PATH", "./data/delta/gold")

BRONZE_RIDES_TABLE = f"{DELTA_BRONZE_PATH}/rides"
BRONZE_DRIVERS_TABLE = f"{DELTA_BRONZE_PATH}/drivers"
BRONZE_PAYMENTS_TABLE = f"{DELTA_BRONZE_PATH}/payments"

SILVER_RIDES_TABLE = f"{DELTA_SILVER_PATH}/rides_clean"
SILVER_DRIVERS_TABLE = f"{DELTA_SILVER_PATH}/drivers_clean"
SILVER_PAYMENTS_TABLE = f"{DELTA_SILVER_PATH}/payments_clean"

GOLD_HOURLY_KPIS_TABLE = f"{DELTA_GOLD_PATH}/hourly_kpis"
GOLD_ZONE_DEMAND_TABLE = f"{DELTA_GOLD_PATH}/zone_demand"
