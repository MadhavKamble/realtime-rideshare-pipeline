# Real-Time Ride-Sharing Analytics Platform
## Complete AI Agent Build Specification

> **Purpose:** This document is a fully self-contained specification for building an end-to-end Big Data analytics platform modelled on Uber/Ola/Rapido internals. Every architectural decision, schema, config value, code pattern, file structure, and implementation detail is documented here. An AI agent must be able to read this file and build the project from scratch without any additional context.

---

## Table of Contents

1. [Project Summary](#1-project-summary)
2. [Exact Tech Stack & Versions](#2-exact-tech-stack--versions)
3. [Repository Structure](#3-repository-structure)
4. [Environment Setup](#4-environment-setup)
5. [Module 1 — Synthetic Data Simulator](#5-module-1--synthetic-data-simulator)
6. [Module 2 — Apache Kafka](#6-module-2--apache-kafka)
7. [Module 3 — PySpark Batch Processing](#7-module-3--pyspark-batch-processing)
8. [Module 4 — PySpark Structured Streaming](#8-module-4--pyspark-structured-streaming)
9. [Module 5 — Delta Lake Medallion Architecture](#9-module-5--delta-lake-medallion-architecture)
10. [Module 6 — ML Model & MLflow](#10-module-6--ml-model--mlflow)
11. [Module 7 — Apache Airflow Orchestration](#11-module-7--apache-airflow-orchestration)
12. [Module 8 — Redis Cache Layer](#12-module-8--redis-cache-layer)
13. [Module 9 — Streamlit Dashboard](#13-module-9--streamlit-dashboard)
14. [Module 10 — Docker Compose Full Stack](#14-module-10--docker-compose-full-stack)
15. [Data Schemas — Complete Definitions](#15-data-schemas--complete-definitions)
16. [Configuration Reference](#16-configuration-reference)
17. [Testing Strategy](#17-testing-strategy)
18. [Known Failure Modes & Fixes](#18-known-failure-modes--fixes)
19. [Interview Q&A Answers](#19-interview-qa-answers)
20. [Resume Bullets by Role](#20-resume-bullets-by-role)
21. [GitHub README Template](#21-github-readme-template)
22. [Build Order & Agent Instructions](#22-build-order--agent-instructions)

---

## 1. Project Summary

### What This Is
A production-grade, end-to-end real-time data pipeline that simulates, ingests, processes, stores, analyzes, and visualizes ride-sharing data. Every component maps to systems used in production at Uber, Ola, PhonePe, Swiggy, and Flipkart.

### What It Demonstrates
| Concern | Implementation |
|---|---|
| Real-time ingestion | Apache Kafka with 3 topics, 12 total partitions |
| Stream processing | PySpark Structured Streaming with watermarks |
| Reliable storage | Delta Lake with ACID, time travel, schema evolution |
| Sub-second reads | Redis cache for live dashboard data |
| ML at scale | XGBoost surge predictor served as PySpark Pandas UDF |
| Experiment tracking | MLflow with model registry |
| Pipeline orchestration | Apache Airflow with 4 production DAGs |
| Visualization | Streamlit dashboard with live maps and KPIs |
| Reproducibility | Docker Compose — full stack with one command |

### Why This Project Is Resume-Worthy
- Covers Data Engineer, Data Analyst, ML Engineer, and Analytics Engineer roles simultaneously
- Real-time streaming is rare in fresher portfolios — interviewers notice immediately
- Medallion architecture is the Databricks/Delta Lake industry standard
- MLflow + model registry shows production ML awareness, not just notebooks
- Every tool can be defended with a "why this over alternatives" answer

### Target Roles
- **Data Engineer** → Kafka, PySpark Streaming, Delta Lake, Airflow, Docker
- **Data Analyst** → SparkSQL, Gold layer design, Streamlit dashboard
- **ML Engineer** → Feature engineering, XGBoost, MLflow, Pandas UDF serving
- **Analytics Engineer** → Medallion architecture, data modeling, Gold schema

---

## 2. Exact Tech Stack & Versions

> **CRITICAL FOR AGENT:** Always use these exact versions. Version mismatches cause cryptic Java/Python errors.
> **Note:** Airflow uses the `deltalake` Python library (no JVM). Only the Spark container uses `pyspark` + `delta-spark`.

```
Python:               3.11.x
Apache Kafka:         7.9.6        (confluentinc/cp-kafka:7.9.6 Docker image)
Apache Zookeeper:     7.9.6        (confluentinc/cp-zookeeper:7.9.6 Docker image)
Apache Spark:         3.5.1        (custom Dockerfile.spark based on apache/spark-py)
Delta Lake (Spark):   3.1.0        (pip: delta-spark==3.1.0 — Spark container only)
Delta Lake (Python):  0.17.4       (pip: deltalake==0.17.4 — Airflow + Dashboard)
PyArrow:              15.0.2       (pip: pyarrow==15.0.2 — required by deltalake)
kafka-python:         2.0.2        (pip: kafka-python==2.0.2)
pyspark:              3.5.1        (pip: pyspark==3.5.1 — Spark container only)
redis:                7.2.x        (redis:7.2 Docker image)
redis-py:             5.0.1        (pip: redis==5.0.1)
xgboost:              2.0.3        (pip: xgboost==2.0.3)
scikit-learn:         1.3.2        (pip: scikit-learn==1.3.2)
mlflow:               2.11.3       (pip: mlflow==2.11.3)
apache-airflow:       2.8.1        (apache/airflow:2.8.1 Docker image)
streamlit:            1.32.2       (pip: streamlit==1.32.2)
pandas:               2.0.3        (pip: pandas==2.0.3 — Airflow) / 2.2.1 (Dashboard)
faker:                24.x         (pip: Faker==24.3.0)
pydeck:               0.8.x        (pip: pydeck==0.8.1b0)
folium:               0.16.x       (pip: folium==0.16.0)
delta-spark JAR:      io.delta:delta-spark_2.12:3.1.0
kafka-spark JAR:      org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
```

### Requirements files by container

```
requirements-airflow.txt    deltalake==0.17.4, pyarrow==15.0.2, mlflow==2.11.3,
                            xgboost==2.0.3, scikit-learn==1.3.2, pandas==2.0.3,
                            redis==5.0.1, python-dotenv==1.0.1
                            *** NO pyspark, NO delta-spark ***

requirements-spark.txt      pyspark==3.5.1, delta-spark==3.1.0, kafka-python==2.0.2

requirements-dashboard.txt  streamlit==1.32.2, pandas==2.2.1, pydeck==0.8.1b0,
                            folium==0.16.0, redis==5.0.1, python-dotenv==1.0.1
                            *** NO streamlit-autorefresh (auto-refresh via JS) ***

requirements-simulator.txt  kafka-python==2.0.2, Faker==24.x, numpy

requirements-live-writer.txt kafka-python==2.0.2, redis==5.0.1
```

### SparkSession base config (Spark container only)
```python
# storage/spark_session.py
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RideshareAnalytics")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.1.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
```

### Delta read in Airflow (no Spark)
```python
# Used in all 4 Airflow DAGs
from deltalake import DeltaTable, write_deltalake
df = DeltaTable("/data/delta/silver/rides_clean").to_pandas()
write_deltalake("/data/delta/gold/zone_demand", gold_df, mode="overwrite")
```

---

## 3. Repository Structure

```
rideshare-analytics/
│
├── README.md                          # Project overview, architecture diagram, setup
├── docker-compose.yml                 # Full stack: Kafka, Spark, Delta, Redis, Airflow, Streamlit
├── requirements.txt                   # All Python dependencies pinned
├── .env                               # Environment variables (never commit secrets)
├── .gitignore
│
├── simulator/
│   ├── __init__.py
│   ├── config.py                      # Zone definitions, vehicle types, peak hours
│   ├── ride_generator.py              # Generates ride event dicts
│   ├── driver_generator.py            # Generates driver event dicts
│   ├── payment_generator.py           # Generates payment event dicts
│   ├── kafka_producer.py              # Publishes all 3 event types to Kafka
│   └── run_simulator.py               # Entry point: runs all generators in threads
│
├── ingestion/
│   ├── __init__.py
│   └── kafka_config.py                # Kafka bootstrap servers, topic names, partition counts
│
├── processing/
│   ├── __init__.py
│   ├── batch/
│   │   ├── bronze_to_silver.py        # Batch Silver layer transformation
│   │   └── silver_to_gold.py          # Batch Gold layer aggregation
│   └── streaming/
│       ├── kafka_to_bronze.py         # PySpark Streaming: Kafka → Bronze Delta
│       ├── bronze_to_silver_stream.py # PySpark Streaming: Bronze → Silver Delta
│       └── surge_prediction_stream.py # Loads MLflow model, writes predictions
│
├── storage/
│   ├── __init__.py
│   ├── delta_config.py                # Delta Lake table paths, partition keys
│   └── redis_client.py                # Redis connection + cache helpers
│
├── ml/
│   ├── __init__.py
│   ├── feature_engineering.py         # All feature transforms, encodings
│   ├── train_surge_model.py           # XGBoost training + MLflow logging
│   ├── evaluate_model.py              # MAE, RMSE, R² computation
│   └── serve_predictions.py           # Pandas UDF wrapper for PySpark serving
│
├── orchestration/
│   ├── dags/
│   │   ├── gold_refresh_dag.py        # Hourly Silver → Gold batch refresh
│   │   ├── ml_retrain_dag.py          # Daily 2am model retrain
│   │   ├── data_quality_dag.py        # 30min data quality checks
│   │   └── dashboard_warmup_dag.py    # 15min Redis cache warmup
│   └── plugins/
│       └── delta_operators.py         # Custom Airflow operator for Delta Lake writes
│
├── dashboard/
│   ├── app.py                         # Streamlit main app
│   ├── components/
│   │   ├── live_counter.py            # Rides/minute big number widget
│   │   ├── demand_heatmap.py          # PyDeck map component
│   │   ├── revenue_charts.py          # Bar + line charts
│   │   ├── driver_utilisation.py      # Driver KPI table + chart
│   │   └── pipeline_health.py         # Airflow + Delta freshness indicators
│   └── data_connectors/
│       ├── delta_reader.py            # Read Gold Delta tables
│       └── redis_reader.py            # Read live data from Redis
│
├── tests/
│   ├── test_simulator.py
│   ├── test_silver_transforms.py
│   ├── test_gold_aggregations.py
│   ├── test_feature_engineering.py
│   └── test_data_quality.py
│
├── notebooks/
│   ├── 01_explore_bronze.ipynb
│   ├── 02_explore_silver.ipynb
│   ├── 03_gold_kpi_analysis.ipynb
│   └── 04_ml_experimentation.ipynb
│
└── data/
    ├── delta/                         # Delta Lake tables (local dev)
    │   ├── bronze/
    │   ├── silver/
    │   └── gold/
    └── mlflow/                        # MLflow tracking store (local)
```

---

## 4. Environment Setup

### 4.1 Prerequisites (Linux — Ubuntu 20.04+)
```bash
# Verify versions
python3 --version    # must be 3.11.x
java --version       # must be Java 11 or 17 (Spark requirement)
docker --version     # must be 24.x+
docker compose version  # must be 2.x+
git --version

# Install Java 17 if missing
sudo apt update && sudo apt install -y openjdk-17-jdk
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
```

### 4.2 Project Bootstrap
```bash
# Create project
mkdir rideshare-analytics && cd rideshare-analytics
git init
git remote add origin https://github.com/YOUR_USERNAME/rideshare-analytics.git

# Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install all dependencies
pip install --upgrade pip
pip install \
  pyspark==3.5.1 \
  delta-spark==3.1.0 \
  kafka-python==2.0.2 \
  redis==5.0.1 \
  xgboost==2.0.3 \
  scikit-learn==1.4.2 \
  mlflow==2.11.3 \
  apache-airflow==2.8.1 \
  streamlit==1.32.2 \
  pandas==2.2.1 \
  Faker==24.3.0 \
  pydeck==0.8.1b0 \
  folium==0.16.0 \
  pytest==8.x \
  python-dotenv==1.0.x

# Freeze
pip freeze > requirements.txt
```

### 4.3 Environment Variables (.env)
```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC_RIDES=ride-events
KAFKA_TOPIC_DRIVERS=driver-events
KAFKA_TOPIC_PAYMENTS=payment-events

# Delta Lake (local dev paths)
DELTA_BRONZE_PATH=./data/delta/bronze
DELTA_SILVER_PATH=./data/delta/silver
DELTA_GOLD_PATH=./data/delta/gold

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# MLflow
MLFLOW_TRACKING_URI=./data/mlflow
MLFLOW_EXPERIMENT_NAME=surge_price_prediction

# Airflow
AIRFLOW_HOME=./orchestration
```

### 4.4 .gitignore
```
venv/
__pycache__/
*.pyc
.env
data/delta/
data/mlflow/
*.egg-info/
.ipynb_checkpoints/
*.log
```

---

## 5. Module 1 — Synthetic Data Simulator

### 5.1 City Zone Definitions (simulator/config.py)

```python
# 5 zones modelled after a generic Indian metro (Bangalore/Mumbai blend)
CITY_ZONES = {
    "airport": {
        "lat_center": 13.1986,  "lon_center": 77.7066,
        "lat_std": 0.005,       "lon_std": 0.005,
        "demand_multiplier": 1.8,   # always busy
        "cancellation_rate": 0.04,
    },
    "railway_station": {
        "lat_center": 12.9784,  "lon_center": 77.5707,
        "lat_std": 0.008,       "lon_std": 0.008,
        "demand_multiplier": 2.2,
        "cancellation_rate": 0.10,
    },
    "cbd": {  # Central Business District
        "lat_center": 12.9716,  "lon_center": 77.5946,
        "lat_std": 0.015,       "lon_std": 0.015,
        "demand_multiplier": 1.5,
        "cancellation_rate": 0.07,
    },
    "mall": {
        "lat_center": 12.9352,  "lon_center": 77.6245,
        "lat_std": 0.010,       "lon_std": 0.010,
        "demand_multiplier": 1.2,
        "cancellation_rate": 0.06,
    },
    "residential": {
        "lat_center": 13.0200,  "lon_center": 77.5800,
        "lat_std": 0.030,       "lon_std": 0.030,
        "demand_multiplier": 1.0,
        "cancellation_rate": 0.08,
    },
}

VEHICLE_TYPES = ["bike", "auto", "cab_economy", "cab_premium"]
VEHICLE_BASE_FARE = {"bike": 15, "auto": 25, "cab_economy": 40, "cab_premium": 80}  # INR/km
VEHICLE_WEIGHTS = [0.30, 0.25, 0.35, 0.10]  # probability weights

# Peak hour definitions
PEAK_HOURS_MORNING = range(8, 11)    # 8am–10am
PEAK_HOURS_EVENING = range(17, 21)   # 5pm–8pm
PEAK_DEMAND_MULTIPLIER = 3.0
OFF_PEAK_DEMAND_MULTIPLIER = 0.3     # 2am–5am

# Late event config
LATE_EVENT_PROBABILITY = 0.05        # 5% of events arrive late
LATE_EVENT_DELAY_MS_MIN = 30_000     # 30 seconds
LATE_EVENT_DELAY_MS_MAX = 120_000    # 2 minutes

# Weather proxy (cycles every 6 hours)
WEATHER_STATES = ["clear", "cloudy", "rain"]
WEATHER_DEMAND_MULTIPLIER = {"clear": 1.0, "cloudy": 1.1, "rain": 1.6}
```

### 5.2 Ride Event Generator (simulator/ride_generator.py)

```python
import uuid
import random
import numpy as np
from datetime import datetime, timezone
from simulator.config import (
    CITY_ZONES, VEHICLE_TYPES, VEHICLE_BASE_FARE, VEHICLE_WEIGHTS,
    PEAK_HOURS_MORNING, PEAK_HOURS_EVENING, PEAK_DEMAND_MULTIPLIER,
    OFF_PEAK_DEMAND_MULTIPLIER, LATE_EVENT_PROBABILITY,
    LATE_EVENT_DELAY_MS_MIN, LATE_EVENT_DELAY_MS_MAX,
    WEATHER_STATES, WEATHER_DEMAND_MULTIPLIER
)


def get_current_demand_multiplier(hour: int, weather: str) -> float:
    if hour in PEAK_HOURS_MORNING or hour in PEAK_HOURS_EVENING:
        base = PEAK_DEMAND_MULTIPLIER
    elif hour in range(2, 6):
        base = OFF_PEAK_DEMAND_MULTIPLIER
    else:
        base = 1.0
    return base * WEATHER_DEMAND_MULTIPLIER[weather]


def compute_surge_multiplier(zone: str, hour: int, weather: str) -> float:
    """Surge is a function of zone demand + time + weather. Capped at 3.5."""
    demand = get_current_demand_multiplier(hour, weather)
    zone_factor = CITY_ZONES[zone]["demand_multiplier"]
    raw = (demand * zone_factor) / 2.0   # normalize
    surge = 1.0 + min(raw - 1.0, 2.5)   # floor=1.0, ceiling=3.5
    return round(max(1.0, min(3.5, surge)), 2)


def generate_ride_event(weather: str = "clear") -> dict:
    now = datetime.now(timezone.utc)
    hour = now.hour

    # Choose pickup zone weighted by demand
    zone_weights = [
        CITY_ZONES[z]["demand_multiplier"] * get_current_demand_multiplier(hour, weather)
        for z in CITY_ZONES
    ]
    zone_name = random.choices(list(CITY_ZONES.keys()), weights=zone_weights, k=1)[0]
    zone = CITY_ZONES[zone_name]

    # Generate GPS coordinates clustered around zone center
    pickup_lat = np.random.normal(zone["lat_center"], zone["lat_std"])
    pickup_lon = np.random.normal(zone["lon_center"], zone["lon_std"])

    # Dropoff in a different (random) zone
    dropoff_zone = CITY_ZONES[random.choice(list(CITY_ZONES.keys()))]
    dropoff_lat = np.random.normal(dropoff_zone["lat_center"], dropoff_zone["lat_std"])
    dropoff_lon = np.random.normal(dropoff_zone["lon_center"], dropoff_zone["lon_std"])

    # GPS drift: 1% chance
    if random.random() < 0.01:
        pickup_lat += random.uniform(-0.01, 0.01)
        pickup_lon += random.uniform(-0.01, 0.01)

    vehicle = random.choices(VEHICLE_TYPES, weights=VEHICLE_WEIGHTS, k=1)[0]
    distance_km = round(random.uniform(1.5, 25.0), 2)
    surge = compute_surge_multiplier(zone_name, hour, weather)
    base_fare = VEHICLE_BASE_FARE[vehicle]
    fare = round(distance_km * base_fare * surge, 2)

    # Cancellation
    cancel_prob = CITY_ZONES[zone_name]["cancellation_rate"]
    status = "cancelled" if random.random() < cancel_prob else random.choice(
        ["requested", "accepted", "started", "completed"]
    )

    # Late event simulation
    event_delay_ms = 0
    if random.random() < LATE_EVENT_PROBABILITY:
        event_delay_ms = random.randint(LATE_EVENT_DELAY_MS_MIN, LATE_EVENT_DELAY_MS_MAX)

    return {
        "ride_id":           str(uuid.uuid4()),
        "event_timestamp":   now.isoformat(),
        "driver_id":         str(uuid.uuid4()),
        "user_id":           str(uuid.uuid4()),
        "status":            status,
        "pickup_lat":        round(pickup_lat, 6),
        "pickup_lon":        round(pickup_lon, 6),
        "dropoff_lat":       round(dropoff_lat, 6),
        "dropoff_lon":       round(dropoff_lon, 6),
        "city_zone":         zone_name,
        "distance_km":       distance_km,
        "vehicle_type":      vehicle,
        "fare_base_inr":     fare,
        "surge_multiplier":  surge,
        "weather":           weather,
        "event_delay_ms":    event_delay_ms,
        "schema_version":    "1.0",
    }
```

### 5.3 Kafka Producer (simulator/kafka_producer.py)

```python
import json
import time
import random
import threading
from kafka import KafkaProducer
from simulator.ride_generator import generate_ride_event
from simulator.driver_generator import generate_driver_event
from simulator.payment_generator import generate_payment_event
from simulator.config import WEATHER_STATES
import os
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_RIDES = os.getenv("KAFKA_TOPIC_RIDES", "ride-events")
TOPIC_DRIVERS = os.getenv("KAFKA_TOPIC_DRIVERS", "driver-events")
TOPIC_PAYMENTS = os.getenv("KAFKA_TOPIC_PAYMENTS", "payment-events")


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",                   # wait for all replicas
        retries=5,
        max_in_flight_requests_per_connection=1,   # preserve ordering
    )


def ride_producer_loop(producer: KafkaProducer, events_per_second: float = 8.0):
    """Produce ride events. Rate varies by simulated hour."""
    weather_cycle = WEATHER_STATES * 100
    while True:
        weather = weather_cycle[int(time.time() / 21600) % len(WEATHER_STATES)]
        event = generate_ride_event(weather=weather)
        # Partition key = city_zone ensures zone events go to same partition
        producer.send(TOPIC_RIDES, key=event["city_zone"], value=event)
        time.sleep(1.0 / events_per_second)


def driver_producer_loop(producer: KafkaProducer, events_per_second: float = 3.0):
    while True:
        event = generate_driver_event()
        producer.send(TOPIC_DRIVERS, key=event["driver_id"], value=event)
        time.sleep(1.0 / events_per_second)


def payment_producer_loop(producer: KafkaProducer, events_per_second: float = 8.0):
    while True:
        event = generate_payment_event()
        producer.send(TOPIC_PAYMENTS, key=event["ride_id"], value=event)
        time.sleep(1.0 / events_per_second)


def run():
    producer = make_producer()
    threads = [
        threading.Thread(target=ride_producer_loop, args=(producer,), daemon=True),
        threading.Thread(target=driver_producer_loop, args=(producer,), daemon=True),
        threading.Thread(target=payment_producer_loop, args=(producer,), daemon=True),
    ]
    for t in threads:
        t.start()
    print("Simulator running. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        producer.flush()
        print("Producer flushed. Exiting.")


if __name__ == "__main__":
    run()
```

---

## 6. Module 2 — Apache Kafka

### 6.1 Kafka Setup via Docker (before Docker Compose is fully ready)
```bash
# Standalone Kafka for early development
docker network create rideshare-net

docker run -d --name zookeeper --network rideshare-net \
  -e ALLOW_ANONYMOUS_LOGIN=yes \
  bitnami/zookeeper:3.8

docker run -d --name kafka --network rideshare-net \
  -p 9092:9092 \
  -e KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e ALLOW_PLAINTEXT_LISTENER=yes \
  bitnami/kafka:3.6
```

### 6.2 Topic Creation
```bash
# Create all 3 topics with correct partition counts
docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic ride-events \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=604800000   # 7 days

docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic driver-events \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000

docker exec kafka kafka-topics.sh --create \
  --bootstrap-server localhost:9092 \
  --topic payment-events \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000

# Verify
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
```

### 6.3 Why These Partition Counts
- `ride-events`: 6 partitions → allows 6 parallel Spark tasks; matches 5 city zones + 1 for overflow
- `driver-events`: 3 partitions → lower volume, 3 tasks sufficient
- `payment-events`: 3 partitions → 1:1 with ride events but delayed, 3 partitions safe

### 6.4 Consumer Group Design
```
Consumer Group: spark-bronze-writer     → reads all 3 topics → writes Bronze
Consumer Group: spark-silver-writer     → reads Bronze change feed (not Kafka)
Consumer Group: dashboard-live-reader   → reads ride-events only → Redis
```

### 6.5 Kafka Verification Commands
```bash
# Check messages are arriving (consume 10 messages)
docker exec kafka kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic ride-events \
  --max-messages 10 \
  --from-beginning

# Check consumer group lag
docker exec kafka kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group spark-bronze-writer
```

---

## 7. Module 3 — PySpark Batch Processing

### 7.1 Purpose
Before writing streaming jobs, validate all transformations in batch mode using Kafka data dumped to JSON files. This is the correct learning sequence — batch first, then streaming.

### 7.2 Bronze → Silver Batch (processing/batch/bronze_to_silver.py)

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, FloatType
)
import os
from dotenv import load_dotenv

load_dotenv()
BRONZE_PATH = os.getenv("DELTA_BRONZE_PATH")
SILVER_PATH = os.getenv("DELTA_SILVER_PATH")


def get_spark():
    from pyspark.sql import SparkSession
    return (SparkSession.builder
            .appName("BronzeToSilverBatch")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


def transform_rides(df):
    """All Silver transformations for ride events."""
    return (
        df
        # 1. Cast types
        .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
        .withColumn("pickup_lat",       F.col("pickup_lat").cast(DoubleType()))
        .withColumn("pickup_lon",       F.col("pickup_lon").cast(DoubleType()))
        .withColumn("dropoff_lat",      F.col("dropoff_lat").cast(DoubleType()))
        .withColumn("dropoff_lon",      F.col("dropoff_lon").cast(DoubleType()))
        .withColumn("distance_km",      F.col("distance_km").cast(DoubleType()))
        .withColumn("fare_base_inr",    F.col("fare_base_inr").cast(DoubleType()))
        .withColumn("surge_multiplier", F.col("surge_multiplier").cast(FloatType()))
        .withColumn("event_delay_ms",   F.col("event_delay_ms").cast(IntegerType()))

        # 2. Drop rows with null critical fields
        .dropna(subset=["ride_id", "driver_id", "event_timestamp", "city_zone"])

        # 3. Filter invalid GPS (must be within India bounding box)
        .filter(F.col("pickup_lat").between(8.0, 37.0))
        .filter(F.col("pickup_lon").between(68.0, 97.0))

        # 4. Derived columns
        .withColumn("hour_of_day",    F.hour("event_timestamp"))
        .withColumn("day_of_week",    F.dayofweek("event_timestamp"))  # 1=Sun, 7=Sat
        .withColumn("date_partition", F.to_date("event_timestamp"))
        .withColumn("is_peak_hour",
                    F.when(F.col("hour_of_day").between(8, 10), 1)
                     .when(F.col("hour_of_day").between(17, 20), 1)
                     .otherwise(0))
        .withColumn("total_fare_inr",
                    F.round(F.col("fare_base_inr") * F.col("surge_multiplier"), 2))

        # 5. Deduplication key
        .dropDuplicates(["ride_id"])
    )


def run():
    spark = get_spark()
    bronze_df = spark.read.format("delta").load(f"{BRONZE_PATH}/rides")
    silver_df = transform_rides(bronze_df)

    (silver_df.write
     .format("delta")
     .mode("overwrite")                    # use MERGE in production
     .partitionBy("date_partition", "city_zone")
     .option("mergeSchema", "true")
     .save(f"{SILVER_PATH}/rides"))

    print(f"Silver rides written: {silver_df.count()} rows")


if __name__ == "__main__":
    run()
```

### 7.3 Silver → Gold Batch (processing/batch/silver_to_gold.py)

```python
def compute_gold_hourly_kpis(silver_rides_df):
    """
    Grain: city_zone + date_partition + hour_of_day
    One row per zone per hour.
    """
    return (
        silver_rides_df
        .filter(F.col("status") == "completed")
        .groupBy("city_zone", "date_partition", "hour_of_day")
        .agg(
            F.count("ride_id")                          .alias("total_rides"),
            F.sum("total_fare_inr")                     .alias("total_revenue_inr"),
            F.avg("fare_base_inr")                      .alias("avg_base_fare_inr"),
            F.avg("surge_multiplier")                   .alias("avg_surge_multiplier"),
            F.avg("distance_km")                        .alias("avg_distance_km"),
            F.sum(F.when(F.col("surge_multiplier") > 1.0,
                         F.col("total_fare_inr")).otherwise(0))
                                                        .alias("surge_revenue_inr"),
            F.countDistinct("driver_id")                .alias("unique_drivers"),
        )
        .withColumn("surge_revenue_pct",
                    F.round(F.col("surge_revenue_inr") / F.col("total_revenue_inr") * 100, 2))
        .withColumn("refreshed_at", F.current_timestamp())
    )
```

---

## 8. Module 4 — PySpark Structured Streaming

### 8.1 Kafka → Bronze Streaming (processing/streaming/kafka_to_bronze.py)

```python
import json
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, FloatType, IntegerType

# Kafka message schema — all fields arrive as JSON string in "value" column
RIDE_EVENT_SCHEMA = StructType([
    StructField("ride_id",           StringType(),  True),
    StructField("event_timestamp",   StringType(),  True),
    StructField("driver_id",         StringType(),  True),
    StructField("user_id",           StringType(),  True),
    StructField("status",            StringType(),  True),
    StructField("pickup_lat",        DoubleType(),  True),
    StructField("pickup_lon",        DoubleType(),  True),
    StructField("dropoff_lat",       DoubleType(),  True),
    StructField("dropoff_lon",       DoubleType(),  True),
    StructField("city_zone",         StringType(),  True),
    StructField("distance_km",       DoubleType(),  True),
    StructField("vehicle_type",      StringType(),  True),
    StructField("fare_base_inr",     DoubleType(),  True),
    StructField("surge_multiplier",  FloatType(),   True),
    StructField("weather",           StringType(),  True),
    StructField("event_delay_ms",    IntegerType(), True),
    StructField("schema_version",    StringType(),  True),
])


def run_kafka_to_bronze(spark, bronze_path: str, kafka_bootstrap: str, topic: str):
    # Read from Kafka
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # Kafka gives: key, value (bytes), topic, partition, offset, timestamp
    # Parse JSON value
    parsed = (
        raw_stream
        .select(
            F.col("key").cast(StringType()).alias("partition_key"),
            F.from_json(F.col("value").cast(StringType()), RIDE_EVENT_SCHEMA).alias("data"),
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_ingest_timestamp"),
        )
        .select("partition_key", "data.*", "topic", "partition", "offset", "kafka_ingest_timestamp")
        # Add Bronze metadata
        .withColumn("bronze_ingested_at", F.current_timestamp())
        .withColumn("date_partition",     F.to_date(F.col("kafka_ingest_timestamp")))
        .withColumn("hour_partition",     F.hour(F.col("kafka_ingest_timestamp")))
    )

    # Write to Delta Bronze — append only, partitioned by date and hour
    query = (
        parsed.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{bronze_path}/rides/_checkpoint")
        .partitionBy("date_partition", "hour_partition")
        .trigger(processingTime="30 seconds")   # micro-batch every 30s
        .start(f"{bronze_path}/rides")
    )

    return query
```

### 8.2 Watermark Configuration (critical for late events)
```python
# Apply watermark BEFORE any aggregation
# Means: accept events up to 2 minutes late; discard anything later
rides_with_watermark = (
    parsed
    .withColumn("event_time", F.to_timestamp("event_timestamp"))
    .withWatermark("event_time", "2 minutes")
)

# Windowed aggregation — 5-minute windows, sliding every 1 minute
windowed_counts = (
    rides_with_watermark
    .groupBy(
        F.window("event_time", "5 minutes", "1 minute"),
        "city_zone"
    )
    .agg(F.count("ride_id").alias("ride_count"))
)
```

### 8.3 Streaming Query Management
```python
# Run multiple streaming queries concurrently
query1 = run_kafka_to_bronze(spark, bronze_path, kafka_bootstrap, "ride-events")
query2 = run_kafka_to_bronze(spark, bronze_path, kafka_bootstrap, "driver-events")

# Wait for termination (blocks)
spark.streams.awaitAnyTermination()

# Monitor active streams
for q in spark.streams.active:
    print(q.name, q.status, q.lastProgress)
```

---

## 9. Module 5 — Delta Lake Medallion Architecture

### 9.1 Layer Definitions

```
Bronze Layer  →  Raw, unmodified Kafka events as Delta tables
                 Never updated or deleted. Append-only.
                 Schema: all source fields + bronze_ingested_at + kafka metadata
                 Partition: date_partition, hour_partition
                 Retention: 90 days

Silver Layer  →  Cleaned, typed, joined, deduplicated
                 MERGE INTO on ride_id (idempotent)
                 Schema: all Silver transforms applied
                 Partition: date_partition, city_zone
                 Retention: 1 year

Gold Layer    →  Aggregated KPIs, ML features, dashboard-ready
                 Overwrite on each refresh (or MERGE on grain key)
                 Partition: date_partition
                 Retention: 3 years
```

### 9.2 Gold Tables — Complete Schema

#### gold_hourly_kpis
```
Grain: (city_zone, date_partition, hour_of_day) — one row per zone per hour
Columns:
  city_zone           STRING     NOT NULL
  date_partition      DATE       NOT NULL
  hour_of_day         INT        NOT NULL  (0–23)
  total_rides         LONG
  total_revenue_inr   DOUBLE
  avg_base_fare_inr   DOUBLE
  avg_surge_multiplier FLOAT
  avg_distance_km     DOUBLE
  surge_revenue_inr   DOUBLE
  surge_revenue_pct   DOUBLE
  unique_drivers      LONG
  refreshed_at        TIMESTAMP
```

#### gold_driver_utilisation
```
Grain: (driver_id, date_partition) — one row per driver per day
Columns:
  driver_id           STRING     NOT NULL
  date_partition      DATE       NOT NULL
  rides_completed     INT
  rides_cancelled     INT
  acceptance_rate     DOUBLE     (completed / (completed + cancelled))
  avg_rating          DOUBLE
  total_earnings_inr  DOUBLE
  online_hours        DOUBLE     (estimated from first to last event)
  refreshed_at        TIMESTAMP
```

#### gold_demand_heatmap
```
Grain: (lat_grid, lon_grid, hour_of_day) — grid cell x hour
  lat_grid = round(pickup_lat, 2)   # ~1km resolution
  lon_grid = round(pickup_lon, 2)
Columns:
  lat_grid            DOUBLE     NOT NULL
  lon_grid            DOUBLE     NOT NULL
  hour_of_day         INT        NOT NULL
  ride_count          LONG
  avg_wait_time_min   DOUBLE
  surge_active        BOOLEAN
  avg_surge_mult      FLOAT
  refreshed_at        TIMESTAMP
```

#### gold_ml_features
```
Grain: (ride_id) — one row per ride, features for ML training/serving
Columns:
  ride_id                   STRING  NOT NULL
  event_timestamp           TIMESTAMP
  city_zone                 STRING
  hour_sin                  DOUBLE   (sin(2π * hour / 24))
  hour_cos                  DOUBLE   (cos(2π * hour / 24))
  day_of_week               INT
  is_peak_hour              INT      (0 or 1)
  zone_demand_last_15min    LONG
  zone_demand_last_1hr      LONG
  active_drivers_in_zone    INT
  demand_supply_ratio       DOUBLE   (zone_demand_15min / max(drivers, 1))
  weather_encoded           INT      (0=clear, 1=cloudy, 2=rain)
  avg_fare_last_1hr         DOUBLE
  cancellation_rate_1hr     DOUBLE
  surge_multiplier          FLOAT    (TARGET variable for ML)
```

### 9.3 MERGE INTO Pattern (Silver Deduplication)
```python
from delta.tables import DeltaTable

def upsert_to_silver(spark, silver_path: str, new_df):
    """Idempotent upsert using ride_id as key."""
    delta_table = DeltaTable.forPath(spark, f"{silver_path}/rides")
    (
        delta_table.alias("existing")
        .merge(
            new_df.alias("incoming"),
            "existing.ride_id = incoming.ride_id"
        )
        .whenMatchedUpdateAll()         # update if ride_id exists (status change)
        .whenNotMatchedInsertAll()      # insert if new ride
        .execute()
    )
```

### 9.4 Time Travel Queries
```python
# Query table 2 hours ago (by timestamp)
spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-15 14:00:00") \
    .load(f"{gold_path}/hourly_kpis")

# Query a specific version
spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load(f"{gold_path}/hourly_kpis")

# Show table history
spark.sql(f"DESCRIBE HISTORY delta.`{gold_path}/hourly_kpis`").show()
```

### 9.5 Z-ORDER Optimization
```python
# After initial load, run optimize + Z-ORDER on frequently filtered columns
spark.sql(f"""
    OPTIMIZE delta.`{gold_path}/hourly_kpis`
    ZORDER BY (city_zone, hour_of_day)
""")
# Run weekly via Airflow DAG
```

---

## 10. Module 6 — ML Model & MLflow

### 10.1 Feature Engineering (ml/feature_engineering.py)

```python
import numpy as np
import pandas as pd


def encode_hour_cyclically(hour: int):
    """Encode hour as sin/cos to capture cyclical nature (23:00 close to 00:00)."""
    return np.sin(2 * np.pi * hour / 24), np.cos(2 * np.pi * hour / 24)


def encode_weather(weather: str) -> int:
    return {"clear": 0, "cloudy": 1, "rain": 2}.get(weather, 0)


def build_feature_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    Input: gold_ml_features DataFrame
    Output: feature matrix ready for XGBoost
    """
    df = df.copy()
    df["hour_sin"], df["hour_cos"] = zip(*df["hour_of_day"].map(encode_hour_cyclically))
    df["weather_encoded"] = df["weather"].map(encode_weather)
    df["demand_supply_ratio"] = (
        df["zone_demand_last_15min"] / df["active_drivers_in_zone"].clip(lower=1)
    )
    feature_cols = [
        "hour_sin", "hour_cos", "day_of_week", "is_peak_hour",
        "zone_demand_last_15min", "zone_demand_last_1hr",
        "active_drivers_in_zone", "demand_supply_ratio",
        "weather_encoded", "avg_fare_last_1hr", "cancellation_rate_1hr"
    ]
    return df[feature_cols], df["surge_multiplier"]
```

### 10.2 Training Script (ml/train_surge_model.py)

```python
import mlflow
import mlflow.xgboost
import xgboost as xgb
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import RandomizedSearchCV
from ml.feature_engineering import build_feature_matrix
import os

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "./data/mlflow")
EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT_NAME", "surge_price_prediction")


def time_based_split(df: pd.DataFrame, train_pct=0.70, val_pct=0.15):
    """
    CRITICAL: Split by time, NEVER random split.
    Random split leaks future data into training (data leakage).
    """
    df = df.sort_values("event_timestamp")
    n = len(df)
    train_end = int(n * train_pct)
    val_end   = int(n * (train_pct + val_pct))
    return df.iloc[:train_end], df.iloc[train_end:val_end], df.iloc[val_end:]


def train():
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment(EXPERIMENT)

    # Load features from Gold layer
    df = pd.read_parquet(f"./data/delta/gold/ml_features")  # or spark.read
    train_df, val_df, test_df = time_based_split(df)

    X_train, y_train = build_feature_matrix(train_df)
    X_val,   y_val   = build_feature_matrix(val_df)
    X_test,  y_test  = build_feature_matrix(test_df)

    param_grid = {
        "n_estimators":     [100, 200, 300],
        "max_depth":        [3, 5, 7],
        "learning_rate":    [0.05, 0.1, 0.2],
        "subsample":        [0.7, 0.9, 1.0],
        "colsample_bytree": [0.7, 0.9, 1.0],
    }

    with mlflow.start_run(run_name="surge_xgboost_v1"):
        # Log training metadata
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("val_size",   len(X_val))
        mlflow.log_param("test_size",  len(X_test))
        mlflow.log_param("feature_count", X_train.shape[1])
        mlflow.log_param("split_strategy", "time_based")

        # Hyperparameter search
        base_model = xgb.XGBRegressor(random_state=42, n_jobs=-1)
        search = RandomizedSearchCV(
            base_model, param_grid, n_iter=20, cv=3,
            scoring="neg_mean_absolute_error", n_jobs=-1, random_state=42
        )
        search.fit(X_train, y_train)
        best_model = search.best_estimator_

        # Log best params
        mlflow.log_params(search.best_params_)

        # Evaluate on val and test
        for split_name, X, y in [("val", X_val, y_val), ("test", X_test, y_test)]:
            preds = best_model.predict(X)
            mae  = mean_absolute_error(y, preds)
            rmse = np.sqrt(mean_squared_error(y, preds))
            r2   = r2_score(y, preds)
            mlflow.log_metric(f"{split_name}_mae",  mae)
            mlflow.log_metric(f"{split_name}_rmse", rmse)
            mlflow.log_metric(f"{split_name}_r2",   r2)
            print(f"{split_name}: MAE={mae:.4f}, RMSE={rmse:.4f}, R2={r2:.4f}")

        # Log model
        mlflow.xgboost.log_model(
            best_model,
            artifact_path="surge_model",
            registered_model_name="surge_predictor"
        )

    print("Training complete. Model registered in MLflow.")
```

### 10.3 Pandas UDF for PySpark Serving (ml/serve_predictions.py)

```python
import mlflow.pyfunc
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType

MODEL_URI = "models:/surge_predictor/Production"  # from MLflow registry


def load_model_udf(spark):
    """Create a Pandas UDF that serves MLflow model predictions in Spark."""
    model = mlflow.pyfunc.load_model(MODEL_URI)

    @F.pandas_udf(FloatType())
    def predict_surge(
        hour_sin: pd.Series, hour_cos: pd.Series,
        day_of_week: pd.Series, is_peak_hour: pd.Series,
        zone_demand_15min: pd.Series, zone_demand_1hr: pd.Series,
        active_drivers: pd.Series, demand_supply_ratio: pd.Series,
        weather_encoded: pd.Series, avg_fare_1hr: pd.Series,
        cancel_rate_1hr: pd.Series
    ) -> pd.Series:
        features = pd.DataFrame({
            "hour_sin": hour_sin, "hour_cos": hour_cos,
            "day_of_week": day_of_week, "is_peak_hour": is_peak_hour,
            "zone_demand_last_15min": zone_demand_15min,
            "zone_demand_last_1hr": zone_demand_1hr,
            "active_drivers_in_zone": active_drivers,
            "demand_supply_ratio": demand_supply_ratio,
            "weather_encoded": weather_encoded,
            "avg_fare_last_1hr": avg_fare_1hr,
            "cancellation_rate_1hr": cancel_rate_1hr,
        })
        return pd.Series(model.predict(features).astype(float))

    return predict_surge
```

---

## 11. Module 7 — Apache Airflow Orchestration

### 11.1 Airflow Init
```bash
export AIRFLOW_HOME=./orchestration
airflow db init
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@rideshare.local
airflow webserver --port 8080 &
airflow scheduler &
```

### 11.2 Gold Refresh DAG (orchestration/dags/gold_refresh_dag.py)

Uses `deltalake` + pandas. No PySpark. No fallbacks.

```python
from __future__ import annotations
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

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
    print(f"✓ Gold refresh complete: {len(gold_df)} rows in zone_demand")


def _validate_gold_counts(**context):
    from deltalake import DeltaTable
    from storage.delta_config import GOLD_ZONE_DEMAND_TABLE
    gold_df = DeltaTable(GOLD_ZONE_DEMAND_TABLE).to_pandas()
    if len(gold_df) == 0:
        raise ValueError("Gold zone_demand table is empty after refresh!")
    print(f"✓ Gold validation passed: {len(gold_df)} rows")


def _alert_on_anomaly(**context):
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
```

### 11.3 ML Retrain DAG (orchestration/dags/ml_retrain_dag.py)

Uses `deltalake` to load Silver. XCom to pass metrics between tasks. MAE threshold gate for promotion.

```python
def _load_training_frame():
    from deltalake import DeltaTable
    from storage.delta_config import SILVER_RIDES_TABLE
    frame = DeltaTable(SILVER_RIDES_TABLE).to_pandas()
    print(f"✓ Loaded {len(frame):,} Silver rows")
    return frame

def _train_surge_model_task(**context):
    from ml.train_surge_model import train_surge_model
    import os
    os.environ.setdefault("MLFLOW_TRACKING_URI", "http://mlflow:5000")
    df = _load_training_frame()
    if len(df) == 0:
        raise ValueError("No data available for ML training!")
    model, metrics = train_surge_model(df, experiment_name="surge_price_prediction")
    context["task_instance"].xcom_push(key="model_metrics", value=metrics)

def _promote_if_better(**context):
    metrics = context["task_instance"].xcom_pull(task_ids="train_surge_model", key="model_metrics")
    mae = metrics.get("mae", float("inf"))
    threshold = 15.0
    if mae < threshold:
        print(f"✓ Model promoted — MAE={mae:.4f} < threshold={threshold}")
    else:
        print(f"✗ Model NOT promoted — MAE={mae:.4f} >= threshold={threshold}")
```

### 11.4 Data Quality DAG (orchestration/dags/data_quality_dag.py)

Runs 4 checks in **parallel** every 30 minutes. Uses `deltalake` — no PySpark.

```python
def check_null_ride_ids(**context):
    from deltalake import DeltaTable
    from storage.delta_config import SILVER_RIDES_TABLE
    df = DeltaTable(SILVER_RIDES_TABLE).to_pandas()
    null_count = df["ride_id"].isna().sum()
    if null_count > 0:
        raise ValueError(f"Found {null_count} NULL ride_ids!")

def check_surge_multiplier_bounds(**context):
    from deltalake import DeltaTable
    from storage.delta_config import SILVER_RIDES_TABLE
    df = DeltaTable(SILVER_RIDES_TABLE).to_pandas()
    out_of_bounds = ((df["surge_multiplier"] < 1.0) | (df["surge_multiplier"] > 3.5)).sum()
    if out_of_bounds > 0:
        raise ValueError(f"Found {out_of_bounds} rides with surge outside [1.0, 3.5]!")

# start >> [check_nulls, check_gold, check_ts, check_surge] >> end  ← parallel
```

### 11.5 Dashboard Warmup DAG (orchestration/dags/dashboard_warmup_dag.py)

Precomputes Redis keys from Delta tables every 15 minutes. Uses `deltalake` + pandas + Redis.

```python
def compute_total_revenue(**context):
    from deltalake import DeltaTable
    from storage.delta_config import GOLD_ZONE_DEMAND_TABLE
    from storage.redis_client import get_client
    import json
    df = DeltaTable(GOLD_ZONE_DEMAND_TABLE).to_pandas()
    total_revenue = float(df["gross_revenue_inr"].fillna(0).sum())
    get_client().setex("dashboard:total_revenue", 900, json.dumps({"amount": total_revenue}))

def compute_driver_utilisation(**context):
    from deltalake import DeltaTable
    from storage.delta_config import SILVER_RIDES_TABLE
    from storage.redis_client import get_client
    import json
    df = DeltaTable(SILVER_RIDES_TABLE).to_pandas()
    total_rides = len(df)
    completed = int((df["status"] == "completed").sum())
    utilisation = (completed / total_rides * 100.0) if total_rides > 0 else 0.0
    get_client().setex("dashboard:driver_utilisation", 900,
                       json.dumps({"utilisation_percent": utilisation, "total_rides": total_rides}))
```

### 11.6 Idempotency Checklist (every task must pass this)

- Does re-running produce duplicate rows? → Gold uses `mode="overwrite"` (full replace)
- Does re-running overwrite correct data? → Silver uses `dropDuplicates(["ride_id"])`
- Does re-running leave partial state? → Delta write is atomic; partial writes roll back
- Is `catchup=False`? → Yes on all DAGs — no backfill of missed intervals

---

## 12. Module 8 — Redis Cache Layer

### 12.1 What Goes in Redis

```
Key Pattern                          TTL       Value
────────────────────────────────     ────────  ──────────────────────────
live:rides_last_5min                 60s       JSON: {count: int}
live:zone:{zone_name}:demand         30s       JSON: {ride_count, surge_mult}
live:zone:{zone_name}:surge          30s       float (surge multiplier)
driver:{driver_id}:location          15s       JSON: {lat, lon, timestamp}
prediction:{zone}:{hour}:surge       120s      float (ML model prediction)
```

### 12.2 Redis Client (storage/redis_client.py)

```python
import redis
import json
import os
from dotenv import load_dotenv

load_dotenv()
_client = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 0)),
            decode_responses=True,
        )
    return _client


def set_zone_demand(zone: str, ride_count: int, surge_mult: float, ttl: int = 30):
    client = get_client()
    data = json.dumps({"ride_count": ride_count, "surge_multiplier": surge_mult})
    client.setex(f"live:zone:{zone}:demand", ttl, data)


def get_zone_demand(zone: str) -> dict | None:
    client = get_client()
    raw = client.get(f"live:zone:{zone}:demand")
    return json.loads(raw) if raw else None


def set_driver_location(driver_id: str, lat: float, lon: float, ttl: int = 15):
    client = get_client()
    data = json.dumps({"lat": lat, "lon": lon})
    client.setex(f"driver:{driver_id}:location", ttl, data)
```

---

## 13. Module 9 — Streamlit Dashboard

### 13.1 App Structure (dashboard/app.py)

Auto-refresh uses JavaScript injection via `components.html()` — no third-party package needed.
The original `streamlit-autorefresh` package was removed because it was unavailable in the container.

```python
import streamlit as st
import streamlit.components.v1 as components

from components.demand_heatmap import render_heatmap
from components.live_counter import render_live_counter
from components.revenue_charts import render_revenue
from components.driver_utilisation import render_driver_utilisation
from components.pipeline_health import render_pipeline_health

st.set_page_config(
    page_title="Rideshare Analytics",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Auto-refresh every 30 seconds via JavaScript — zero extra dependency
components.html(
    "<script>setTimeout(function(){window.location.reload()},30000);</script>",
    height=0,
)

st.title("🚗 Real-Time Ride-Sharing Analytics Platform")
st.caption("Live pipeline — data flowing from Kafka → Redis → Dashboard")

col1, col2, col3, col4 = st.columns(4)
with col1:
    render_live_counter()

with st.expander("📊 Pipeline Health", expanded=True):
    render_pipeline_health()

tab1, tab2, tab3 = st.tabs(["🗺️ Demand Heatmap", "💰 Revenue", "🧑‍✈️ Drivers"])

with tab1:
    st.subheader("Live Demand Heatmap")
    render_heatmap()

with tab2:
    st.subheader("Revenue Analytics")
    render_revenue()

with tab3:
    st.subheader("Driver Utilisation")
    render_driver_utilisation()
```

**Do NOT use:**
- `streamlit-autorefresh` — not in requirements, causes `ModuleNotFoundError`
- `time.sleep(30); st.rerun()` — blocks the server thread for every connected user

### 13.2 Demand Heatmap Component

```python
import streamlit as st
import pydeck as pdk
from dashboard.data_connectors.delta_reader import read_gold_heatmap


def render_heatmap():
    df = read_gold_heatmap()  # returns pandas DF from Gold Delta table
    layer = pdk.Layer(
        "HeatmapLayer",
        data=df,
        get_position=["lon_grid", "lat_grid"],
        get_weight="ride_count",
        radius_pixels=60,
        intensity=1,
        threshold=0.3,
    )
    view = pdk.ViewState(latitude=12.97, longitude=77.59, zoom=11, pitch=0)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view))
```

---

## 14. Module 10 — Docker Compose Full Stack

### 14.1 docker-compose.yml (actual current state)

**Critical design decisions reflected here:**
1. Uses Confluent Kafka images (`confluentinc/cp-*`), not bitnami
2. Delta Lake uses bind mount `./data/delta:/data/delta` (NOT a named volume) so Spark + Airflow + Dashboard share the same physical files
3. All Dockerfiles use `CMD` not `ENTRYPOINT` so compose `command:` cleanly overrides
4. `kafka_init` is a separate one-shot container for topic creation

```yaml
networks:
  rideshare-net:
    driver: bridge

volumes:
  kafka_data:
  zookeeper_data:
  redis_data:
  mlflow_data:
  airflow_data:
  # NOTE: NO delta_data named volume — Delta uses bind mount ./data/delta instead

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.9.6
    networks: [rideshare-net]
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper_data:/bitnami
    healthcheck:
      test: ["CMD", "bash", "-lc", "echo srvr | nc localhost 2181 | grep 'Mode:'"]
      interval: 10s
      timeout: 5s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.9.6
    networks: [rideshare-net]
    ports:
      - "9092:9092"
    depends_on:
      zookeeper:
        condition: service_started
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
    volumes:
      - kafka_data:/bitnami

  kafka_init:
    build:
      context: .
      dockerfile: Dockerfile.live_writer
    networks: [rideshare-net]
    depends_on: [kafka]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    entrypoint: ["python", "-m", "live_writer.create_topics"]
    # One-shot container: runs create_topics.py then exits with code 0

  redis:
    image: redis:7.2
    networks: [rideshare-net]
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    command: redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru

  spark:
    build:
      context: .
      dockerfile: Dockerfile.spark
    networks: [rideshare-net]
    ports:
      - "4040:4040"
    volumes:
      - ./:/app                          # full repo as app (for processing/, storage/, etc.)
      - ./data/delta:/data/delta         # BIND MOUNT: same host directory as airflow
    environment:
      PYTHONPATH: /app
      DELTA_BRONZE_PATH: /data/delta/bronze
      DELTA_SILVER_PATH: /data/delta/silver
      DELTA_GOLD_PATH: /data/delta/gold
    command: ["bash", "-lc", "python3 -m processing.streaming.run_streaming_pipeline"]

  mlflow:
    image: python:3.11-slim
    networks: [rideshare-net]
    ports:
      - "5000:5000"
    volumes:
      - mlflow_data:/mlflow
    command: >
      sh -c "pip install mlflow==2.11.3 &&
             mlflow server --host 0.0.0.0 --port 5000
             --backend-store-uri /mlflow/tracking
             --default-artifact-root /mlflow/artifacts"

  airflow:
    build:
      context: .
      dockerfile: Dockerfile.airflow      # apache/airflow:2.8.1 + deltalake + pyarrow
    networks: [rideshare-net]
    ports:
      - "8080:8080"
    depends_on: [kafka, redis, spark]
    volumes:
      - ./orchestration/dags:/opt/airflow/dags
      - ./storage:/opt/airflow/storage
      - ./ml:/opt/airflow/ml
      - ./data/delta:/data/delta         # BIND MOUNT: same physical path as Spark
      - airflow_data:/opt/airflow/airflow_data
    environment:
      AIRFLOW__CORE__EXECUTOR: SequentialExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////opt/airflow/airflow_data/airflow.db
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"
      PYTHONPATH: /opt/airflow
      DELTA_BRONZE_PATH: /data/delta/bronze
      DELTA_SILVER_PATH: /data/delta/silver
      DELTA_GOLD_PATH: /data/delta/gold
    command: ["standalone"]

  simulator:
    build:
      context: .
      dockerfile: Dockerfile.simulator
    networks: [rideshare-net]
    depends_on: [kafka]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    volumes:
      - ./simulator:/app/simulator
    command: python -m simulator.run_simulator
    # CMD in Dockerfile.simulator, overridden cleanly by compose command:

  dashboard:
    build:
      context: .
      dockerfile: Dockerfile.dashboard
    networks: [rideshare-net]
    ports:
      - "8501:8501"
    depends_on: [redis, spark]
    volumes:
      - ./dashboard:/app/dashboard
      - ./storage:/app/storage
      - ./data/delta:/data/delta         # BIND MOUNT: reads same Gold tables as Airflow
    environment:
      REDIS_HOST: redis
      DELTA_GOLD_PATH: /data/delta/gold
    command: streamlit run /app/dashboard/app.py --server.port=8501 --server.address=0.0.0.0

  live_writer:
    build:
      context: .
      dockerfile: Dockerfile.live_writer
    networks: [rideshare-net]
    depends_on: [kafka, redis]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      REDIS_HOST: redis
      REDIS_PORT: 6379
    volumes:
      - ./live_writer:/app/live_writer
      - ./storage:/app/storage
    command: python -m live_writer.run_live_writer
```

### 14.2 One-Command Startup
```bash
# Start everything
docker compose up -d

# Check all services healthy
docker compose ps

# View logs for a specific service
docker compose logs -f kafka
docker compose logs -f spark

# Stop everything
docker compose down

# Stop and remove all volumes (full reset)
docker compose down -v
```

---

## 15. Data Schemas — Complete Definitions

### 15.1 Kafka Message Schemas

**ride-events topic:**
```json
{
  "ride_id":          "550e8400-e29b-41d4-a716-446655440000",
  "event_timestamp":  "2024-01-15T08:34:22.123456+00:00",
  "driver_id":        "UUID",
  "user_id":          "UUID",
  "status":           "completed",
  "pickup_lat":       12.978234,
  "pickup_lon":       77.594562,
  "dropoff_lat":      13.021456,
  "dropoff_lon":      77.580123,
  "city_zone":        "cbd",
  "distance_km":      8.34,
  "vehicle_type":     "cab_economy",
  "fare_base_inr":    334.60,
  "surge_multiplier": 1.8,
  "weather":          "rain",
  "event_delay_ms":   0,
  "schema_version":   "1.0"
}
```

**driver-events topic:**
```json
{
  "driver_event_id":  "UUID",
  "event_timestamp":  "ISO8601",
  "driver_id":        "UUID",
  "event_type":       "location_update | status_change | ride_accepted | ride_completed",
  "current_lat":      float,
  "current_lon":      float,
  "online_status":    "online | offline | on_trip",
  "rating":           float,
  "schema_version":   "1.0"
}
```

**payment-events topic:**
```json
{
  "payment_id":       "UUID",
  "event_timestamp":  "ISO8601",
  "ride_id":          "UUID",
  "driver_id":        "UUID",
  "amount_inr":       float,
  "payment_method":   "upi | card | cash | wallet",
  "payment_status":   "success | failed | refunded",
  "failure_reason":   "null or string",
  "schema_version":   "1.0"
}
```

---

## 16. Configuration Reference

### 16.1 Spark Streaming Tuning
```python
# For local development (low resource)
.config("spark.sql.shuffle.partitions", "8")        # default 200 is too high locally
.config("spark.default.parallelism", "8")
.config("spark.streaming.stopGracefullyOnShutdown", "true")

# Trigger options
.trigger(processingTime="30 seconds")    # micro-batch every 30s (development)
.trigger(processingTime="5 seconds")     # production target
.trigger(once=True)                      # run exactly once then stop (testing)
```

### 16.2 Delta Lake Optimizations
```python
# Enable auto-optimization (Delta Lake 3.x)
.config("spark.databricks.delta.optimizeWrite.enabled", "true")
.config("spark.databricks.delta.autoCompact.enabled", "true")

# Checkpoint interval (commit Delta log every N micro-batches)
.option("checkpointLocation", "/path/to/checkpoint")
```

### 16.3 Kafka Consumer Tuning
```python
# In readStream
.option("maxOffsetsPerTrigger", 10000)    # limit records per micro-batch
.option("fetchOffset.numRetries", 3)
.option("kafka.session.timeout.ms", "30000")
```

---

## 17. Testing Strategy

### 17.1 Unit Tests (tests/test_silver_transforms.py)

```python
import pytest
from pyspark.sql import SparkSession
from processing.batch.bronze_to_silver import transform_rides

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_null_ride_id_dropped(spark):
    data = [{"ride_id": None, "driver_id": "d1", "event_timestamp": "2024-01-01T10:00:00Z",
             "city_zone": "cbd", "pickup_lat": 12.97, "pickup_lon": 77.59,
             "dropoff_lat": 13.0, "dropoff_lon": 77.6, "distance_km": 5.0,
             "fare_base_inr": 200.0, "surge_multiplier": 1.0,
             "status": "completed", "vehicle_type": "cab_economy",
             "weather": "clear", "event_delay_ms": 0, "schema_version": "1.0"}]
    df = spark.createDataFrame(data)
    result = transform_rides(df)
    assert result.count() == 0, "Null ride_id should be dropped"

def test_peak_hour_flagged(spark):
    data = [{"ride_id": "r1", "driver_id": "d1", "event_timestamp": "2024-01-01T09:00:00Z",
             "city_zone": "cbd", "pickup_lat": 12.97, "pickup_lon": 77.59,
             "dropoff_lat": 13.0, "dropoff_lon": 77.6, "distance_km": 5.0,
             "fare_base_inr": 200.0, "surge_multiplier": 1.0,
             "status": "completed", "vehicle_type": "cab_economy",
             "weather": "clear", "event_delay_ms": 0, "schema_version": "1.0"}]
    df = spark.createDataFrame(data)
    result = transform_rides(df)
    row = result.first()
    assert row["is_peak_hour"] == 1, "9am should be flagged as peak hour"
```

### 17.2 Data Quality Checks (tests/test_data_quality.py)

```python
def check_bronze_freshness(spark, bronze_path: str, max_delay_minutes: int = 10):
    """Fail if no new data in Bronze in the last max_delay_minutes."""
    from pyspark.sql import functions as F
    df = spark.read.format("delta").load(f"{bronze_path}/rides")
    latest = df.agg(F.max("bronze_ingested_at")).collect()[0][0]
    delay = (datetime.now(timezone.utc) - latest).total_seconds() / 60
    assert delay < max_delay_minutes, f"Bronze freshness violated: {delay:.1f} min old"
```

---

## 18. Known Failure Modes & Fixes

The following bugs were all encountered and fixed during actual development of this project.

### 18.1 Bugs Discovered and Fixed

| # | Failure | Symptom | Root Cause | Fix Applied |
|---|---|---|---|---|
| 1 | **Delta split-brain** | Airflow reads empty Gold table despite Spark writing data | Named `delta_data` Docker volume is separate from Spark's bind-mounted `/app/data/delta` — two different filesystems | Replace named volume with bind mount `./data/delta:/data/delta` in all three containers (Spark, Airflow, Dashboard) |
| 2 | **Airflow using Redis estimates instead of real Delta data** | DAGs run green, metrics look reasonable, but are computed from Redis zone snapshots | `build_delta_spark()` in Airflow failed when Maven couldn't download JARs; `try/except` silently caught the error and fell back to Redis | Remove PySpark from Airflow entirely; replace with `deltalake` Python library (Rust-based, no JVM, no Maven) |
| 3 | **`ModuleNotFoundError: streamlit_autorefresh`** | Dashboard container crashes on import | `streamlit-autorefresh` was listed in `requirements-dashboard.txt` but the installed package name differs and was unavailable | Remove the package; use `components.html("<script>setTimeout(...);</script>", height=0)` instead |
| 4 | **Silver path mismatch** | Airflow DAGs read empty Silver table | Delta config had `SILVER_RIDES_TABLE = .../rides` but Spark wrote to `.../rides_clean` (the actual on-disk path) | Revert `delta_config.py` to `rides_clean`; update all DAGs to import `SILVER_RIDES_TABLE` constant instead of hardcoding paths |
| 5 | **Dockerfile ENTRYPOINT + compose command duplication** | Simulator and dashboard containers crash on startup | `ENTRYPOINT ["python", "-m", "..."]` in Dockerfile + `command: python -m ...` in compose → Docker runs both, doubling the command | Change all custom Dockerfiles to use `CMD` instead of `ENTRYPOINT` |
| 6 | **`mlflow.sklearn.log_model` on an XGBoost model** | MLflow saves model with wrong serialisation; load fails in serving | `XGBRegressor` is not a scikit-learn estimator; `mlflow.sklearn` uses pickle which misses XGBoost-native format | Change to `mlflow.xgboost.log_model(model, artifact_path="model")` |
| 7 | **`mean_squared_error(..., squared=False)` removed** | `TypeError: unexpected keyword argument 'squared'` on scikit-learn 1.3+ | The `squared` parameter was deprecated then removed in scikit-learn 1.3 | Replace with `math.sqrt(mean_squared_error(y_true, y_pred))` |
| 8 | **Streaming checkpoint reset on every startup** | Bronze→Silver stream reprocesses all Bronze data on every container restart | `_reset_checkpoint()` function deleted checkpoint dir at startup | Remove `_reset_checkpoint` function and its call; checkpoints are intentionally persistent |

### 18.2 General Operational Failures

| Failure | Symptom | Root Cause | Fix |
|---|---|---|---|
| Kafka `NoBrokersAvailable` | Producer fails at startup | Kafka not ready when simulator starts | `kafka_init` container waits with retry loop; simulator also retries on connect error |
| Delta table not found | `FileNotFoundError` in Airflow task | Spark streaming hasn't written first batch yet | Wait 30–60s for first micro-batch; check `docker compose logs spark` |
| Airflow DAG not in UI | DAG silently absent from Airflow web UI | Python import error in DAG file | Run `docker compose exec airflow python /opt/airflow/dags/<dag>.py` to see full traceback |
| Airflow CSRF error | "CSRF token missing" when triggering DAGs via API | Airflow 2.8 enforces CSRF on API calls | Trigger DAGs through the Airflow UI, not raw API calls without token |
| Full reset needed | Data corruption / wrong schema after code changes | Stale Delta transaction log + checkpoint mismatch | `docker compose down -v && rm -rf data/delta && docker compose up -d` |

---

## 19. Interview Q&A Answers

### Q: Why Kafka over writing directly to a database?
**A:** Kafka is a distributed commit log — messages persist to disk for 7 days, so any consumer can replay from any offset. If our Spark job crashes and restarts, it reads the Kafka checkpoint, finds the last committed offset, and resumes exactly there — no reprocessing, no data loss. A database can't do this: you'd need to poll for new rows, and replaying a time range would require complex queries. Kafka also decouples producer and consumer completely — the simulator doesn't know or care whether Spark is up.

### Q: Explain watermarks in your pipeline.
**A:** In our simulator, 5% of events have `event_delay_ms` up to 120 seconds — simulating a mobile network delay where a ride starts at 10:00 but the event arrives at Kafka at 10:02. Without watermarks, any windowed aggregation (e.g., "rides per 5-minute window") must keep state open forever waiting for potentially late events — state grows unboundedly. A watermark of "2 minutes" tells Spark: accept events up to 2 minutes late, then close the window and garbage-collect that state. Events arriving more than 2 minutes late are dropped. In our Bronze→Silver streaming path, we use append mode (no windowed aggregations) and let Airflow batch over the completed Silver table, so we don't need explicit watermarks there.

### Q: Why Delta Lake over plain Parquet?
**A:** If a Spark job fails mid-write on plain Parquet, you get a partial file. Readers see a corrupted table. Delta Lake writes atomically: it stages files then commits a transaction log entry. If it fails, the staged files are ignored — readers see the previous clean version. This is exactly what happened in dev: we restarted Spark mid-job several times, and Silver was never corrupted. Delta also gives `dropDuplicates`-free deduplication via MERGE INTO (update if exists, insert if new), and time travel which we used during debugging to compare Silver before and after a transform change.

### Q: Why did you use `deltalake` (Python/Rust) in Airflow instead of PySpark?
**A:** Originally Airflow DAGs used PySpark. Inside the container, `spark.jars.packages` tried to download Delta JARs from Maven Central at runtime. When Maven was unavailable, the SparkSession started silently with a reduced configuration that couldn't read Delta format. The DAGs were showing green in the UI while computing from Redis estimates, not real Delta data. The fix: replace PySpark in Airflow with the `deltalake` Python library — pure Rust, no JVM, no Maven, starts in under a second. Airflow is an orchestrator, not a processing engine. For 6,000 Silver rows, pandas is more than sufficient.

### Q: What is idempotency in Airflow and why does it matter?
**A:** An idempotent task produces the same result regardless of how many times it runs. In `gold_refresh_dag`, if the Silver→Gold task fails halfway and Airflow retries it, a non-idempotent implementation would produce partial duplicate Gold rows. We make it idempotent with `write_deltalake(mode="overwrite")` — the second run replaces the Gold table entirely, not appends to it. The grain (event_date, event_hour, city_zone) is always recomputed from the current Silver snapshot. This means `catchup=False` is safe — no matter how many times a run retries, the Gold table ends up in the same correct state.

### Q: How would this scale to 10 million rides per day?
**A:** Our current setup handles ~28,800 events/hour (8/sec) on a single machine. At 10M/day (~115K/hour): (1) Kafka: increase partitions from 4 to 24 on `rides-events`, add 2 broker nodes for replication factor 3; (2) Spark: move from single-container to a 5-node Spark cluster (YARN or Kubernetes); reduce trigger interval from 30s to 5s; (3) Delta Lake: move from local `./data/delta` to S3 or GCS — Delta on object storage scales to petabytes; (4) Gold computation: pandas groupby would be too slow on 10M rows — switch Gold refresh back to PySpark with partitioned writes; (5) Airflow: switch from `SequentialExecutor` to `KubernetesExecutor` for parallel DAG tasks; (6) Redis: upgrade to Redis Cluster for horizontal sharding.

### Q: Why XGBoost over a neural network for surge prediction?
**A:** Three concrete reasons: (1) Dataset size — we have ~6,000 Silver rows for training. Neural networks need orders of magnitude more data to outperform gradient boosting. On tabular data with thousands to tens-of-thousands rows, XGBoost is the consistent winner. (2) Training speed — XGBoost trains in under 5 seconds, enabling daily retraining in Airflow without a GPU. A neural network equivalent would take 10–30 minutes. (3) Interpretability — XGBoost gives feature importances directly: which features (zone, hour, weather) matter most for surge. An interviewer at an Ola or Uber data team would expect you to explain your model's predictions, not just report R².

### Q: Why is the Silver table named `rides_clean` not `rides`?
**A:** This was an actual bug. The original design spec used `rides` as the Silver table name. During implementation, the Spark streaming job committed checkpoints to a path called `rides_clean` — which was the on-disk reality. When we initially "fixed" the config to `rides`, Spark kept writing to `rides_clean` (checkpoint-committed path), Airflow read from `rides` (empty), and DAGs silently passed because the empty DataFrame produced no errors. Lesson: always derive table paths from a single constants file (`delta_config.py`) — never hardcode the same string in two places.

### Q: What does `catchup=False` do and when would you want `True`?
**A:** `catchup=False` tells Airflow: if the scheduler starts and the last scheduled interval was missed (e.g., Airflow was down for 3 hours), don't backfill those 3 missed hourly runs — just run the next scheduled interval. We use `False` because our Gold table is computed from the full current Silver snapshot (not just a time slice), so running the missed intervals would produce the same result as running once. You'd want `catchup=True` for a report that says "summarise yesterday's orders" — if that job was missed, you genuinely need to run it for each missed day.

---

## 20. Resume Bullets by Role

### Data Engineer
- Built end-to-end real-time streaming pipeline ingesting ~8 ride events/second using Apache Kafka (3 topics, 10 partitions) and PySpark Structured Streaming with exactly-once delivery via checkpoints
- Implemented Delta Lake medallion architecture (Bronze → Silver → Gold) with ACID transactions, idempotent MERGE/overwrite patterns, and time-travel queries on 6,000+ ride records
- Designed and deployed 4 production-grade Apache Airflow DAGs (hourly, daily, 30-min, 15-min) with retry logic, execution timeouts, parallel task execution, and XCom-based inter-task communication
- Containerised 10-service stack (Kafka, ZooKeeper, Spark, MLflow, Airflow, Redis, Dashboard, Simulator, Live Writer) using Docker Compose; fully reproducible with a single `docker compose up --build`
- Diagnosed and resolved a storage split-brain bug where a named Docker volume silently prevented Spark-written Delta files from being visible to Airflow; fixed by replacing with a shared bind mount

### Data Analyst
- Designed Gold layer KPI table (`zone_demand`) with grain `(event_date, event_hour, city_zone)` — ride counts, completed/cancelled, gross revenue, avg surge — readable in <1s via `deltalake` + pandas
- Built data quality pipeline with 4 automated checks (NULL ride IDs, surge multiplier bounds, Gold row counts, timestamp freshness) running every 30 minutes via Airflow
- Developed live Streamlit dashboard with real-time KPI counters, PyDeck demand heatmap, revenue charts by zone, and driver utilisation metrics; auto-refreshes every 30 seconds via JS injection

### ML Engineer
- Engineered tabular feature matrix from Silver ride data: one-hot encoded zone/vehicle/weather, numeric distance/fare/hour; trained XGBoost regression model (`XGBRegressor`, 100 estimators) to predict surge multiplier
- Tracked all training experiments in MLflow (`mlflow.xgboost.log_model`, MAE/RMSE/R² metrics); implemented promotion gate — model advances to Production registry only if MAE < 15.0
- Deployed trained MLflow model as a PySpark Pandas UDF (`@F.pandas_udf(DoubleType())`) for batch scoring of Silver data; model loaded once per Spark partition for efficient inference

### Analytics Engineer
- Designed and implemented 3-layer medallion architecture: Bronze (append-only raw Kafka events), Silver (deduplicated + typed, `dropDuplicates(["ride_id"])`), Gold (hourly KPIs via pandas groupby)
- Chose `deltalake` Python library over PySpark for Airflow orchestration tasks, eliminating JVM startup latency (60s → 0.3s per task) and Maven dependency failures in containerised CI environments
- Wrote comprehensive project documentation covering system design rationale, per-technology feature inventory, interview Q&A, and a 6-month learning roadmap for junior engineers

---

## 21. GitHub README Template

```markdown
# Real-Time Ride-Sharing Analytics Platform

> End-to-end Big Data pipeline: Kafka → PySpark Streaming → Delta Lake → XGBoost → Streamlit

[Architecture diagram here — export from draw.io as PNG]
[Demo GIF here — screen recording of live dashboard]

## What This Solves
Ride-sharing platforms process millions of location and fare events per hour.
This project builds the core data infrastructure: a real-time pipeline that
ingests ride events, computes live KPIs, predicts surge pricing, and serves
a live operations dashboard — modelled on Uber/Ola internals.

## Tech Stack
| Tool | Version | Role |
|---|---|---|
| Apache Kafka | 3.6 | Event streaming |
| PySpark | 3.5 | Batch + streaming processing |
| Delta Lake | 3.1 | ACID storage, medallion architecture |
| XGBoost + MLflow | 2.0 + 2.11 | Surge prediction + experiment tracking |
| Apache Airflow | 2.8 | Pipeline orchestration |
| Redis | 7.2 | Sub-second cache for live data |
| Streamlit | 1.32 | Live KPI dashboard |
| Docker Compose | 3.8 | Full stack containerization |

## Setup (Linux — runs in one command)
\`\`\`bash
git clone https://github.com/YOUR_USERNAME/rideshare-analytics
cd rideshare-analytics
cp .env.example .env
docker compose up -d
# Dashboard: http://localhost:8501
# Airflow:   http://localhost:8080  (admin/admin)
# MLflow:    http://localhost:5000
# Spark UI:  http://localhost:4040
\`\`\`

## Design Decisions
**Why Kafka over a database queue?** Kafka retains messages for 7 days,
enabling replay of any time window if a downstream job fails. Critical
for a pipeline where Spark jobs may need to reprocess after a bug fix.

**Why Delta Lake over Parquet?** ACID transactions prevent partial writes.
MERGE INTO enables idempotent Silver upserts. Time travel enables auditing.

**Why XGBoost over neural nets?** 11 tabular features at this scale —
gradient boosted trees consistently outperform neural nets on tabular data,
train in seconds, and produce interpretable feature importances.

**Why synthetic data?** Full control over volume, patterns, and edge cases
(late events, GPS drift, schema evolution). Real datasets are static — this
pipeline streams live, requiring a live data source.

## What I Would Do Differently
- Replace local Delta Lake with cloud object storage (S3) for horizontal scale
- Add Flink for sub-second stateful stream processing
- Implement data contracts with schema registry (Confluent)

## Project Structure
[Link to repo structure]

## Author
Madhav | MTech Data Engineering | IIIT Allahabad
[LinkedIn] [Resume PDF]
```

---

## 22. Build Order & Agent Instructions

### For an AI Agent: Follow This Exact Sequence

```
PHASE 0 — Environment (Day 1)
  ✓ Verify Python 3.11, Java 17, Docker, Git installed
  ✓ Create repo, virtual env, install all pinned dependencies
  ✓ Create .env, .gitignore, directory structure
  ✓ First commit: "Day 1: project scaffold"

PHASE 1 — Simulator (Days 2–4)
  ✓ Implement simulator/config.py with all zone definitions
  ✓ Implement simulator/ride_generator.py with all realistic patterns
  ✓ Implement simulator/driver_generator.py
  ✓ Implement simulator/payment_generator.py
  ✓ Implement simulator/kafka_producer.py
  ✓ Run Kafka in Docker (standalone, not compose yet)
  ✓ Run simulator — verify messages arriving in all 3 topics
  ✓ Commit: "Module 1: data simulator with realistic patterns"

PHASE 2 — Spark Batch (Days 5–7)
  ✓ Dump 1000 Kafka messages to JSON files (manual step)
  ✓ Implement processing/batch/bronze_to_silver.py
  ✓ Run batch job on JSON dump — verify Silver output
  ✓ Implement processing/batch/silver_to_gold.py
  ✓ Verify Gold tables have correct shape and values
  ✓ Write unit tests for all transforms
  ✓ Commit: "Module 2: batch Bronze→Silver→Gold transforms"

PHASE 3 — Spark Streaming (Days 8–11)
  ✓ Implement processing/streaming/kafka_to_bronze.py
  ✓ Run streaming job — verify Bronze Delta table populating
  ✓ Add watermark for late event handling
  ✓ Verify Bronze has correct schema and partition structure
  ✓ Commit: "Module 3: Kafka to Bronze streaming pipeline"

PHASE 4 — Delta Lake (Days 12–14)
  ✓ Implement MERGE INTO Silver upsert pattern
  ✓ Verify deduplication works on repeated run
  ✓ Build all 5 Gold tables
  ✓ Run time travel query — demonstrate versioning
  ✓ Run OPTIMIZE + ZORDER on Silver and Gold
  ✓ Commit: "Module 4: complete medallion architecture"

PHASE 5 — ML + MLflow (Days 15–18)
  ✓ Implement ml/feature_engineering.py
  ✓ Implement ml/train_surge_model.py
  ✓ Run training — verify MLflow UI shows experiment
  ✓ Register best model in MLflow registry as Production
  ✓ Implement ml/serve_predictions.py (Pandas UDF)
  ✓ Run prediction UDF on a sample Silver batch
  ✓ Commit: "Module 5: XGBoost surge model with MLflow"

PHASE 6 — Airflow (Days 19–22)
  ✓ Init Airflow, verify webserver running
  ✓ Implement gold_refresh_dag.py with full idempotency
  ✓ Implement ml_retrain_dag.py
  ✓ Implement data_quality_dag.py
  ✓ Implement dashboard_warmup_dag.py
  ✓ Trigger all DAGs manually — verify no errors
  ✓ Commit: "Module 6: Airflow orchestration — 4 DAGs"

PHASE 7 — Redis (Days 23–24)
  ✓ Implement storage/redis_client.py
  ✓ Write a Spark streaming job that writes live zone demand to Redis
  ✓ Verify Redis keys populating with correct TTLs
  ✓ Commit: "Module 7: Redis cache layer"

PHASE 8 — Dashboard (Days 25–28)
  ✓ Implement dashboard/app.py with tab layout
  ✓ Implement all 5 components
  ✓ Connect to Redis for live data, Gold Delta for historical
  ✓ Verify dashboard runs with `streamlit run dashboard/app.py`
  ✓ Record demo GIF
  ✓ Commit: "Module 8: Streamlit live dashboard"

PHASE 9 — Docker Compose (Days 29–31)
  ✓ Write docker-compose.yml with all 7 services
  ✓ Write Dockerfile.simulator and Dockerfile.dashboard
  ✓ Run `docker compose up -d` — verify all services healthy
  ✓ Run `docker compose down -v && docker compose up -d` — verify idempotent
  ✓ Commit: "Module 9: full stack Docker Compose"

PHASE 10 — Polish (Days 32–35)
  ✓ Write README.md with architecture diagram (Excalidraw/draw.io)
  ✓ Add Design Decisions section to README
  ✓ Add setup instructions — test on fresh terminal
  ✓ Add .env.example (no real secrets)
  ✓ Final commit: "v1.0: production-ready rideshare analytics platform"
```

### Agent Decision Rules
- If a library version is not specified above, do not use it — ask for the version first
- If a design decision is not specified, default to the simpler option and add a TODO comment
- If a streaming job fails, fall back to batch mode first, verify transforms, then retry streaming
- Never use `mode("overwrite")` on Silver or Gold tables — always use MERGE INTO
- Never commit `.env` files or Delta Lake data directories
- Run unit tests after every module before moving to the next
- If Docker memory issues arise, reduce `spark.driver.memory` to `1g` first

---

*This document is the single source of truth for the rideshare-analytics project.
All implementation decisions not covered here should default to simplicity and be documented in a TODO comment for future revision.*
