# Real-Time Ride-Sharing Analytics Platform

An end-to-end data engineering project that mirrors the internals of Uber / Ola / Rapido.
A synthetic ride simulator produces events that travel through Apache Kafka into a
Delta Lake medallion architecture, an XGBoost surge-price model tracked with MLflow,
four production Airflow DAGs, a Redis cache layer, and a live Streamlit dashboard —
all containerised with Docker Compose.

---

## What This Project Demonstrates

| Concern | Tool | What We Actually Do |
|---|---|---|
| Real-time event ingestion | Apache Kafka | 3 topics, threaded producers, key-based partitioning |
| Stream processing | PySpark Structured Streaming | Kafka → Bronze Delta with watermarks for late events |
| Reliable lakehouse storage | Delta Lake (medallion) | Bronze → Silver (dedup + enrich) → Gold (hourly KPIs) |
| Pipeline orchestration | Apache Airflow | 4 DAGs: gold refresh, ML retrain, data quality, cache warmup |
| Sub-second dashboard reads | Redis | Kafka consumer seeds zone demand every event; 15-min TTL warmup from DAG |
| ML training + tracking | XGBoost + MLflow | Surge multiplier regressor, experiment registry, promotion gate |
| Live dashboard | Streamlit | KPI counters, demand heatmap, revenue charts, driver metrics |
| Reproducibility | Docker Compose | 8 services, one command, shared bind-mount Delta storage |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     SYNTHETIC DATA SIMULATOR                     │
│  ride_generator ──┐                                              │
│  driver_generator─┼──► kafka_producer.py ──► Kafka (3 topics)   │
│  payment_generator┘                                              │
└──────────────────────────────────────────────────────────────────┘
                               │
               ┌───────────────┼───────────────┐
               ▼               ▼               ▼
        rides-events    driver-events   payment-events
               │
    ┌──────────▼────────────────────────────────────┐
    │       PySpark Structured Streaming             │
    │          (Spark container, Kafka → Bronze)     │
    │  • JSON parsing  • schema enforcement          │
    │  • partitioned by city_zone                    │
    │  • append-only, checkpoint-backed              │
    └──────────┬────────────────────────────────────┘
               │ Bronze Delta table
    ┌──────────▼────────────────────────────────────┐
    │     Bronze → Silver (streaming transform)      │
    │  • dropDuplicates(ride_id)                     │
    │  • cast types, compute gross_fare_inr          │
    │  • derive event_hour, event_date, is_completed │
    │  • partitioned by event_date                   │
    └──────────┬────────────────────────────────────┘
               │ Silver Delta table (rides_clean)
    ┌──────────▼────────────────────────────────────┐  ◄── Airflow gold_refresh_dag (hourly)
    │     Silver → Gold (Airflow batch via deltalake)│
    │  • groupby event_date / event_hour / city_zone │
    │  • ride_count, completed, cancelled            │
    │  • gross_revenue_inr, avg_surge_multiplier     │
    │  • write_deltalake(mode="overwrite")           │
    └────┬─────────────────────────────┬────────────┘
         │ Gold Delta table            │
         ▼                             ▼
  ┌──────────────┐           ┌─────────────────────┐
  │ MLflow       │           │ Redis Cache          │
  │ XGBoost      │           │ live:zone:*:demand   │
  │ surge model  │           │ dashboard:*          │
  │ (daily 2am)  │           │ (15-min TTL warmup)  │
  └──────────────┘           └──────────┬──────────┘
                                        │
                             ┌──────────▼──────────┐
                             │  Streamlit Dashboard │
                             │  • Live KPI counters │
                             │  • Demand heatmap    │
                             │  • Revenue charts    │
                             │  • Driver metrics    │
                             │  • Pipeline health   │
                             └─────────────────────┘

 Also running in parallel:
   live_writer → Kafka consumer → seeds Redis on every ride event (sub-second)
   Airflow data_quality_dag (30min) → null checks, surge bounds, timestamp freshness
   Airflow dashboard_warmup_dag (15min) → precomputes Redis keys from Delta tables
```

---

## Repository Layout

```
ride-sharing/
├── simulator/                  Synthetic event generators (ride, driver, payment)
│   ├── config.py               City zones, vehicle types, peak hours, weather states
│   ├── ride_generator.py       Surge logic, GPS noise, late-event simulation
│   ├── driver_generator.py     Driver status events
│   ├── payment_generator.py    Payment events linked to ride_ids
│   ├── kafka_producer.py       3 threaded KafkaProducers
│   └── run_simulator.py        Entry point
│
├── ingestion/
│   └── kafka_config.py         Topic names, partition counts, bootstrap servers
│
├── live_writer/
│   ├── create_topics.py        Creates Kafka topics on startup (kafka_init container)
│   └── run_live_writer.py      Kafka consumer → Redis seeder (real-time zone demand)
│
├── processing/
│   ├── batch/
│   │   ├── bronze_to_silver.py Silver transform function (shared by stream + batch)
│   │   ├── silver_to_gold.py   PySpark Gold aggregation
│   │   ├── delta_utils.py      Delta MERGE helper
│   │   └── run_gold_standalone.py  One-shot Gold population script
│   └── streaming/
│       ├── kafka_to_bronze.py          Kafka → Bronze Delta (3 streams)
│       ├── bronze_to_silver_stream.py  Bronze → Silver Delta (streaming)
│       ├── surge_prediction_stream.py  Silver → Gold/surge_predictions (ML UDF)
│       └── run_streaming_pipeline.py   Entry point for Spark container
│
├── storage/
│   ├── delta_config.py         Table path constants (reads DELTA_*_PATH env vars)
│   ├── redis_client.py         Singleton Redis client, set_zone_demand helpers
│   └── spark_session.py        SparkSession builder with Delta + Kafka JARs
│
├── ml/
│   ├── feature_engineering.py  pd.get_dummies + numeric prep for XGBoost
│   ├── train_surge_model.py    XGBRegressor + mlflow.xgboost.log_model
│   ├── train_surge_model_nyc.py XGBRegressor on NYC data, time-based split
│   ├── nyc_taxi_loader.py      Downloads + maps NYC TLC parquet to Silver schema
│   ├── evaluate_model.py       MAE, RMSE (math.sqrt), R² metrics
│   └── serve_predictions.py    Pandas UDF wrapping MLflow model for Spark serving
│
├── common/
│   └── logging_config.py       Shared structured logger used by processing/batch
│
├── orchestration/
│   ├── dags/
│   │   ├── gold_refresh_dag.py       Hourly: Silver → Gold via deltalake + pandas
│   │   ├── ml_retrain_dag.py         Daily 2am: XGBoost retrain + MLflow promotion
│   │   ├── data_quality_dag.py       30-min: null, bounds, freshness checks
│   │   └── dashboard_warmup_dag.py   15-min: Redis cache pre-warm from Delta
│   └── plugins/
│       └── delta_operators.py        DeltaMergeOperator, DeltaVacuumOperator
│
├── dashboard/
│   ├── app.py                  Streamlit entry point (JS auto-refresh, no extra package)
│   ├── components/             live_counter, demand_heatmap, revenue_charts,
│   │                           driver_utilisation, pipeline_health
│   └── data_connectors/        delta_reader (reads Gold Delta), redis_reader
│
├── tests/                      pytest suite for all transforms + quality checks
├── notebooks/                  01 Bronze · 02 Silver · 03 Gold KPIs · 04 ML
│
├── docker-compose.yml          8 services: zookeeper, kafka, kafka_init, redis,
│                               spark, mlflow, airflow, simulator, dashboard, live_writer
├── Dockerfile.airflow          apache/airflow:2.8.1 + deltalake + pyarrow (no PySpark)
├── Dockerfile.spark            Custom Spark image with Delta + Kafka JARs
├── Dockerfile.dashboard        python:3.11-slim + streamlit deps
├── Dockerfile.simulator        python:3.11-slim + kafka-python + faker
├── Dockerfile.live_writer      python:3.11-slim + kafka-python + redis
│
├── requirements-airflow.txt    deltalake, pyarrow, mlflow, xgboost, sklearn, pandas, redis, requests
├── requirements-spark.txt      pyspark, delta-spark, kafka-python
├── requirements-dashboard.txt  streamlit, pydeck, folium, pandas, redis
├── requirements-simulator.txt  kafka-python, faker, numpy
└── requirements-live-writer.txt kafka-python, redis
```

---

## Quick Start

### Prerequisites

```bash
# Verify installed
docker --version          # 24.x+
docker compose version    # 2.x+
python3 --version         # 3.11.x (only needed for local dev / tests)
```

> Java is **not** required on your host — the Spark and Airflow containers bundle their own JVM.

### Step 1 — Clone and configure

```bash
git clone https://github.com/MadhavKamble/realtime-rideshare-pipeline.git
cd realtime-rideshare-pipeline
cp .env.example .env          # defaults are fine for local Docker setup
```

### Step 2 — Build and start

```bash
docker compose up -d --build
```

This starts 10 services in order. First startup takes 3–5 minutes as images build and Kafka topics are created.

### Step 3 — Verify everything is running

```bash
docker compose ps
```

Expected: all services `running` or `exited 0` (kafka_init exits after creating topics).

```bash
# Watch the simulator producing events
docker compose logs -f simulator

# Watch Spark streaming writing to Delta
docker compose logs -f spark

# Watch Airflow schedule runs
docker compose logs -f airflow
```

### Step 4 — Open the UIs

| Service | URL | Credentials |
|---|---|---|
| Streamlit Dashboard | http://localhost:8501 | — |
| Airflow UI | http://localhost:8080 | admin / admin |
| MLflow UI | http://localhost:5000 | — |
| Spark Streaming UI | http://localhost:4040 | — |

### Step 5 — Trigger Airflow DAGs manually (optional)

The DAGs run on schedule automatically, but to see results immediately:

1. Open http://localhost:8080
2. Enable all 4 DAGs (toggle the slider next to each)
3. Click the play button → "Trigger DAG" on `gold_refresh_dag` first

Wait ~30 seconds, then the dashboard at http://localhost:8501 will show live Gold data.

---

## Starting, Stopping, and Managing the Project

### First time setup (run once)

```bash
git clone https://github.com/MadhavKamble/realtime-rideshare-pipeline.git
cd realtime-rideshare-pipeline
cp .env.example .env
docker compose up -d --build
```

`--build` tells Docker to build the custom images (Spark, Airflow, Dashboard, Simulator, Live Writer). This takes 3–5 minutes the first time. You only need `--build` again if you change a `Dockerfile` or a `requirements-*.txt` file.

---

### Start the project (day-to-day)

```bash
docker compose up -d
```

No `--build` needed. All your previous data (Delta tables, Airflow run history, MLflow experiments) is exactly where you left it. Takes about 30 seconds for everything to be ready.

---

### Stop the project — DATA IS KEPT SAFE

```bash
docker compose down
```

This stops and removes all running containers. **Your data is not touched.** When you start again, everything picks up from where it left off — Spark resumes streaming from the last Kafka checkpoint, Airflow schedules continue, Delta tables are intact.

What is kept:
- `./data/delta/` — all Bronze, Silver, Gold tables (lives on your hard drive, not inside Docker)
- Airflow run history and schedules
- MLflow experiments and models
- Redis data (persisted to disk inside Docker)
- Kafka message offsets

---

### Stop the project — WIPE EVERYTHING (full reset)

Only do this when you want a completely fresh start — like if the database is corrupted or you want to re-run the whole pipeline from scratch.

```bash
docker compose down -v          # stops containers AND deletes Docker-managed data
rm -rf data/delta               # deletes all Delta tables (Bronze, Silver, Gold)
```

Then start fresh:

```bash
docker compose up -d --build
```

What gets deleted:
- `docker compose down -v` deletes: Airflow database, MLflow experiments, Kafka messages, Redis cache
- `rm -rf data/delta` deletes: all your Delta Lake tables (the actual ride data)

> Delta tables live in `./data/delta/` on your machine (not inside Docker), so `docker compose down -v` alone does **not** delete them. You must run `rm -rf data/delta` manually if you want to wipe the ride data too.

---

### Other useful commands

```bash
# See what is currently running
docker compose ps

# Restart just one service (e.g. after editing a DAG file)
docker compose restart airflow

# Rebuild and restart just one service (after changing requirements-airflow.txt)
docker compose build airflow && docker compose up -d airflow

# Watch live logs from a specific service
docker compose logs -f spark        # Spark streaming activity
docker compose logs -f airflow      # Airflow task runs
docker compose logs -f simulator    # Events being produced to Kafka
docker compose logs -f live_writer  # Redis being seeded

# Stop one service without stopping everything
docker compose stop simulator       # stop event production
docker compose start simulator      # resume it

# Open a shell inside a running container (for debugging)
docker compose exec airflow bash
docker compose exec spark bash
```

---

### Quick reference summary

| What you want to do | Command |
|---|---|
| First time setup | `docker compose up -d --build` |
| Start the project | `docker compose up -d` |
| Stop, keep all data | `docker compose down` |
| Wipe Docker data (keep Delta tables) | `docker compose down -v` |
| Wipe everything including Delta tables | `docker compose down -v && rm -rf data/delta` |
| Restart one service | `docker compose restart <service>` |
| Rebuild one image after code change | `docker compose build <service> && docker compose up -d <service>` |
| See running services | `docker compose ps` |
| Watch logs | `docker compose logs -f <service>` |

---

## How the Data Flows (step by step)

1. **Simulator** generates ride/driver/payment JSON events every ~125ms and publishes to Kafka.

2. **Kafka** stores events in 3 topics (`rides-events`, `driver-events`, `payment-events`). Messages are retained for replay; partition key = `city_zone` keeps same-zone events ordered.

3. **Spark streaming** (`kafka_to_bronze.py`) reads from Kafka, parses JSON using a typed schema, and appends to Bronze Delta tables partitioned by `city_zone`. Checkpoints ensure exactly-once.

4. **Spark streaming** (`bronze_to_silver_stream.py`) reads Bronze Delta as a stream, deduplicates on `ride_id`, computes `gross_fare_inr = fare_base_inr × surge_multiplier`, derives `event_hour`/`event_date`/`is_completed`, and writes Silver Delta partitioned by `event_date`.

5. **live_writer** simultaneously reads Kafka (separate consumer group), maintains a 5-minute rolling window in memory, and writes zone demand + ride counts to Redis with 30-second TTL. This is the "live" path for the dashboard.

6. **Airflow `gold_refresh_dag`** (hourly) reads Silver via the `deltalake` Python library (pure Rust, no JVM), runs a pandas groupby to compute hourly KPIs per zone, and writes Gold Delta.

7. **Airflow `dashboard_warmup_dag`** (15-min) reads Gold + Silver via `deltalake`, computes aggregate metrics, and pre-loads Redis keys like `dashboard:total_revenue` and `dashboard:avg_surge`.

8. **Airflow `ml_retrain_dag`** (daily 2am) loads Silver via `deltalake`, runs feature engineering, trains XGBoost, logs to MLflow, and promotes if MAE < 15.

9. **Airflow `data_quality_dag`** (30-min) checks for NULL `ride_id`s, verifies surge multipliers are in `[1.0, 3.5]`, checks Gold is non-empty, and warns if >10% events are stale.

10. **Streamlit Dashboard** reads from Redis for live metrics, from Gold Delta for historical charts. Auto-refreshes every 30 seconds via a JavaScript `setTimeout` injected into the page.

---

## Key Architecture Decisions

### Why `deltalake` (Python) in Airflow instead of PySpark?

Airflow DAGs originally used PySpark to read Delta tables. This caused a critical failure: Spark's JAR resolution (`spark.jars.packages`) tries to download Delta/Kafka JARs from Maven at runtime inside the container. When Maven is unreachable, the SparkSession silently falls back to a minimal session that cannot read Delta format. The DAGs were running green but computing from Redis estimates, not real data.

The fix: replaced PySpark in Airflow with the `deltalake` Python library — a Rust-based implementation that reads Delta tables natively, requires no JVM, starts in milliseconds, and has zero Maven dependency. `DeltaTable(path).to_pandas()` gives you a pandas DataFrame. Spark remains only in the dedicated Spark container for streaming.

### Why a bind mount (`./data/delta:/data/delta`) instead of a Docker named volume?

Initially, `delta_data` was a named Docker volume mounted at `/data/delta` in Spark, Airflow, and Dashboard containers. Named volumes are managed by Docker daemon — they live inside `/var/lib/docker/volumes/`, not your project directory. The symptom: Spark wrote Delta files to `/app/data/delta` (its working directory bind mount), Airflow read from `/data/delta` (the named volume) — two different filesystems. Airflow saw an empty table.

The fix: use a bind mount `./data/delta:/data/delta` (a directory on your host machine, mounted into every container at the same absolute path `/data/delta`). All containers now share the same physical files. You can also browse the files directly with `ls data/delta/`.

### Why separate `deltalake` and `pyspark` requirements?

Different containers need different things. The Spark container needs the full PySpark + Delta-Spark (JVM) stack for streaming. Airflow only needs to read Delta tables for orchestration decisions — using the lightweight Rust `deltalake` library saves ~1GB of container image size and eliminates JVM startup latency (10s → 0.3s per task).

### Why Redis in front of Delta for the dashboard?

Reading a Delta table requires: deserializing Parquet files, reading the Delta transaction log, and (in PySpark) JVM startup. Total: 5–15 seconds per read. The dashboard needs sub-second response. Redis stores a hot slice of Gold metrics with 15-minute TTL. The `dashboard_warmup_dag` refreshes those keys every 15 minutes. Result: dashboard reads take ~1ms.

### Why CMD not ENTRYPOINT in Dockerfiles?

Docker Compose's `command:` field **appends** to `ENTRYPOINT` but **replaces** `CMD`. If the Dockerfile uses `ENTRYPOINT ["python"]` and Compose says `command: -m simulator.run_simulator`, Docker runs `python -m simulator.run_simulator`. But if the Dockerfile uses `ENTRYPOINT ["python", "-m", "simulator.run_simulator"]` and Compose also says `command: python -m simulator.run_simulator`, Docker runs `python -m simulator.run_simulator python -m simulator.run_simulator` — doubled and broken. Using `CMD` everywhere means Compose always cleanly overrides.

### Why a synthetic simulator instead of replaying static NYC data for the live path?

The live Kafka → Bronze → Silver path exists to demonstrate real-time stream processing — watermarking, checkpointed exactly-once writes, dedup under backpressure. Replaying a static historical file on a timer would fake the "live" story without exercising any of that: no genuinely unbounded arrival order, no late events, no realistic partition skew. The synthetic simulator generates events with jittered timestamps and a configurable late-arrival rate, so the streaming layer has real out-of-order data to handle. NYC TLC data instead plays a different role — see the next decision.

### Why NYC TLC data for ML training instead of synthetic ride events?

The synthetic simulator's `surge_multiplier` is generated from the same formula the model would be asked to learn (`compute_surge_multiplier` in `simulator/config.py`), so training on it mostly teaches the model to invert a known function — not a realistic fare/distance/tip relationship. NYC TLC data has real fare, distance, and tip patterns, giving the surge model something non-trivial to learn. It's loaded into a separate historical table (`rides_historical_nyc`, not `rides_clean`) rather than merged into the live schema, because NYC's `PULocationID` zone buckets (`zone_A`..`zone_E`) are a different taxonomy from the simulator's named zones (`airport`, `cbd`, `mall`, `railway_station`, `residential`) that the dashboard heatmap and data-quality checks depend on — merging them would silently corrupt zone-based aggregations with five phantom zones that have no coordinates.

### Why a time-based train/test split for the NYC model?

Random split leaks future data into training — a row from January 31st sitting in the training set while a row from January 5th sits in the test set means the model implicitly learns from the future to predict the past, which never happens in production. `ml/train_surge_model_nyc.py` sorts by `event_timestamp` and trains on the first 80% chronologically, testing on the last 20%, which simulates how the model would actually be deployed: trained on the past, evaluated on data it hasn't seen yet.

### Why watermarking is necessary for production streaming

Without a watermark, `dropDuplicates` on a streaming DataFrame keeps state for every key it has ever seen, forever — memory grows unbounded for as long as the query runs. A 10-minute watermark tells Spark it's safe to forget state for any event-time older than (max event-time seen so far − 10 minutes), bounding memory at the cost of dropping any event that arrives later than that. For a ride-sharing pipeline, 10 minutes is a conservative buffer against the simulator's late-event injection (0–120 seconds), so it trades away only intentionally-extreme stragglers.

---

## Known Limitations

- **NYC Taxi data covers 2023-01 only** — the ML model is trained on that single month of yellow taxi trips; the live simulator continues to use synthetic data for the streaming demo, so the two data sources never overlap in time or represent the same city.
- **Watermark set to 10 minutes** — late events beyond this window are dropped rather than processed, on the Bronze→Silver rides, drivers, and payments streams.

---

## Interview Talking Points

**Q: Why not use PySpark in Airflow?**
A: PySpark in Airflow silently fell back to a non-Delta-aware session when Maven JAR resolution failed, causing DAGs to run green while reading empty data. Switching to the Rust-backed `deltalake` library eliminated the JVM dependency and reduced per-task startup from ~10s to ~0.3s.

**Q: Why a bind mount instead of a named Docker volume for Delta tables?**
A: A named volume put Airflow's read path and Spark's write path on two different filesystems, so Airflow always saw an empty table despite Spark writing successfully. A host bind mount at the same absolute path in every container guarantees they're reading and writing the identical files.

**Q: Why generate synthetic events instead of replaying real historical data for the live pipeline?**
A: Replaying a static file wouldn't exercise genuine out-of-order arrival, late events, or partition skew — the exact conditions the streaming layer (watermarking, dedup, checkpointing) exists to handle. The simulator injects jittered timestamps and a configurable late-arrival rate so the "real-time" story is actually real.

**Q: Why time-based split instead of random split for the NYC model?**
A: Random split let rows from the end of the month leak into training data used to predict rows from earlier in the month — an information leak that never happens in production, where you only ever have the past to predict the future. Sorting by `event_timestamp` and taking the last 20% as test data reproduces that real deployment constraint. `train_surge_model_nyc.py` prints both MAEs side by side specifically so you can check, on your own run, whether the time-based split's MAE comes out worse — that gap (if present) is the leakage the random split was hiding.

**Q: Why does the streaming pipeline need a watermark at all?**
A: Structured Streaming's `dropDuplicates` keeps a row of state per key indefinitely unless told otherwise, so a long-running query without a watermark eventually exhausts memory. A 10-minute watermark bounds that state at the cost of dropping events later than 10 minutes past the latest-seen event time — a deliberate tradeoff, since the simulator only ever injects up to 2 minutes of lateness.

---

## Running Tests

```bash
# In a local virtualenv (no Docker needed)
python3 -m venv venv && source venv/bin/activate
pip install pyspark==3.5.1 deltalake pyarrow pandas xgboost scikit-learn pytest

pytest tests/ -v
```

| Test file | What it covers |
|---|---|
| `test_simulator.py` | Event schema, surge bounds stay in `[1.0, 3.5]` |
| `test_silver_transforms.py` | Rides: dedup, gross_fare_inr, event_hour, is_completed. Drivers: dedup, is_available, current_zone cleanup. Payments: dedup, payment_method_clean, is_completed |
| `test_gold_aggregations.py` | ride_count, completed/cancelled, revenue, avg surge per group |
| `test_feature_engineering.py` | One-hot encoding, numeric prep, target extraction |
| `test_data_quality.py` | NULL detection, surge out-of-bounds, empty table handling |

---

## Notebooks

```bash
pip install jupyter matplotlib seaborn deltalake pyarrow
jupyter notebook notebooks/
```

| Notebook | What to explore |
|---|---|
| `01_explore_bronze.ipynb` | Raw Kafka event structure, duplicate analysis, Delta time travel |
| `02_explore_silver.ipynb` | Cleaned data quality, hourly volume charts, revenue by zone |
| `03_gold_kpi_analysis.ipynb` | Peak/off-peak comparison, revenue heatmap by zone |
| `04_ml_experimentation.ipynb` | XGBoost training, feature importance, residual analysis, MLflow runs |

---

## Troubleshooting

**Dashboard shows "no data"**
Run `gold_refresh_dag` manually in Airflow (http://localhost:8080). The Gold table must have at least one row.

**Airflow DAG import error / not showing up**
```bash
docker compose exec airflow python /opt/airflow/dags/gold_refresh_dag.py
```
This prints the full traceback if there's an import error.

**Spark not writing Delta files**
```bash
docker compose logs spark | grep -E "error|exception|ERROR" -i
```
Check that `./data/delta/` exists on the host — Spark's bind mount requires the directory to pre-exist.

**Redis empty / dashboard showing zeros**
```bash
docker compose logs live_writer
```
live_writer must successfully consume from Kafka. If Kafka isn't ready yet, it retries automatically.

**Full reset**
```bash
docker compose down -v && rm -rf data/delta && docker compose up -d --build
```
