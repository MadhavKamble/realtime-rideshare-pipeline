# Complete Learning Guide — Real-Time Ride-Sharing Analytics Platform

> Written for someone starting from scratch in data engineering.
> This guide explains every technology used, exactly what features of each we used,
> why we made specific choices, what to learn before each tool, and how everything
> connects into a coherent system.

---

## Table of Contents

1. [How to Read This Guide](#1-how-to-read-this-guide)
2. [System Design — The Big Picture](#2-system-design--the-big-picture)
3. [Technology Deep Dives](#3-technology-deep-dives)
   - [Python (the glue)](#31-python)
   - [Apache Kafka](#32-apache-kafka)
   - [Apache Spark + PySpark](#33-apache-spark--pyspark)
   - [Delta Lake](#34-delta-lake)
   - [Apache Airflow](#35-apache-airflow)
   - [Redis](#36-redis)
   - [XGBoost + scikit-learn](#37-xgboost--scikit-learn)
   - [MLflow](#38-mlflow)
   - [Streamlit](#39-streamlit)
   - [Docker + Docker Compose](#310-docker--docker-compose)
4. [Architecture Decisions — Why We Did Things This Way](#4-architecture-decisions--why-we-did-things-this-way)
5. [Data Schemas — What Every Field Means](#5-data-schemas--what-every-field-means)
6. [Interview Preparation — Q&A With Full Reasoning](#6-interview-preparation--qa-with-full-reasoning)
7. [Learning Roadmap — What to Study First](#7-learning-roadmap--what-to-study-first)

---

## 1. How to Read This Guide

This project contains ~10 technologies. Most of them are interconnected — you cannot fully understand Airflow without understanding Delta Lake, because Airflow reads Delta tables. The recommended reading order:

```
Python → Kafka → Spark (batch) → Delta Lake → Spark (streaming) → Airflow → Redis → MLflow → XGBoost → Streamlit → Docker
```

For each technology section, you will find:
- **What is it?** — Simple intuitive explanation
- **What we used from it** — Only the exact features, APIs, and configs used in this project
- **Why we chose it over alternatives** — The reasoning for interviews
- **Prerequisites** — What you should know before studying this tool
- **How to verify you understand it** — Questions to ask yourself

---

## 2. System Design — The Big Picture

### The Problem We're Solving

Imagine Ola at 6pm on a Friday in Bangalore. Thousands of people are requesting rides simultaneously in the airport zone. Ola needs to:

1. **Know about every ride request the instant it happens** (streaming ingestion)
2. **Store all events durably** so nothing is lost if a system crashes (reliable storage)
3. **Clean and standardize the data** before analysis (data quality)
4. **Compute business KPIs hourly** — how many rides, how much revenue, which zones are busy (batch aggregation)
5. **Predict what surge price to show** the next rider based on current demand (ML)
6. **Show operations teams a live dashboard** without querying raw data every time (caching)
7. **Run all of this reliably on a schedule** without manual intervention (orchestration)

This project implements all 7 of those concerns using the exact tools that companies like Ola, Uber, and Swiggy use in production.

### Data Flow Overview

```
PRODUCER SIDE                   TRANSPORT             STORAGE + PROCESSING
─────────────                   ─────────             ───────────────────
Simulator generates          Kafka stores            Delta Lake stores
JSON ride events  ────────►  events by              events as Parquet
(~8 events/sec)              topic/partition         files with ACID

                              live_writer             Redis stores
                              reads Kafka ──────────► hot metrics
                              (sub-second)            (sub-second reads)

                              Spark streaming
                              reads Kafka ──────────► Bronze Delta
                                                       ↓
                                                      Silver Delta
                                                       ↓
                              Airflow (hourly) ──────► Gold Delta
                                                       ↓
                              Airflow (daily) ───────► MLflow model
                                                       ↓
CONSUMER SIDE                                         Streamlit
─────────────                                         Dashboard
Dashboard reads Redis (live) + Delta (historical)
```

### Why This Architecture?

Each component solves a specific problem that the previous one can't:

| Problem | Why the previous tool fails | Solution |
|---|---|---|
| Events arrive faster than a database can write | Relational DB write latency ~5ms, 8 events/sec needs a buffer | Kafka: durable buffer, consumers can lag safely |
| Raw Kafka events have duplicates and type errors | Can't build reliable KPIs on dirty data | Spark transforms to Silver (deduplicated, typed) |
| Dashboard needs sub-second reads | Reading Delta files: 5–15 seconds per query | Redis: 1ms reads for hot metrics |
| Need to retrain model daily automatically | Running Python scripts manually is unreliable | Airflow: scheduled, retried, monitored |

---

## 3. Technology Deep Dives

---

### 3.1 Python

**What is it?**
The primary language for this entire project. Every component except Spark's internal execution engine is Python.

**What we used from it in this project:**

| Feature | Where | Why |
|---|---|---|
| `threading.Thread` | `simulator/kafka_producer.py` | Run 3 Kafka producers concurrently (rides, drivers, payments) |
| `dataclasses` / plain `dict` | Event generators | Simple data containers for JSON serialization |
| `collections.deque` | `live_writer/run_live_writer.py` | Efficient rolling time window — O(1) append + popleft |
| `collections.defaultdict` | `live_writer/run_live_writer.py` | Auto-initialise zone event buckets |
| `datetime`, `timezone`, `timedelta` | Everywhere | UTC-aware timestamps; TTL calculations |
| `json.loads` / `json.dumps` | Kafka producer, Redis | Serialize events to bytes; deserialize from bytes |
| `os.getenv` | Config files | Read environment variables without hardcoding secrets |
| `uuid.uuid4()` | Simulator | Generate unique ride/driver/payment IDs |
| `random.choices(weights=...)` | Simulator | Weighted sampling for zones and vehicle types |
| `numpy.random.normal` | Simulator | Gaussian GPS coordinate generation around zone centers |
| `from __future__ import annotations` | All files | Enable PEP 604 `X | Y` type hints on Python 3.9+ |

**Prerequisites to learn first:**
- Python basics: variables, loops, functions, classes
- Standard library: `json`, `os`, `datetime`, `collections`
- Python packaging: `pip`, `requirements.txt`, virtual environments

---

### 3.2 Apache Kafka

**What is it?**
Kafka is a distributed event streaming platform. Think of it as a durable, high-throughput message bus. When the simulator generates a ride event, it publishes to Kafka. Any number of consumers (Spark, live_writer, future systems) can read that event independently, at their own pace, and from any offset.

The key mental model: Kafka is a **commit log**. Events are written sequentially to disk. Consumers track their position (offset) independently. This means you can replay events from the beginning, which is impossible with traditional message queues that delete messages after delivery.

**What we used from it in this project:**

#### Topics

We created 3 topics in `live_writer/create_topics.py`:

```
rides-events       4 partitions    7-day retention
driver-events      3 partitions    7-day retention
payment-events     3 partitions    7-day retention
```

**Why different partition counts?**
Partitions = parallel consumers. `rides-events` gets 4 partitions because it's the highest-volume topic and we want Spark to consume it with 4 parallel tasks. Driver and payment events are lower volume — 3 partitions is enough.

**Why key-based partitioning?**
In `simulator/kafka_producer.py`, ride events are published with `key=event["city_zone"]`. Kafka hashes the key to determine which partition. This guarantees that all events from the same city zone go to the same partition, and thus are processed in order by the same Spark task. This matters for windowed aggregations — if airport events were spread across all partitions, you'd need to shuffle data to aggregate them.

#### KafkaProducer (kafka-python library)

```python
# simulator/kafka_producer.py
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),  # dict → bytes
    key_serializer=lambda k: k.encode("utf-8"),                # str → bytes
    acks="all",                    # wait for leader + all replicas to confirm
    retries=5,                     # retry 5 times on transient failures
    max_in_flight_requests_per_connection=1,  # preserve ordering per partition
)
producer.send(TOPIC_RIDES, key=event["city_zone"], value=event)
```

`acks="all"` means the producer waits until the Kafka leader AND all in-sync replicas have written the message. Slower but guaranteed durable. In a single-broker dev setup this is overkill, but it's the production setting.

#### KafkaConsumer (live_writer)

```python
# live_writer/run_live_writer.py
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    KAFKA_TOPIC_RIDES,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset="earliest",     # start from beginning if no committed offset
    enable_auto_commit=True,          # auto-commit offset every 5 seconds
    group_id="rideshare-live-dashboard-seeder",  # consumer group
    value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
)
for message in consumer:
    seeder.ingest_ride(message.value)  # message.value is already a dict
```

**Consumer groups** are critical: each group gets its own copy of every message. `rideshare-live-dashboard-seeder` and Spark's consumer group (`spark-bronze-writer`) both read the same `rides-events` topic independently. Neither blocks the other.

#### Kafka Docker Setup

We use Confluent's Kafka image (`confluentinc/cp-kafka:7.9.6`), not bitnami. Confluent is the company that created and maintains Kafka. Their image uses the standard `KAFKA_` env var prefix.

Key configs in `docker-compose.yml`:
```yaml
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092   # how other containers reach it
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"             # topics must be created explicitly
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1            # single broker → replication=1
```

**Why disable auto-topic creation?**
If a consumer tries to read a topic that doesn't exist yet and auto-creation is enabled, Kafka creates it with default settings (1 partition). We want 4 partitions for rides. Disabling this forces explicit topic creation via `live_writer/create_topics.py`, which runs as the `kafka_init` container on startup.

**Prerequisites to learn Kafka:**
- Basic networking: what is a host, port, IP
- JSON: Kafka values are JSON-serialized bytes
- Understand what a message queue is (RabbitMQ or SQS first if needed)

**How to verify you understand Kafka:**
- Can you explain what a partition is and why it enables parallelism?
- Can you explain why consumer groups allow independent consumers on the same topic?
- Can you explain what `offset` means and why it enables replay?

---

### 3.3 Apache Spark + PySpark

**What is it?**
Apache Spark is a distributed data processing engine. It can process data in parallel across a cluster of machines. PySpark is the Python API for Spark. In this project, Spark runs in a single Docker container (not a real cluster), but the same code would run across hundreds of machines in production.

Spark has two modes we use:
- **Batch**: Read a table, process it, write output. Finite data.
- **Structured Streaming**: Read a continuously updating source (Kafka), process micro-batches every 30 seconds, write output continuously. Infinite data.

**What we used from it in this project:**

#### SparkSession — the entry point

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

`spark.jars.packages` downloads JARs from Maven Central. The two JARs are:
1. `delta-spark` — lets Spark read/write Delta format
2. `spark-sql-kafka` — lets Spark read from Kafka as a streaming source

`getOrCreate()` returns an existing session if one already exists (important for DAGs that might call this twice).

#### Schema Definition with StructType

```python
# processing/streaming/kafka_to_bronze.py
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

RIDE_SCHEMA = StructType([
    StructField("ride_id",           StringType(),  True),
    StructField("surge_multiplier",  DoubleType(),  True),
    ...
])
```

Spark needs to know the schema of incoming JSON before it can parse it. Without this, it would scan all data to infer types (slow). Defining it explicitly is called "schema-on-read."

#### Reading from Kafka (Structured Streaming)

```python
# processing/streaming/kafka_to_bronze.py
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", "rides-events")
    .option("startingOffsets", "latest")    # start from newest messages
    .option("failOnDataLoss", "false")      # don't fail if Kafka deletes old offsets
    .load()
)
```

Kafka gives Spark a DataFrame with columns: `key` (bytes), `value` (bytes), `topic`, `partition`, `offset`, `timestamp`. The actual event JSON is in `value`.

```python
# Parse JSON from value bytes
parsed = (
    raw_stream
    .select(
        F.from_json(F.col("value").cast("string"), RIDE_SCHEMA).alias("data")
    )
    .select("data.*")  # expand the struct into flat columns
)
```

#### Writing to Delta (Streaming)

```python
query = (
    silver_df.writeStream
    .format("delta")
    .outputMode("append")                              # only write new rows
    .option("checkpointLocation", checkpoint_path)    # stores Kafka offsets + Delta state
    .option("path", output_path)
    .partitionBy("event_date")                        # organize files by date
    .start()
)
spark.streams.awaitAnyTermination()  # block until a query fails or is stopped
```

**Checkpoints** are the key to exactly-once processing. Spark writes the Kafka offset it last processed to the checkpoint directory. If Spark restarts, it reads the checkpoint and resumes from exactly where it stopped — no duplicate processing, no data loss.

#### Bronze → Silver Transform (batch function, reused by stream)

```python
# processing/batch/bronze_to_silver.py
def transform_bronze_to_silver(df):
    return (
        df
        .dropDuplicates(["ride_id"])          # deduplicate on primary key
        .dropna(subset=["ride_id", "city_zone"])  # drop rows with NULL critical fields
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
        .withColumn("event_hour", F.hour("event_ts"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("is_completed", F.col("status") == "completed")
        .withColumn("gross_fare_inr",
                    F.round(F.col("fare_base_inr") * F.col("surge_multiplier"), 2))
    )
```

This function is called from both the batch Bronze→Silver script AND the streaming job — same transform logic, same function. This is one of Spark's strengths: the DataFrame API is identical for batch and streaming.

#### Watermarks (late event handling)

```python
# bronze_to_silver_stream.py
bronze_df = spark.readStream.format("delta").load(input_path)
silver_df = transform_bronze_to_silver(bronze_df)
```

The simulator sets `event_delay_ms` on 5% of events (simulating mobile network delays). Without watermarks, Spark would accumulate state for potentially delayed events indefinitely. Watermarks tell Spark: "only wait `N` minutes for late events, then close the window." We rely on Delta's append-only model rather than explicit watermark windows for this streaming path.

#### Pandas UDF for ML Serving

```python
# ml/serve_predictions.py
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
import mlflow

def make_surge_predictor_udf():
    model = mlflow.pyfunc.load_model("models:/surge_predictor/Production")
    # Model is loaded ONCE per worker process, not once per row

    @F.pandas_udf(DoubleType())
    def predict_surge(features: pd.DataFrame) -> pd.Series:
        return pd.Series(model.predict(features).astype(float))

    return predict_surge
```

A Pandas UDF is a bridge between Spark and Python ML libraries. Spark splits a DataFrame into partitions, sends each partition to a Python worker as a pandas DataFrame, gets pandas Series back, and assembles the results. The model is loaded once per partition (not per row) — critical for performance.

**Prerequisites to learn PySpark:**
- Python fluency (at least 3 months of practice)
- Pandas: understand DataFrames, Series, groupby, merge
- SQL: SELECT, GROUP BY, JOIN, WHERE — PySpark's API mirrors SQL logic
- Basic understanding of what a cluster is (master/worker nodes)

**How to verify you understand PySpark:**
- Can you explain the difference between `withColumn` and `select`?
- Can you explain what a shuffle is and why `spark.sql.shuffle.partitions` matters?
- Can you explain why checkpoints are needed for streaming but not batch?

---

### 3.4 Delta Lake

**What is it?**
Delta Lake is an open-source storage format built on top of Parquet files. It adds ACID transactions, schema enforcement, time travel, and MERGE (upsert) capabilities to what is otherwise just a folder of `.parquet` files.

Think of it as: Parquet files + a transaction log (`_delta_log/`). Every write is recorded as a JSON entry in the log. If a write fails halfway through, you just see the previous version — the failed files are ignored. This is what "ACID" means in practice.

**What we used from it in this project:**

#### Medallion Architecture — Bronze / Silver / Gold

We organize all Delta tables into three "layers" (called medallion because they map to Bronze → Silver → Gold value):

```
data/delta/
├── bronze/
│   ├── rides/            Raw Kafka events, append-only
│   ├── drivers/          Raw driver events
│   └── payments/         Raw payment events
├── silver/
│   └── rides_clean/      Deduplicated, typed, enriched rides
└── gold/
    └── zone_demand/      Hourly KPIs per zone (aggregated)
```

**Bronze layer rules:**
- Never modify or delete. Append only.
- Keep every raw event exactly as received from Kafka, including bad records
- Add only metadata: `silver_ingest_ts` (when processed)
- This is your audit log — if a bug corrupts Silver, you can reprocess from Bronze

**Silver layer rules:**
- One deduplicated, clean record per `ride_id`
- All types are correct (strings cast to floats, timestamps parsed)
- All derived columns computed (event_hour, is_completed, gross_fare_inr)
- This is what analysts query and what ML trains on

**Gold layer rules:**
- Aggregated to a business grain: one row per (event_date, event_hour, city_zone)
- Ready for dashboard consumption — no joins or complex logic needed
- Overwrite on each hourly refresh (the full table is small enough)

#### `deltalake` Python Library (used in Airflow)

The `deltalake` library is a pure Python/Rust implementation of the Delta Lake reader/writer. No JVM, no Spark, no JAR downloads. This is what the Airflow DAGs use:

```python
# orchestration/dags/gold_refresh_dag.py
from deltalake import DeltaTable, write_deltalake

# Read Silver as pandas DataFrame
silver_df = DeltaTable("/data/delta/silver/rides_clean").to_pandas()

# Write Gold
write_deltalake("/data/delta/gold/zone_demand", gold_df, mode="overwrite")
```

`DeltaTable(path)` reads the transaction log to find the current set of valid Parquet files, then reads them. `to_pandas()` loads into memory as a pandas DataFrame.

#### Delta's transaction log

Every write creates a new file in `_delta_log/`:
```
_delta_log/
├── 00000000000000000000.json    Version 0: initial table creation
├── 00000000000000000001.json    Version 1: first data write
├── 00000000000000000002.json    Version 2: second write
...
```

Each log entry lists which Parquet files were added, which were removed (for overwrite/delete operations), and the schema. This is how time travel works: to see version 3, you replay log entries 0, 1, 2, 3.

#### Time Travel

```python
# Read table as it was at version 2
DeltaTable("/data/delta/silver/rides_clean").load_with_datetime("2026-01-01T09:00:00")

# Or using Spark (in the Spark container)
spark.read.format("delta").option("versionAsOf", 2).load("/data/delta/silver/rides_clean")
```

This is invaluable for debugging: if a bad transformation ran at 10am and corrupted Silver, you can read Silver from version before 10am and verify.

#### MERGE (upsert) — used conceptually in Silver

```python
# This is the DeltaTable.merge() pattern (delta-spark in the Spark container)
from delta.tables import DeltaTable

silver = DeltaTable.forPath(spark, "/data/delta/silver/rides_clean")
(
    silver.alias("target")
    .merge(new_df.alias("source"), "target.ride_id = source.ride_id")
    .whenMatchedUpdateAll()      # if ride_id already exists, update all columns
    .whenNotMatchedInsertAll()   # if ride_id is new, insert
    .execute()
)
```

This is idempotent: running the same Silver load twice produces the same result. No duplicates accumulate.

**Why Delta over plain Parquet?**
- Parquet has no ACID: if Spark crashes mid-write, you get a partially written table with no way to recover
- Parquet has no deduplication: rerunning a job creates duplicate files
- Parquet has no schema enforcement: a bad write with wrong column types corrupts the whole table
- Parquet has no time travel: you can't see what the data looked like yesterday

**Prerequisites to learn Delta Lake:**
- Parquet format: understand columnar storage basics
- SQL: MERGE, INSERT, UPDATE concepts
- PySpark basics: DataFrames, read/write

---

### 3.5 Apache Airflow

**What is it?**
Airflow is a workflow orchestration tool. You define "DAGs" (Directed Acyclic Graphs) — a set of tasks with dependencies between them — and Airflow runs them on a schedule, retries failures, and gives you a UI to monitor everything.

Think of it as a smarter cron job. Instead of `0 * * * * python run_gold.py`, you write a DAG that:
- Runs at the top of every hour
- Has 3 sequential tasks: refresh → validate → alert
- Retries each task 2 times on failure with a 5-minute wait
- Sends an email if the final retry also fails
- Shows you a timeline of every run and its status

**What we used from it in this project:**

#### DAG Definition

```python
# orchestration/dags/gold_refresh_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "retries": 2,                           # retry 2 times on failure
    "retry_delay": timedelta(minutes=5),    # wait 5 minutes between retries
    "execution_timeout": timedelta(minutes=45),  # kill task if it runs > 45 min
}

with DAG(
    dag_id="gold_refresh_dag",
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 * * * *",   # cron: top of every hour
    catchup=False,                   # don't run missed past intervals on startup
    tags=["gold", "kpi", "hourly"],
    default_args=default_args,
) as dag:
    start = EmptyOperator(task_id="start")       # no-op, just a visual start node
    task = PythonOperator(task_id="silver_to_gold", python_callable=my_function)
    end = EmptyOperator(task_id="end")

    start >> task >> end   # task dependency: run in sequence
```

`EmptyOperator` is a placeholder — it has no logic, just acts as a visual start/end sentinel in the DAG graph.

#### PythonOperator — how tasks actually run

```python
def _run_gold_refresh(**context):
    # context["ds"] is the execution date string, e.g. "2026-05-06"
    # context["task_instance"] lets you push/pull XCom values
    from deltalake import DeltaTable, write_deltalake
    silver_df = DeltaTable(SILVER_RIDES_TABLE).to_pandas()
    # ... compute gold_df ...
    write_deltalake(GOLD_ZONE_DEMAND_TABLE, gold_df, mode="overwrite")
```

The `**context` parameter receives Airflow's execution context — the date, run ID, task instance, etc.

**Important**: imports inside the function body (not at module level) are a pattern used in Airflow DAGs to avoid import errors when Airflow first loads the DAG file. Airflow imports all DAG files when it starts to build the DAG catalog — if a top-level import fails (like `from deltalake import DeltaTable` when deltalake isn't installed), the entire DAG disappears from the UI.

#### XCom — passing data between tasks

```python
# ml_retrain_dag.py — Task 1 pushes metrics
def _train_surge_model_task(**context):
    model, metrics = train_surge_model(df)
    context["task_instance"].xcom_push(key="model_metrics", value=metrics)

# Task 2 pulls what Task 1 pushed
def _promote_if_better(**context):
    metrics = context["task_instance"].xcom_pull(
        task_ids="train_surge_model",
        key="model_metrics"
    )
    mae = metrics.get("mae", float("inf"))
    if mae < 15.0:
        print("Model promoted to Production")
```

XCom (Cross-Communication) is Airflow's way of passing small values between tasks. It stores values in the Airflow database (SQLite in our setup). Don't use it for large DataFrames — only for small metadata like metrics, counts, flags.

#### Task Dependencies

```python
start >> [check_nulls, check_gold, check_ts, check_surge] >> end
```

This means:
- `start` runs first
- `check_nulls`, `check_gold`, `check_ts`, `check_surge` all run in **parallel** after `start`
- `end` runs after **all 4 checks** complete

The `>>` operator is syntactic sugar for `set_downstream()`.

#### Schedule Intervals (cron expressions)

| Expression | Meaning | Which DAG |
|---|---|---|
| `"0 * * * *"` | Top of every hour | gold_refresh_dag |
| `"0 2 * * *"` | 2am every day | ml_retrain_dag |
| `"*/30 * * * *"` | Every 30 minutes | data_quality_dag |
| `"*/15 * * * *"` | Every 15 minutes | dashboard_warmup_dag |

Cron format: `minute hour day_of_month month day_of_week`. `*` means "every". `*/30` means "every 30".

#### catchup=False — Why It Matters

If you set `start_date=datetime(2026, 1, 1)` and today is June 1, Airflow would by default try to run the DAG for every hour between January and June (thousands of runs). `catchup=False` tells Airflow: "only run going forward from now, not for missed past intervals."

#### SequentialExecutor

```yaml
# docker-compose.yml
AIRFLOW__CORE__EXECUTOR: SequentialExecutor
```

SequentialExecutor runs one task at a time in a single process. It's the simplest setup — no Celery, no Kubernetes. Sufficient for development and small workloads. In production, you'd use `LocalExecutor` (parallel, single machine) or `KubernetesExecutor` (parallel, autoscaling pods).

**Prerequisites to learn Airflow:**
- Python: understand functions, imports, decorators
- cron syntax: learn at crontab.guru
- Basic scheduling concepts: what is a scheduled job?

**How to verify you understand Airflow:**
- Can you explain what happens if a DAG task fails and `retries=2`?
- Can you explain the difference between `schedule_interval` and `execution_date`?
- Can you explain why `catchup=False` is usually correct for data pipelines?

---

### 3.6 Redis

**What is it?**
Redis is an in-memory key-value store. It's like a Python dictionary that lives outside your process, survives restarts (with persistence enabled), can be shared across multiple services, and has built-in expiry (TTL) on keys.

In this project, Redis serves as the "hot cache" layer. Instead of reading Delta tables (which takes seconds), the dashboard reads Redis keys (which takes milliseconds).

**What we used from it in this project:**

#### Connection

```python
# storage/redis_client.py
import redis

_client = None

def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            decode_responses=True,    # return str instead of bytes
        )
    return _client
```

`decode_responses=True` means Redis returns Python `str` instead of `bytes`. Without it, every value comes back as `b"..."` and you need to decode manually.

`_client = None` with a global variable is a singleton pattern — we create the connection once and reuse it. Creating a new connection for every Redis call would be wasteful (TCP connection setup overhead).

#### setex — set with expiry

```python
# TTL-based caching pattern
client.setex(
    "live:zone:airport:demand",   # key
    30,                            # TTL in seconds
    json.dumps({                   # value (must be a string)
        "ride_count": 42,
        "surge_multiplier": 1.8
    })
)
```

`setex` = SET + EXpire. The key automatically disappears after 30 seconds. This is how we prevent stale data: if the live_writer stops updating a zone key, the key expires and the dashboard falls back to a default. The TTL is your staleness budget.

#### get

```python
raw = client.get("dashboard:total_revenue")
if raw:
    data = json.loads(raw)   # stored as JSON string, parse back to dict
    total_revenue = data["amount"]
```

Returns `None` if the key doesn't exist (expired or never set). Always check for `None`.

#### keys() — pattern matching

```python
# gold_refresh_dag.py reads all zone demand keys
for key in client.keys("live:zone:*:demand"):
    payload = json.loads(client.get(key))
    zone = key.split(":")[2]   # "live:zone:airport:demand" → "airport"
```

`*` is a glob wildcard. `keys("live:zone:*:demand")` returns all keys matching that pattern. Note: `keys()` scans the entire keyspace and is slow on large datasets. In production, use `scan()` instead.

#### Key Naming Convention

```
live:zone:{zone_name}:demand       → real-time zone demand (30s TTL) — set by live_writer
live:rides_last_5min               → count of rides in last 5 minutes (60s TTL)
dashboard:total_revenue            → aggregated from Gold Delta (900s TTL) — set by Airflow
dashboard:avg_surge                → average surge across all Silver rides (900s TTL)
dashboard:zone:{zone}:count        → per-zone ride count (900s TTL)
dashboard:driver_utilisation       → completion rate (900s TTL)
```

The `live:` prefix = updated every event by live_writer (30-second TTL).
The `dashboard:` prefix = updated every 15 minutes by Airflow DAG (900-second / 15-minute TTL).

#### Redis Config in Docker

```yaml
# docker-compose.yml
command: redis-server --maxmemory 256mb --maxmemory-policy allkeys-lru
```

`maxmemory 256mb` — Redis will never use more than 256MB of RAM.
`allkeys-lru` — when the memory limit is hit, evict the Least Recently Used key. This makes Redis self-managing: we never need to manually delete old keys; they expire or get evicted automatically.

**Prerequisites to learn Redis:**
- Understand what a key-value store is (vs relational database)
- JSON: Redis values are stored as strings, often JSON-encoded
- Understand TTL / cache invalidation concepts

---

### 3.7 XGBoost + scikit-learn

**What is it?**
XGBoost (eXtreme Gradient Boosting) is a machine learning algorithm for supervised learning. It builds an ensemble of decision trees sequentially, where each tree corrects the errors of the previous ones (gradient boosting). It excels at tabular data (structured data in rows and columns).

scikit-learn provides the evaluation metrics (MAE, RMSE, R²) and the `train_test_split` utility.

**What we used from it in this project:**

#### The Problem: Surge Price Prediction

We want to predict what surge multiplier to charge for the next ride in a given zone, at a given hour, given current conditions. This is a **regression** problem (predicting a continuous number like 1.4 or 2.1).

#### Feature Engineering

```python
# ml/feature_engineering.py
CATEGORICAL_COLUMNS = ["city_zone", "vehicle_type", "weather", "status"]
NUMERIC_COLUMNS = ["distance_km", "fare_base_inr", "event_hour"]
TARGET_COLUMN = "surge_multiplier"

def build_features(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    # One-hot encode categoricals: "airport" → [1,0,0,0,0]
    features = pd.get_dummies(
        data[CATEGORICAL_COLUMNS + NUMERIC_COLUMNS],
        columns=CATEGORICAL_COLUMNS,
        drop_first=False
    )
    target = data[TARGET_COLUMN]
    return features, target
```

`pd.get_dummies` converts categorical strings into binary columns. `city_zone="airport"` becomes `city_zone_airport=1, city_zone_cbd=0, city_zone_mall=0, ...`. XGBoost can only work with numbers, not strings.

#### Model Training

```python
# ml/train_surge_model.py
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split

model = XGBRegressor(
    n_estimators=100,      # 100 trees in the ensemble
    max_depth=5,           # each tree can be at most 5 levels deep
    learning_rate=0.1,     # how much each tree contributes (shrinkage)
    subsample=0.8,         # use 80% of rows for each tree (reduces overfitting)
    colsample_bytree=0.8,  # use 80% of features for each tree
    random_state=42,       # reproducibility
)

x_train, x_test, y_train, y_test = train_test_split(features, target, test_size=0.2, random_state=42)
model.fit(x_train, y_train)
predictions = model.predict(x_test)
```

#### Evaluation Metrics

```python
# ml/evaluate_model.py
import math
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

def evaluate_regression(y_true, y_pred) -> dict:
    return {
        "mae":  float(mean_absolute_error(y_true, y_pred)),   # average absolute error
        "rmse": float(math.sqrt(mean_squared_error(y_true, y_pred))),  # root mean squared error
        "r2":   float(r2_score(y_true, y_pred)),              # 1.0 = perfect, 0.0 = baseline
    }
```

**MAE (Mean Absolute Error)**: On average, the prediction is off by this many units (surge multiplier units). MAE=0.15 means predictions are off by ±0.15 on a scale of 1.0–3.5. More interpretable.

**RMSE (Root Mean Squared Error)**: Like MAE but penalizes large errors more. A prediction of 2.5 when the truth is 1.0 hurts RMSE much more than MAE.

**R² (R-squared)**: Fraction of variance explained by the model. 1.0 = perfect. 0.0 = model is no better than always predicting the mean. Negative = model is worse than predicting the mean.

Note: In older scikit-learn, you could write `mean_squared_error(y, pred, squared=False)` to get RMSE directly. This `squared` parameter was removed in newer versions. We use `math.sqrt(mean_squared_error(...))` to be compatible.

#### Why XGBoost over neural networks?

**Tabular data advantage**: XGBoost consistently outperforms neural networks on structured/tabular data. Neural networks shine on unstructured data (images, text, audio).

**Speed**: XGBoost trains in seconds on 6,000 rows. A neural network equivalent would take minutes and require GPU for reasonable training time.

**Interpretability**: XGBoost gives you feature importances — you can tell which features matter most. Black-box neural networks can't do this easily.

**No scaling needed**: XGBoost is invariant to feature scale. Neural networks require normalizing all features to similar ranges.

**Prerequisites to learn XGBoost:**
- Python + Pandas
- Basic ML concepts: supervised learning, regression vs classification, train/test split, overfitting
- Understand what a decision tree is (XGBoost is an ensemble of trees)

---

### 3.8 MLflow

**What is it?**
MLflow is an open-source platform for managing the machine learning lifecycle: tracking experiments, storing models, and promoting models to production.

Without MLflow, you'd run a training script, print metrics to the terminal, save a `.pkl` file, and have no record of what hyperparameters produced that model. A week later you have 10 `.pkl` files and no idea which one is best.

**What we used from it in this project:**

#### Experiment Tracking

```python
# ml/train_surge_model.py
import mlflow

mlflow.set_tracking_uri("http://mlflow:5000")   # where to store runs
mlflow.set_experiment("surge_price_prediction") # group runs under this experiment

with mlflow.start_run():
    model.fit(x_train, y_train)
    predictions = model.predict(x_test)
    metrics = evaluate_regression(y_test, predictions)

    mlflow.log_metrics(metrics)          # log MAE, RMSE, R²
    mlflow.xgboost.log_model(            # save the trained model artifact
        model,
        artifact_path="model"
    )
```

Every `start_run()` block creates a new entry in the MLflow tracking database with:
- All logged metrics (`mlflow.log_metrics`)
- All logged parameters (`mlflow.log_params`)
- The model artifact (serialized model files)
- Automatic metadata: start time, duration, who ran it, git commit

#### MLflow Server

```yaml
# docker-compose.yml
mlflow:
  image: python:3.11-slim
  command: >
    sh -c "pip install mlflow==2.11.3 &&
           mlflow server --host 0.0.0.0 --port 5000
           --backend-store-uri /mlflow/tracking
           --default-artifact-root /mlflow/artifacts"
```

`--backend-store-uri`: where to store run metadata (SQLite file or PostgreSQL URL)
`--default-artifact-root`: where to store model artifacts (local path or S3 URI)

The MLflow UI at http://localhost:5000 lets you compare runs side by side, filter by metric, and register models.

#### Model Registry and Promotion Gate

```python
# ml/ml_retrain_dag.py
def _promote_if_better(**context):
    metrics = context["task_instance"].xcom_pull(
        task_ids="train_surge_model", key="model_metrics"
    )
    mae = metrics.get("mae", float("inf"))
    threshold = 15.0

    if mae < threshold:
        print(f"✓ Model promoted — MAE={mae:.4f} < threshold={threshold}")
    else:
        print(f"✗ Model NOT promoted — MAE={mae:.4f} >= threshold={threshold}")
```

The promotion gate prevents bad models from reaching production. If the daily retrain produces a model with MAE > 15 (worse than the threshold), it stays in Staging and the Production model continues serving. This is an automated quality gate.

**Why use `mlflow.xgboost.log_model` instead of `mlflow.sklearn.log_model`?**
The model is an XGBRegressor (from the `xgboost` library), not a scikit-learn estimator. `mlflow.sklearn.log_model` uses the sklearn serialization protocol (pickle). `mlflow.xgboost.log_model` uses XGBoost's native serialization, which is more efficient, produces smaller files, and loads faster. Using the wrong one caused a bug in the original code.

**Prerequisites to learn MLflow:**
- Python ML basics: how to train and save a model
- Understand experiment vs run vs metric vs artifact
- Basic CLI: running `mlflow server`

---

### 3.9 Streamlit

**What is it?**
Streamlit is a Python library for building data apps and dashboards. You write a Python script, and Streamlit turns it into a web app — no HTML, CSS, or JavaScript knowledge required.

**What we used from it in this project:**

#### App Layout

```python
# dashboard/app.py
import streamlit as st
import streamlit.components.v1 as components

st.set_page_config(page_title="Rideshare Analytics", layout="wide")

# Auto-refresh via JavaScript injection — no extra package needed
components.html(
    "<script>setTimeout(function(){window.location.reload()},30000);</script>",
    height=0
)

st.title("Real-Time Ride-Sharing Analytics Platform")

col1, col2, col3, col4 = st.columns(4)
with col1:
    render_live_counter()

tab1, tab2, tab3 = st.tabs(["Demand Heatmap", "Revenue", "Drivers"])
with tab1:
    render_heatmap()
```

`st.columns(4)` creates a 4-column layout. `st.tabs([...])` creates a tabbed interface. `st.expander(...)` creates a collapsible section.

#### Auto-Refresh

The original design used `streamlit-autorefresh`, a third-party package that was unavailable in the container environment. We replaced it with a JavaScript snippet injected via `components.html()`. The JavaScript calls `window.location.reload()` after 30 seconds — exactly the same effect, zero dependency.

This is an example of choosing the simplest solution when a dependency causes problems.

#### Components We Built

```
live_counter.py       → st.metric("Rides/min", value=42)
demand_heatmap.py     → st.pydeck_chart(...) with PyDeck HeatmapLayer
revenue_charts.py     → st.bar_chart(df) and st.line_chart(df)
driver_utilisation.py → st.dataframe(df) and st.metric()
pipeline_health.py    → st.success() / st.warning() / st.error() status indicators
```

#### Data Connectors

```python
# dashboard/data_connectors/redis_reader.py
def get_total_revenue() -> float:
    raw = get_client().get("dashboard:total_revenue")
    return json.loads(raw)["amount"] if raw else 0.0

# dashboard/data_connectors/delta_reader.py
def read_gold_zone_demand() -> pd.DataFrame:
    return DeltaTable(GOLD_ZONE_DEMAND_TABLE).to_pandas()
```

The dashboard reads Redis for live metrics (fast) and Delta for historical charts (slower but more data).

**Prerequisites to learn Streamlit:**
- Python: functions, imports, basic data structures
- Pandas: DataFrames (most Streamlit charts accept DataFrames)
- Basic understanding of web apps (optional but helpful)

---

### 3.10 Docker + Docker Compose

**What is it?**
Docker packages an application and all its dependencies into a "container" — a lightweight, isolated process that runs identically on any machine. Docker Compose defines multiple containers as a YAML file and manages them together.

Without Docker, setting up this project would require installing Java 17, Python 3.11, Kafka, ZooKeeper, Spark, Redis, Airflow, and MLflow on your machine — a process that takes hours and differs on every OS. With Docker, it's `docker compose up`.

**What we used from it in this project:**

#### Networks

```yaml
networks:
  rideshare-net:
    driver: bridge
```

All containers join `rideshare-net`. Within this network, containers can refer to each other by service name: Airflow connects to Redis at `redis:6379`, Spark connects to Kafka at `kafka:9092`. Without a shared network, containers can't communicate.

#### Named Volumes vs Bind Mounts (Critical Distinction)

```yaml
volumes:
  kafka_data:        # Named volume: managed by Docker, lives in /var/lib/docker/volumes/
  redis_data:        # Good for Kafka/Redis — we don't need to browse these files

services:
  airflow:
    volumes:
      - ./data/delta:/data/delta    # Bind mount: ./data/delta on your host → /data/delta in container
      - ./orchestration/dags:/opt/airflow/dags  # Bind mount: edit DAGs locally, container sees changes immediately
```

**Named volumes** (`kafka_data:`) are managed by Docker. They live in `/var/lib/docker/volumes/` on your host. Good for Kafka and Redis because you don't need to browse their internal files.

**Bind mounts** (`./data/delta:/data/delta`) mount a directory from your host filesystem directly into the container. Any file written to `/data/delta` inside the container appears at `./data/delta` on your host. This is how multiple containers share the same Delta tables.

**The bug this caused:** Originally, Delta was a named volume `delta_data` mounted at `/data/delta` in Airflow and Dashboard, but Spark wrote to `/app/data/delta` (because of a separate bind mount `./:/app`). Named volume and bind mount are different filesystems — Spark wrote to one, Airflow read from the other (empty). Fix: use a bind mount `./data/delta:/data/delta` in all three containers.

#### CMD vs ENTRYPOINT

```dockerfile
# Dockerfile.simulator
CMD ["python", "-m", "simulator.run_simulator"]
```

```yaml
# docker-compose.yml
simulator:
  command: python -m simulator.run_simulator  # overrides CMD completely
```

**Rule**: Docker Compose's `command:` **replaces** `CMD` but **appends** to `ENTRYPOINT`.

If you use `ENTRYPOINT ["python", "-m", "simulator.run_simulator"]` and Compose also has `command: python -m simulator.run_simulator`, Docker runs the container with:
```
python -m simulator.run_simulator python -m simulator.run_simulator
```
— the whole command repeated twice, which crashes. Use `CMD` in Dockerfiles so Compose can cleanly override.

#### Healthchecks

```yaml
# docker-compose.yml
zookeeper:
  healthcheck:
    test: ["CMD", "bash", "-lc", "echo srvr | nc localhost 2181 | grep 'Mode:'"]
    interval: 10s
    timeout: 5s
    retries: 5
```

Healthchecks let Docker know when a service is actually ready (not just running). `depends_on: condition: service_started` checks if the container started, but `condition: service_healthy` waits until the healthcheck passes. This prevents Kafka from starting before ZooKeeper is ready.

#### kafka_init — One-Shot Container

```yaml
kafka_init:
  entrypoint: ["python", "-m", "live_writer.create_topics"]
```

This container runs once, creates the Kafka topics, then exits with code 0. It's not a long-running service. This separates topic creation (done once at startup) from event production (live_writer).

**Prerequisites to learn Docker:**
- Linux command line basics
- What is a process? What is a filesystem?
- Basic networking: IP, port, hostname

**How to verify you understand Docker:**
- Can you explain the difference between an image and a container?
- Can you explain what a bind mount is and why it's different from a named volume?
- Can you explain what happens when you run `docker compose up` vs `docker compose up --build`?

---

## 4. Architecture Decisions — Why We Did Things This Way

### Decision 1: Why Kafka instead of writing directly to a database?

**The naive approach:** Simulator writes directly to PostgreSQL. Spark reads from PostgreSQL.

**The problem:** PostgreSQL write latency is ~5ms per record. At 8 events/second, that's fine. But what if Spark goes down for maintenance? Events would pile up and PostgreSQL would become a bottleneck. Also, Spark would need to poll PostgreSQL for new rows — there's no "push" notification.

**Kafka's solution:** Events are buffered in Kafka. If Spark is down for 2 hours, Kafka retains all events (7-day retention). When Spark restarts, it reads from the committed offset and catches up. Spark doesn't poll — Kafka pushes new batches every 30 seconds via the streaming API. This decoupling means Spark and the simulator are completely independent — either can restart without affecting the other.

### Decision 2: Why Medallion (Bronze/Silver/Gold) instead of writing clean data directly?

**The naive approach:** Simulator writes directly to a "clean" database. Only good data is stored.

**The problem:** What if your cleaning logic has a bug? What if next month you decide that `status="cancelled_by_driver"` should count as "completed" for billing purposes? If you threw away the raw data, you can't recompute. Also, debugging is impossible — you can't tell if a bad value came from the source or from your transform.

**Medallion's solution:**
- Bronze keeps everything, exactly as received. Your audit log. Append-only, never modified.
- Silver is the cleaned version. If cleaning logic is wrong, reprocess Bronze → Silver.
- Gold is the aggregated business layer. If KPI logic changes, reprocess Silver → Gold.

Each layer gives you a recovery point. You only need to reprocess from the layer where the bug was introduced.

### Decision 3: Why `deltalake` Python library in Airflow instead of PySpark?

**The original approach:** Airflow DAGs used PySpark to read Delta tables, same as the Spark container.

**The problem discovered in production:** PySpark inside Airflow tried to download Delta JARs from Maven Central at runtime via `spark.jars.packages`. Inside a Docker container without reliable internet, this failed silently — the SparkSession started without Delta support. The `try/except` blocks caught the failure and fell back to Redis estimates. The DAGs showed green in the UI while computing wrong data.

**The fix:** Replace PySpark in Airflow with `deltalake` — a pure Rust implementation that reads Delta tables natively. No JVM, no Maven, no JAR downloads. `DeltaTable(path).to_pandas()` gives you a pandas DataFrame in under a second. Airflow does orchestration logic; Spark does the heavy distributed processing. Clear separation of concerns.

**The principle:** Don't use a distributed engine (Spark) for orchestration tasks that run on a single machine. Airflow tasks run one at a time in SequentialExecutor — there's no parallelism benefit from Spark here.

### Decision 4: Why Redis as a cache layer instead of reading Delta directly from the dashboard?

**The naive approach:** Dashboard reads Gold Delta tables on every page load.

**The problem:** Reading a Delta table in Streamlit means: importing deltalake → reading `_delta_log` → finding valid Parquet files → reading Parquet → building pandas DataFrame. This takes 2–8 seconds per read. Streamlit reruns the whole script on every user interaction, so every metric click triggers a 2–8s wait.

**Redis's solution:** Airflow precomputes the answers every 15 minutes and stores them in Redis at millisecond speed. Dashboard reads Redis: ~1ms. Users experience instant response. The tradeoff: metrics are at most 15 minutes stale (acceptable for a business dashboard).

### Decision 5: Why bind mount `./data/delta` instead of Docker named volume?

Explained in Section 3.10 above. The key insight: **named volumes are isolated to Docker's storage driver**, invisible on your host filesystem, and cannot be shared between containers via a simple path. Bind mounts are just directories on your host mounted into containers — transparent, shareable, and browsable.

### Decision 6: Why synthetic data instead of a real dataset?

A real dataset (e.g., the NYC Taxi dataset) is static — 10 million rows that never change. You can't stream it realistically. You'd have to replay it in a loop, and the timestamps would be from 2020.

A synthetic simulator gives:
- **Real-time streaming**: events actually arrive at 8/second
- **Controllable patterns**: peak hours, zone demand, weather effects, late events — all configurable
- **Schema control**: you can add new fields without finding a new dataset
- **Edge case simulation**: GPS noise, cancellations, schema versions, late arrivals

The tradeoff: simulated data is less messy than real data. But for demonstrating pipeline architecture, it's superior.

### Decision 7: Why XGBoost over linear regression or a neural network?

**Linear regression** would be too simple — surge is non-linear. A zone with 50 demand and 5 drivers should have very different surge than a zone with 5 demand and 50 drivers. Linear models can't capture this interaction.

**Neural networks** would overengineer the problem — we have 6,000 training rows and ~20 features after one-hot encoding. Neural networks need large datasets. XGBoost consistently outperforms on small tabular datasets. Neural networks also require more preprocessing (normalization, careful learning rate tuning) and take much longer to train.

**XGBoost** trains in under 5 seconds on 6,000 rows, produces interpretable feature importances, handles missing values natively, and has battle-tested performance on tabular data. It's the default choice for structured data ML in industry (and wins most Kaggle competitions on tabular data).

---

## 5. Data Schemas — What Every Field Means

### Ride Event (Kafka → Bronze)

```json
{
  "ride_id":           "UUID — unique identifier for this ride",
  "event_timestamp":   "ISO-8601 UTC — when the event occurred",
  "driver_id":         "UUID — which driver",
  "user_id":           "UUID — which passenger",
  "status":            "requested | accepted | started | completed | cancelled",
  "pickup_lat":        "float — latitude of pickup (Bangalore area: 12.9–13.2)",
  "pickup_lon":        "float — longitude of pickup (77.5–77.7)",
  "dropoff_lat":       "float — destination latitude",
  "dropoff_lon":       "float — destination longitude",
  "city_zone":         "airport | railway_station | cbd | mall | residential",
  "distance_km":       "float — estimated trip distance",
  "vehicle_type":      "bike | auto | cab_economy | cab_premium",
  "fare_base_inr":     "float — base fare before surge (distance × rate_per_km)",
  "surge_multiplier":  "float — surge pricing factor: [1.0, 3.5]",
  "weather":           "clear | cloudy | rain",
  "event_delay_ms":    "int — simulated network delay (0 for 95% of events)",
  "schema_version":    "1.0 — for future schema evolution tracking"
}
```

### Silver Additions (computed in bronze_to_silver)

```
event_ts         → parsed datetime from event_timestamp (timezone-aware)
event_date       → date extracted from event_ts (for partitioning)
event_hour       → 0–23 (for hourly aggregations and ML features)
is_completed     → bool: status == "completed"
gross_fare_inr   → fare_base_inr × surge_multiplier (actual revenue)
silver_ingest_ts → when Spark processed this event
```

### Gold Schema (zone_demand table)

```
event_date           → date: e.g. 2026-05-06
event_hour           → int: 0–23
city_zone            → str: airport | railway_station | cbd | mall | residential
ride_count           → int: total rides in this hour/zone
completed_rides      → int: rides with status=completed
cancelled_rides      → int: rides with status=cancelled
gross_revenue_inr    → float: sum of gross_fare_inr
avg_surge_multiplier → float: mean surge for this hour/zone
```

**Grain**: One row per (event_date, event_hour, city_zone). If it's 2pm in the airport zone, there's exactly one Gold row for it.

### ML Features (what XGBoost is trained on)

```
city_zone_*     → one-hot encoded: city_zone_airport, city_zone_cbd, etc.
vehicle_type_*  → one-hot encoded: vehicle_type_bike, vehicle_type_auto, etc.
weather_*       → one-hot encoded: weather_clear, weather_cloudy, weather_rain
status_*        → one-hot encoded: status_completed, etc.
distance_km     → numeric
fare_base_inr   → numeric
event_hour      → 0–23 numeric
```

**Target**: `surge_multiplier` (what we're predicting).

---

## 6. Interview Preparation — Q&A With Full Reasoning

### Q: Walk me through your pipeline from an event occurring to it appearing on the dashboard.

**A:** A ride event is generated by the simulator with fields like `ride_id`, GPS coordinates, `surge_multiplier`, and `city_zone`. The simulator publishes it to the `rides-events` Kafka topic, using `city_zone` as the partition key so all events from the same zone go to the same partition.

Two consumers read this simultaneously. First, `live_writer` — a pure Python Kafka consumer — reads the event immediately and updates Redis: the rides-in-5-minutes counter and zone-level demand metrics with a 30-second TTL. The dashboard reads these Redis keys in under 1ms, so the live KPI counter updates within seconds.

Second, PySpark Structured Streaming reads the same Kafka topic every 30 seconds. It parses the JSON, deduplicates on `ride_id` (in case of Kafka redelivery), adds derived columns like `gross_fare_inr`, and appends to the Silver Delta table partitioned by `event_date`. This Silver data is used by Airflow hourly to compute Gold KPIs.

The Airflow `dashboard_warmup_dag` runs every 15 minutes — it reads Silver and Gold via the `deltalake` Python library, computes total revenue, avg surge, and per-zone counts, and caches them in Redis with a 15-minute TTL. The dashboard reads these cached values for the historical charts.

### Q: How do you handle duplicate events?

**A:** We handle duplicates at the Silver layer using `dropDuplicates(["ride_id"])`. The Bronze layer is append-only — we never deduplicate there, to preserve the audit log. The Silver layer runs `dropDuplicates(["ride_id"])` before writing, which keeps only the first occurrence of each unique `ride_id`. Since Spark checkpoints track Kafka offsets, each event is processed exactly once by Spark — duplicates would only come from the source publishing the same `ride_id` twice, which we prevent by using `uuid.uuid4()`.

If we used MERGE INTO (instead of append + dropDuplicates), we'd handle duplicates idempotently: re-running a Silver load for the same date would update existing rows rather than duplicating them.

### Q: What is a watermark and when is it needed?

**A:** A watermark is a threshold that tells Spark how long to wait for late-arriving events before closing a time window. In our pipeline, the simulator marks 5% of events with `event_delay_ms` up to 120 seconds — simulating mobile network delays where the event is generated at time T but arrives at Kafka at T+2 minutes.

Without a watermark, if you do a windowed aggregation (e.g., "count rides per zone per 5-minute window"), Spark would wait indefinitely for late events — the window never closes and state grows unboundedly. With `.withWatermark("event_time", "2 minutes")`, Spark waits up to 2 minutes for late events then closes the window. Events arriving more than 2 minutes late are dropped.

In our current implementation, we don't use explicit windowed aggregations in the streaming path — we use append mode and let Airflow compute hourly KPIs on the completed Silver table. Watermarks would be essential if we were computing rolling-window metrics directly in the stream.

### Q: Why did you separate Airflow and Spark? Why not have Airflow call Spark jobs?

**A:** We initially did — Airflow DAGs used PySpark with `spark.jars.packages` to download Delta JARs. This failed silently in the Docker environment when Maven was unreachable: the SparkSession started successfully (no error) but couldn't read Delta format. DAGs ran green while computing from Redis estimates rather than real data.

The architectural insight: Airflow is an orchestrator, not a processing engine. Its job is to schedule tasks, retry failures, and pass results between steps — not to run Spark jobs. For the hourly KPI computation, `deltalake` + pandas on a single machine is entirely sufficient: 6,000 Silver rows take under a second. Spark's power is for data that exceeds a single machine's memory — we don't need it in Airflow.

### Q: How would you scale this to 100× traffic (800 events/second, 100M rides/day)?

**A:**
1. **Kafka**: Increase partitions from 4 to 32 for `rides-events`. Add broker nodes (currently 1, scale to 3+ for replication factor 3).
2. **Spark**: Move from single container to a real Spark cluster (YARN or Kubernetes). Add 5+ worker nodes. Reduce trigger interval from 30s to 5s.
3. **Delta Lake**: Move from local disk to S3 or GCS. Delta on cloud storage scales infinitely — you just pay for storage.
4. **Airflow**: Switch from `SequentialExecutor` to `KubernetesExecutor` so DAG tasks run in parallel pods.
5. **Redis**: Upgrade from single-instance to Redis Cluster (horizontal sharding). Increase `maxmemory` limit.
6. **Gold computation**: At 100M rides/day, the Gold aggregation in pandas would be too slow. Switch back to PySpark for the Gold refresh (or use DuckDB, which handles ~1B rows on a single machine).
7. **Dashboard**: Add connection pooling, separate read replicas for Redis, CDN for static assets.

### Q: Why did you use `deltalake` in Airflow but `delta-spark` in Spark?

**A:** They are two different implementations of the same Delta Lake format.

`delta-spark` is the JVM-based implementation bundled with Apache Spark. It provides the full Spark integration: MERGE INTO in Spark SQL, Z-ORDER optimization, streaming write support. Required for the Spark streaming pipeline.

`deltalake` is a pure Rust/Python library from the Linux Foundation. No JVM, no Spark, no Maven dependencies. Reads Delta tables directly from Parquet files + transaction log. Suitable for Airflow tasks that only need to read a table into pandas — they don't need Spark's distributed execution.

The choice is pragmatic: in Airflow, installing PySpark + Delta-Spark would add 1GB+ to the image size, require JVM in the container, and introduce Maven dependency issues. `deltalake` is 50MB, pure Python/Rust, and works everywhere.

### Q: How do you ensure the pipeline is idempotent?

**A:** Multiple levels:

**Kafka**: Spark checkpoints store committed Kafka offsets. Restarting Spark resumes from the last committed offset — no reprocessing.

**Silver**: `dropDuplicates(["ride_id"])` ensures no duplicate rides in Silver even if the same event arrives from Kafka twice.

**Gold**: `write_deltalake(mode="overwrite")` replaces the Gold table on each hourly refresh. Re-running the hourly DAG produces the same result — no accumulated duplicates.

**Airflow**: `catchup=False` prevents Airflow from running missed intervals on restart. Retries use the same `execution_date` context, so a retried task computes the same result as the original.

**Redis**: `setex` with TTL means old values expire automatically. If a task runs twice, the second write overwrites the first with the same value.

### Q: What would you do differently in a production system?

**A:**
1. **Schema Registry**: Use Confluent Schema Registry to enforce Avro/Protobuf schemas on Kafka topics. Currently, the simulator can publish any JSON shape — a bad field type would silently corrupt Silver.
2. **Data Contracts**: Define contracts at each layer boundary — the simulator promises to produce a specific schema, Silver promises specific column types. Use Great Expectations or Soda for automated checks.
3. **Alerting**: Connect the `data_quality_dag` failures to PagerDuty or Slack, not just print statements.
4. **Cloud storage**: Replace local `./data/delta` with S3 for durability and horizontal scale.
5. **Airflow metadata DB**: Replace SQLite with PostgreSQL for production Airflow.
6. **Kubernetes**: Replace Docker Compose with Kubernetes Helm charts for HA and autoscaling.
7. **Multi-partition tables**: Partition Silver by `event_date` AND `city_zone` for query pruning.
8. **Streaming ML serving**: Instead of daily batch retraining, use a real-time feature store (Feast) and online serving (MLflow's model server or Triton).

---

## 7. Learning Roadmap — What to Study First

This is the recommended sequence for a beginner. Each item builds on the previous.

### Month 1 — Foundations

**Week 1–2: Python Proficiency**
- Fluent in: loops, functions, classes, decorators, list comprehensions
- Standard library: `json`, `os`, `datetime`, `collections`, `threading`
- Tools: `pip`, `venv`, `pytest`
- Project: Build a CLI tool that reads a CSV and computes statistics

**Week 3–4: Pandas + SQL**
- Pandas: DataFrame, Series, `groupby`, `merge`, `apply`, `pivot_table`
- SQL: SELECT, WHERE, GROUP BY, JOIN, subqueries, window functions
- Practice: Analyze a public dataset (NYC Taxi, Airbnb listings) with both SQL and Pandas
- Project: Replicate a SQL aggregation in Pandas and verify identical results

### Month 2 — Data Storage

**Week 5–6: Relational Databases**
- PostgreSQL: tables, indexes, ACID, transactions
- Write Python code that reads/writes PostgreSQL with `psycopg2` or `SQLAlchemy`
- Understand: what is a primary key, foreign key, index?

**Week 7–8: Parquet + Delta Lake**
- Learn columnar storage: why Parquet is faster than CSV for analytics
- Install `deltalake` and read/write Delta tables from Python
- Understand: transaction log, versioning, `mode="overwrite"` vs `mode="append"`
- Practice: Replicate the Silver → Gold aggregation from this project in pandas + deltalake

### Month 3 — Streaming

**Week 9–10: Kafka**
- Run Kafka locally in Docker
- Write a Python producer that sends 1 message/second
- Write a Python consumer that prints each message
- Learn: topics, partitions, offsets, consumer groups
- Practice: Add a second consumer group that reads the same topic independently

**Week 11–12: PySpark**
- Start with batch: read a Parquet file, apply transformations, write output
- Then streaming: read from Kafka, write to Delta
- Understand: DataFrames, schemas, `groupBy`, `agg`, `writeStream`, checkpoints
- Practice: Replicate `kafka_to_bronze.py` and `bronze_to_silver_stream.py` from this project

### Month 4 — Orchestration + ML

**Week 13–14: Apache Airflow**
- Install Airflow locally (via pip or Docker)
- Write a simple DAG with 3 tasks in sequence
- Then write a DAG that runs in parallel
- Understand: DAGs, operators, task dependencies, XCom, scheduler
- Practice: Write the `gold_refresh_dag` from this project from memory

**Week 15–16: ML with XGBoost + MLflow**
- Train a regression model on any public dataset
- Track experiments with MLflow (log params, metrics, model artifact)
- Practice: Replicate `train_surge_model.py` and `evaluate_model.py` from this project
- Learn: what is overfitting? What is train/test split? Why time-based split?

### Month 5 — Infrastructure

**Week 17–18: Docker + Docker Compose**
- Build a Docker image for a simple Python app
- Write a `docker-compose.yml` with 3 services
- Understand: Dockerfile, CMD vs ENTRYPOINT, bind mounts vs named volumes, networking
- Practice: Containerize the Streamlit dashboard from this project

**Week 19–20: Redis + Caching**
- Run Redis locally: `docker run -d redis:7.2`
- Use `redis-py` to set/get keys, use TTLs
- Understand: eviction policies, when to use a cache, cache invalidation
- Practice: Replicate the live_writer and dashboard_warmup_dag from this project

### Month 6 — Portfolio Project

Rebuild this entire project from the design document without looking at the code. This is the real test. If you can build it, you understand every component.

Then extend it:
- Add a fourth Kafka topic: `cancellation-events`
- Add a new Gold table: `driver_earnings_daily`
- Add a new Airflow DAG that sends a daily revenue report email
- Replace the XGBoost model with LightGBM and compare in MLflow

---

*This guide was written alongside building the project, documenting every decision, bug, and insight. The bugs we hit (Docker named volumes, Spark Maven downloads, Dockerfile ENTRYPOINT confusion) are the most valuable learning — they're the exact same bugs you'll hit in production systems.*
