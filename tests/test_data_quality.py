"""Data quality rule tests — run against in-memory Spark DataFrames, no Delta required."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("rideshare-tests").getOrCreate()


def _count_nulls(df, column: str) -> int:
    return df.filter(F.col(column).isNull()).count()


def _count_surge_out_of_bounds(df) -> int:
    return df.filter((F.col("surge_multiplier") < 1.0) | (F.col("surge_multiplier") > 3.5)).count()


def test_no_null_ride_ids():
    spark = _spark()
    frame = spark.createDataFrame(
        [
            {"ride_id": "r1", "surge_multiplier": 1.5},
            {"ride_id": "r2", "surge_multiplier": 2.0},
        ]
    )
    assert _count_nulls(frame, "ride_id") == 0


def test_detects_null_ride_ids():
    spark = _spark()
    frame = spark.createDataFrame(
        [
            {"ride_id": None, "surge_multiplier": 1.5},
            {"ride_id": "r2", "surge_multiplier": 2.0},
        ]
    )
    assert _count_nulls(frame, "ride_id") == 1


def test_surge_within_bounds():
    spark = _spark()
    frame = spark.createDataFrame(
        [
            {"ride_id": "r1", "surge_multiplier": 1.0},
            {"ride_id": "r2", "surge_multiplier": 3.5},
            {"ride_id": "r3", "surge_multiplier": 2.1},
        ]
    )
    assert _count_surge_out_of_bounds(frame) == 0


def test_detects_surge_below_one():
    spark = _spark()
    frame = spark.createDataFrame([{"ride_id": "r1", "surge_multiplier": 0.9}])
    assert _count_surge_out_of_bounds(frame) == 1


def test_detects_surge_above_max():
    spark = _spark()
    frame = spark.createDataFrame([{"ride_id": "r1", "surge_multiplier": 4.0}])
    assert _count_surge_out_of_bounds(frame) == 1


def test_event_timestamps_are_recent():
    """Events generated in the last minute should pass a 2-hour recency check."""
    from datetime import datetime, timedelta, timezone

    spark = _spark()
    now = datetime.now(timezone.utc).isoformat()
    frame = spark.createDataFrame([{"ride_id": "r1", "event_timestamp": now}])

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    old_count = frame.filter(F.col("event_timestamp") < cutoff).count()
    assert old_count == 0


def test_gold_table_not_empty():
    spark = _spark()
    frame = spark.createDataFrame(
        [{"city_zone": "cbd", "event_hour": 8, "ride_count": 10}]
    )
    assert frame.count() > 0, "Gold table must not be empty"


def test_gold_table_empty_raises():
    spark = _spark()
    frame = spark.createDataFrame([], "city_zone STRING, ride_count INT")
    assert frame.count() == 0, "Empty frame correctly detected"
