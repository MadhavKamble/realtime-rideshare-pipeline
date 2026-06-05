from pyspark.sql import SparkSession

from processing.batch.bronze_to_silver import transform_bronze_to_silver


def _spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("rideshare-tests").getOrCreate()


def test_silver_drops_duplicates():
    spark = _spark()
    frame = spark.createDataFrame(
        [
            {"ride_id": "r1", "event_timestamp": "2024-01-01T08:00:00Z", "status": "completed", "fare_base_inr": 100.0, "surge_multiplier": 1.5, "city_zone": "cbd"},
            {"ride_id": "r1", "event_timestamp": "2024-01-01T08:00:00Z", "status": "completed", "fare_base_inr": 100.0, "surge_multiplier": 1.5, "city_zone": "cbd"},
        ]
    )
    result = transform_bronze_to_silver(frame)
    assert result.count() == 1, "Duplicate ride_id should be removed"


def test_silver_computes_gross_fare():
    spark = _spark()
    frame = spark.createDataFrame(
        [{"ride_id": "r2", "event_timestamp": "2024-01-01T09:00:00Z", "status": "completed", "fare_base_inr": 200.0, "surge_multiplier": 2.0, "city_zone": "airport"}]
    )
    result = transform_bronze_to_silver(frame)
    row = result.collect()[0]
    assert abs(row["gross_fare_inr"] - 400.0) < 0.01, f"Expected 400.0, got {row['gross_fare_inr']}"


def test_silver_adds_event_hour():
    spark = _spark()
    frame = spark.createDataFrame(
        [{"ride_id": "r3", "event_timestamp": "2024-06-15T14:30:00Z", "status": "completed", "fare_base_inr": 150.0, "surge_multiplier": 1.2, "city_zone": "suburbs"}]
    )
    result = transform_bronze_to_silver(frame)
    row = result.collect()[0]
    assert row["event_hour"] == 14


def test_silver_is_completed_flag():
    spark = _spark()
    frame = spark.createDataFrame(
        [
            {"ride_id": "r4", "event_timestamp": "2024-01-01T10:00:00Z", "status": "completed", "fare_base_inr": 80.0, "surge_multiplier": 1.0, "city_zone": "cbd"},
            {"ride_id": "r5", "event_timestamp": "2024-01-01T10:01:00Z", "status": "cancelled", "fare_base_inr": 80.0, "surge_multiplier": 1.0, "city_zone": "cbd"},
        ]
    )
    result = transform_bronze_to_silver(frame)
    rows = {row["ride_id"]: row["is_completed"] for row in result.collect()}
    assert rows["r4"] is True
    assert rows["r5"] is False


def test_silver_adds_event_date():
    spark = _spark()
    frame = spark.createDataFrame(
        [{"ride_id": "r6", "event_timestamp": "2024-03-10T22:00:00Z", "status": "completed", "fare_base_inr": 300.0, "surge_multiplier": 1.8, "city_zone": "tech_park"}]
    )
    result = transform_bronze_to_silver(frame)
    row = result.collect()[0]
    assert str(row["event_date"]) == "2024-03-10"
