from pyspark.sql import SparkSession

from processing.batch.bronze_to_silver import (
    transform_bronze_to_silver,
    transform_bronze_to_silver_drivers,
    transform_bronze_to_silver_payments,
)


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


def test_silver_drivers_drops_duplicates():
    spark = _spark()
    frame = spark.createDataFrame(
        [
            {"driver_id": "d1", "event_timestamp": "2024-01-01T08:00:00Z", "status": "online", "current_zone": "cbd"},
            {"driver_id": "d1", "event_timestamp": "2024-01-01T08:00:00Z", "status": "online", "current_zone": "cbd"},
        ]
    )
    result = transform_bronze_to_silver_drivers(frame)
    assert result.count() == 1, "Duplicate driver_id + event_timestamp should be removed"


def test_silver_drivers_adds_event_hour_and_date():
    spark = _spark()
    frame = spark.createDataFrame(
        [{"driver_id": "d2", "event_timestamp": "2024-06-15T14:30:00Z", "status": "online", "current_zone": "cbd"}]
    )
    result = transform_bronze_to_silver_drivers(frame)
    row = result.collect()[0]
    assert row["event_hour"] == 14
    assert str(row["event_date"]) == "2024-06-15"


def test_silver_drivers_is_available_flag():
    spark = _spark()
    frame = spark.createDataFrame(
        [
            {"driver_id": "d3", "event_timestamp": "2024-01-01T10:00:00Z", "status": "online", "current_zone": "cbd"},
            {"driver_id": "d4", "event_timestamp": "2024-01-01T10:01:00Z", "status": "on_trip", "current_zone": "cbd"},
        ]
    )
    result = transform_bronze_to_silver_drivers(frame)
    rows = {row["driver_id"]: row["is_available"] for row in result.collect()}
    assert rows["d3"] is True
    assert rows["d4"] is False


def test_silver_drivers_cleans_current_zone():
    spark = _spark()
    frame = spark.createDataFrame(
        [{"driver_id": "d5", "event_timestamp": "2024-01-01T10:00:00Z", "status": "online", "current_zone": "  CBD  "}]
    )
    result = transform_bronze_to_silver_drivers(frame)
    row = result.collect()[0]
    assert row["current_zone"] == "cbd"


def test_silver_payments_drops_duplicates():
    spark = _spark()
    frame = spark.createDataFrame(
        [
            {"payment_id": "p1", "event_timestamp": "2024-01-01T08:00:00Z", "status": "success", "payment_method": "upi"},
            {"payment_id": "p1", "event_timestamp": "2024-01-01T08:00:00Z", "status": "success", "payment_method": "upi"},
        ]
    )
    result = transform_bronze_to_silver_payments(frame)
    assert result.count() == 1, "Duplicate payment_id should be removed"


def test_silver_payments_normalizes_method_casing():
    spark = _spark()
    frame = spark.createDataFrame(
        [{"payment_id": "p2", "event_timestamp": "2024-01-01T09:00:00Z", "status": "success", "payment_method": "  UPI  "}]
    )
    result = transform_bronze_to_silver_payments(frame)
    row = result.collect()[0]
    assert row["payment_method_clean"] == "upi"


def test_silver_payments_is_completed_flag():
    spark = _spark()
    frame = spark.createDataFrame(
        [
            {"payment_id": "p3", "event_timestamp": "2024-01-01T10:00:00Z", "status": "success", "payment_method": "card"},
            {"payment_id": "p4", "event_timestamp": "2024-01-01T10:01:00Z", "status": "failed", "payment_method": "cash"},
        ]
    )
    result = transform_bronze_to_silver_payments(frame)
    rows = {row["payment_id"]: row["is_completed"] for row in result.collect()}
    assert rows["p3"] is True
    assert rows["p4"] is False
