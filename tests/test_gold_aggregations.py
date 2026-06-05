from pyspark.sql import SparkSession

from processing.batch.silver_to_gold import build_hourly_kpis


def _spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("rideshare-tests").getOrCreate()


def _make_silver_frame(spark: SparkSession):
    return spark.createDataFrame(
        [
            {"event_date": "2024-01-01", "event_hour": 8, "city_zone": "cbd",      "ride_id": "r1", "is_completed": True,  "status": "completed", "gross_fare_inr": 150.0, "surge_multiplier": 1.5},
            {"event_date": "2024-01-01", "event_hour": 8, "city_zone": "cbd",      "ride_id": "r2", "is_completed": False, "status": "cancelled", "gross_fare_inr": 0.0,   "surge_multiplier": 1.5},
            {"event_date": "2024-01-01", "event_hour": 8, "city_zone": "airport",  "ride_id": "r3", "is_completed": True,  "status": "completed", "gross_fare_inr": 300.0, "surge_multiplier": 2.0},
            {"event_date": "2024-01-01", "event_hour": 9, "city_zone": "cbd",      "ride_id": "r4", "is_completed": True,  "status": "completed", "gross_fare_inr": 200.0, "surge_multiplier": 1.8},
        ]
    )


def test_gold_groups_by_zone_and_hour():
    spark = _spark()
    result = build_hourly_kpis(_make_silver_frame(spark))
    assert result.count() == 3  # cbd@8, airport@8, cbd@9


def test_gold_ride_count():
    spark = _spark()
    result = build_hourly_kpis(_make_silver_frame(spark))
    from pyspark.sql import functions as F
    cbd_8 = result.filter((F.col("city_zone") == "cbd") & (F.col("event_hour") == 8)).collect()[0]
    assert cbd_8["ride_count"] == 2


def test_gold_completed_and_cancelled_counts():
    spark = _spark()
    result = build_hourly_kpis(_make_silver_frame(spark))
    from pyspark.sql import functions as F
    cbd_8 = result.filter((F.col("city_zone") == "cbd") & (F.col("event_hour") == 8)).collect()[0]
    assert cbd_8["completed_rides"] == 1
    assert cbd_8["cancelled_rides"] == 1


def test_gold_gross_revenue():
    spark = _spark()
    result = build_hourly_kpis(_make_silver_frame(spark))
    from pyspark.sql import functions as F
    airport_8 = result.filter((F.col("city_zone") == "airport") & (F.col("event_hour") == 8)).collect()[0]
    assert abs(airport_8["gross_revenue_inr"] - 300.0) < 0.01


def test_gold_avg_surge():
    spark = _spark()
    result = build_hourly_kpis(_make_silver_frame(spark))
    from pyspark.sql import functions as F
    cbd_8 = result.filter((F.col("city_zone") == "cbd") & (F.col("event_hour") == 8)).collect()[0]
    assert abs(cbd_8["avg_surge_multiplier"] - 1.5) < 0.01


def test_gold_output_columns():
    spark = _spark()
    result = build_hourly_kpis(_make_silver_frame(spark))
    expected = {"event_date", "event_hour", "city_zone", "ride_count", "completed_rides", "cancelled_rides", "gross_revenue_inr", "avg_surge_multiplier"}
    assert expected.issubset(set(result.columns))
