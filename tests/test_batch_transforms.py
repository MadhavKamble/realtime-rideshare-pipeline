from pyspark.sql import SparkSession

from processing.batch.bronze_to_silver import transform_bronze_to_silver
from processing.batch.silver_to_gold import build_hourly_kpis


def _spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("rideshare-tests").getOrCreate()


def test_bronze_to_silver_enriches_rows():
    spark = _spark()
    frame = spark.createDataFrame(
        [
            {
                "ride_id": "ride-1",
                "event_timestamp": "2024-01-01T08:00:00Z",
                "status": "completed",
                "fare_base_inr": 100.0,
                "surge_multiplier": 1.5,
                "city_zone": "cbd",
            }
        ]
    )

    spark_like = transform_bronze_to_silver(frame)
    assert "event_date" in spark_like.columns
    assert "gross_fare_inr" in spark_like.columns


def test_gold_builder_groups_rows():
    spark = _spark()
    frame = spark.createDataFrame(
        [
            {
                "event_date": "2024-01-01",
                "event_hour": 8,
                "city_zone": "cbd",
                "ride_id": "ride-1",
                "is_completed": True,
                "status": "completed",
                "gross_fare_inr": 150.0,
                "surge_multiplier": 1.5,
            }
        ]
    )

    output = build_hourly_kpis(frame)
    assert "ride_count" in output.columns
