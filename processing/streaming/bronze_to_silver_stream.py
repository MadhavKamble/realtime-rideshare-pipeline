from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from processing.batch.bronze_to_silver import (
    transform_bronze_to_silver,
    transform_bronze_to_silver_drivers,
    transform_bronze_to_silver_payments,
)
from processing.streaming.kafka_to_bronze import DRIVER_SCHEMA, PAYMENT_SCHEMA, RIDE_SCHEMA, build_spark
from storage.delta_config import (
    BRONZE_DRIVERS_TABLE,
    BRONZE_PAYMENTS_TABLE,
    BRONZE_RIDES_TABLE,
    SILVER_DRIVERS_TABLE,
    SILVER_PAYMENTS_TABLE,
    SILVER_RIDES_TABLE,
)

# 10-minute watermark: events arriving more than 10 min late are dropped.
# Tradeoff: reduces unbounded state accumulation at the cost of losing
# genuinely late events. For ride-sharing, 10 min is conservative —
# most late events arrive within 2-3 min.
WATERMARK_DELAY = "10 minutes"


def _ensure_bronze_schema(spark: SparkSession, input_path: str, schema: StructType) -> None:
    delta_log_path = os.path.join(input_path, "_delta_log")
    if os.path.exists(delta_log_path):
        return

    spark.createDataFrame([], schema).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(input_path)


def _watermarked_dedup(bronze_df: DataFrame, key_column: str) -> DataFrame:
    return (
        bronze_df
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
        .withWatermark("event_ts", WATERMARK_DELAY)
        .dropDuplicates([key_column, "event_ts"])
    )


def start_bronze_to_silver_stream(input_path: str = BRONZE_RIDES_TABLE, output_path: str = SILVER_RIDES_TABLE):
    spark = build_spark("BronzeToSilverStream")
    _ensure_bronze_schema(spark, input_path, RIDE_SCHEMA)
    checkpoint_path = f"{output_path}/_checkpoints"
    bronze_df = spark.readStream.format("delta").load(input_path)
    watermarked_df = _watermarked_dedup(bronze_df, "ride_id")
    silver_df = transform_bronze_to_silver(watermarked_df).withColumn("silver_ingest_ts", F.current_timestamp())
    return (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("event_date")
        .start()
    )


def start_bronze_to_silver_drivers_stream(input_path: str = BRONZE_DRIVERS_TABLE, output_path: str = SILVER_DRIVERS_TABLE):
    spark = build_spark("BronzeToSilverDriversStream")
    _ensure_bronze_schema(spark, input_path, DRIVER_SCHEMA)
    checkpoint_path = f"{output_path}/_checkpoints"
    bronze_df = spark.readStream.format("delta").load(input_path)
    watermarked_df = _watermarked_dedup(bronze_df, "driver_id")
    silver_df = transform_bronze_to_silver_drivers(watermarked_df).withColumn("silver_ingest_ts", F.current_timestamp())
    return (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("event_date")
        .start()
    )


def start_bronze_to_silver_payments_stream(input_path: str = BRONZE_PAYMENTS_TABLE, output_path: str = SILVER_PAYMENTS_TABLE):
    spark = build_spark("BronzeToSilverPaymentsStream")
    _ensure_bronze_schema(spark, input_path, PAYMENT_SCHEMA)
    checkpoint_path = f"{output_path}/_checkpoints"
    bronze_df = spark.readStream.format("delta").load(input_path)
    watermarked_df = _watermarked_dedup(bronze_df, "payment_id")
    silver_df = transform_bronze_to_silver_payments(watermarked_df).withColumn("silver_ingest_ts", F.current_timestamp())
    return (
        silver_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("event_date")
        .start()
    )


def main() -> None:
    ride_query = start_bronze_to_silver_stream()
    driver_query = start_bronze_to_silver_drivers_stream()
    payment_query = start_bronze_to_silver_payments_stream()
    ride_query.sparkSession.streams.awaitAnyTermination()
    driver_query.awaitTermination()
    payment_query.awaitTermination()


if __name__ == "__main__":
    main()
