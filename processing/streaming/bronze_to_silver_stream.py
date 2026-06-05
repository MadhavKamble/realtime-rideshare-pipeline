from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from processing.batch.bronze_to_silver import transform_bronze_to_silver
from processing.streaming.kafka_to_bronze import RIDE_SCHEMA, build_spark
from storage.delta_config import BRONZE_RIDES_TABLE, SILVER_RIDES_TABLE


def _ensure_bronze_schema(spark: SparkSession, input_path: str) -> None:
    delta_log_path = os.path.join(input_path, "_delta_log")
    if os.path.exists(delta_log_path):
        return

    spark.createDataFrame([], RIDE_SCHEMA).write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(input_path)


def start_bronze_to_silver_stream(input_path: str = BRONZE_RIDES_TABLE, output_path: str = SILVER_RIDES_TABLE):
    spark = build_spark("BronzeToSilverStream")
    _ensure_bronze_schema(spark, input_path)
    checkpoint_path = f"{output_path}/_checkpoints"
    bronze_df = spark.readStream.format("delta").load(input_path)
    silver_df = transform_bronze_to_silver(bronze_df).withColumn("silver_ingest_ts", F.current_timestamp())
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
    query = start_bronze_to_silver_stream()
    query.sparkSession.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
