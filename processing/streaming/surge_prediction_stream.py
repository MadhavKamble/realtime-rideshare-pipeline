from __future__ import annotations

import os

from pyspark.sql import functions as F

from ml.serve_predictions import add_surge_predictions
from processing.streaming.kafka_to_bronze import build_spark
from storage.delta_config import SILVER_RIDES_TABLE, DELTA_GOLD_PATH

PREDICTIONS_TABLE = f"{DELTA_GOLD_PATH}/surge_predictions"


def start_surge_prediction_stream(
    input_path: str = SILVER_RIDES_TABLE,
    output_path: str = PREDICTIONS_TABLE,
    model_uri: str | None = None,
):
    """Read the Silver rides stream, apply the registered MLflow surge model as a
    Pandas UDF, and write live predictions to the Gold predictions table.

    The model is loaded once per partition (not per row) so driver-side memory is
    not saturated at high throughput.
    """
    spark = build_spark("SurgePredictionStream")

    silver_stream = (
        spark.readStream
        .format("delta")
        .load(input_path)
        .withColumn("event_hour", F.coalesce(F.col("event_hour"), F.lit(0)))
    )

    predicted_stream = add_surge_predictions(silver_stream, model_uri)

    checkpoint_path = f"{output_path}/_checkpoints"
    return (
        predicted_stream
        .select(
            "ride_id",
            "event_ts",
            "city_zone",
            "vehicle_type",
            "surge_multiplier",
            "predicted_surge_multiplier",
        )
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("city_zone")
        .start()
    )


def main() -> None:
    query = start_surge_prediction_stream()
    query.sparkSession.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
