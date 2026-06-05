from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from ingestion.kafka_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_DRIVERS, KAFKA_TOPIC_PAYMENTS, KAFKA_TOPIC_RIDES
from storage.delta_config import BRONZE_DRIVERS_TABLE, BRONZE_PAYMENTS_TABLE, BRONZE_RIDES_TABLE


RIDE_SCHEMA = StructType([
    StructField("ride_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("pickup_lat", DoubleType(), True),
    StructField("pickup_lon", DoubleType(), True),
    StructField("dropoff_lat", DoubleType(), True),
    StructField("dropoff_lon", DoubleType(), True),
    StructField("city_zone", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("vehicle_type", StringType(), True),
    StructField("fare_base_inr", DoubleType(), True),
    StructField("surge_multiplier", DoubleType(), True),
    StructField("weather", StringType(), True),
    StructField("event_delay_ms", IntegerType(), True),
    StructField("schema_version", StringType(), True),
])

DRIVER_SCHEMA = StructType([
    StructField("driver_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("status", StringType(), True),
    StructField("current_zone", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("rating", DoubleType(), True),
    StructField("schema_version", StringType(), True),
])

PAYMENT_SCHEMA = StructType([
    StructField("payment_id", StringType(), True),
    StructField("ride_id", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("amount_inr", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("schema_version", StringType(), True),
])


def _bootstrap_delta_table(spark: SparkSession, path: str, schema: StructType, partition_column: str | None = None) -> None:
    empty_df = spark.createDataFrame([], schema)
    writer = empty_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_column:
        writer = writer.partitionBy(partition_column)
    writer.save(path)


def _ensure_bronze_table(spark: SparkSession, path: str, schema: StructType, partition_column: str | None = None) -> None:
    _bootstrap_delta_table(spark, path, schema, partition_column)


def build_spark(app_name: str = "KafkaToBronze") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def _read_stream(spark: SparkSession, topic: str, schema: StructType):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .select(F.col("value").cast("string").alias("json_str"), F.col("timestamp").alias("kafka_timestamp"))
        .select(F.from_json("json_str", schema).alias("data"), "kafka_timestamp")
        .select("data.*")
    )


def start_ride_stream(output_path: str = BRONZE_RIDES_TABLE):
    spark = build_spark()
    _ensure_bronze_table(spark, output_path, RIDE_SCHEMA, partition_column="city_zone")
    df = _read_stream(spark, KAFKA_TOPIC_RIDES, RIDE_SCHEMA)
    checkpoint_path = f"{output_path}/_checkpoints"
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("city_zone")
        .start()
    )


def start_driver_stream(output_path: str = BRONZE_DRIVERS_TABLE):
    spark = build_spark()
    _ensure_bronze_table(spark, output_path, DRIVER_SCHEMA, partition_column="current_zone")
    df = _read_stream(spark, KAFKA_TOPIC_DRIVERS, DRIVER_SCHEMA)
    checkpoint_path = f"{output_path}/_checkpoints"
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .partitionBy("current_zone")
        .start()
    )


def start_payment_stream(output_path: str = BRONZE_PAYMENTS_TABLE):
    spark = build_spark()
    _ensure_bronze_table(spark, output_path, PAYMENT_SCHEMA)
    df = _read_stream(spark, KAFKA_TOPIC_PAYMENTS, PAYMENT_SCHEMA)
    checkpoint_path = f"{output_path}/_checkpoints"
    return (
        df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", output_path)
        .start()
    )


def main() -> None:
    ride_query = start_ride_stream()
    driver_query = start_driver_stream()
    payment_query = start_payment_stream()
    spark = ride_query.sparkSession
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
