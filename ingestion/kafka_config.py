from __future__ import annotations

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC_RIDES = os.getenv("KAFKA_TOPIC_RIDES", "ride-events")
KAFKA_TOPIC_DRIVERS = os.getenv("KAFKA_TOPIC_DRIVERS", "driver-events")
KAFKA_TOPIC_PAYMENTS = os.getenv("KAFKA_TOPIC_PAYMENTS", "payment-events")

TOPIC_PARTITIONS = {
    KAFKA_TOPIC_RIDES: 4,
    KAFKA_TOPIC_DRIVERS: 4,
    KAFKA_TOPIC_PAYMENTS: 4,
}
