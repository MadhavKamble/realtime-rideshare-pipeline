from __future__ import annotations

import json
import os
import time
from typing import Callable

from kafka import KafkaProducer

from ingestion.kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_DRIVERS,
    KAFKA_TOPIC_PAYMENTS,
    KAFKA_TOPIC_RIDES,
)
from simulator.driver_generator import generate_driver_event
from simulator.payment_generator import generate_payment_event
from simulator.ride_generator import generate_ride_event


class RideShareKafkaProducer:
    def __init__(self) -> None:
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8") if value is not None else None,
            acks="all",
            retries=3,
        )

    def send_event(self, topic: str, event: dict, key: str | None = None) -> None:
        self.producer.send(topic, value=event, key=key)
        self.producer.flush()

    def close(self) -> None:
        self.producer.close()


def publish_once() -> None:
    producer = RideShareKafkaProducer()
    try:
        producer.send_event(KAFKA_TOPIC_RIDES, generate_ride_event(), key="ride")
        producer.send_event(KAFKA_TOPIC_DRIVERS, generate_driver_event(), key="driver")
        producer.send_event(KAFKA_TOPIC_PAYMENTS, generate_payment_event(), key="payment")
    finally:
        producer.close()


def _loop_emitter(interval_seconds: float, generator: Callable[[], dict], topic: str, key: str) -> None:
    producer = RideShareKafkaProducer()
    try:
        while True:
            producer.send_event(topic, generator(), key=key)
            time.sleep(interval_seconds)
    finally:
        producer.close()
