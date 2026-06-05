from __future__ import annotations

import logging
import time

from kafka import KafkaAdminClient
from kafka.admin import NewTopic

from ingestion.kafka_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_DRIVERS, KAFKA_TOPIC_PAYMENTS, KAFKA_TOPIC_RIDES


LOGGER = logging.getLogger(__name__)
TOPICS = (
    KAFKA_TOPIC_RIDES,
    KAFKA_TOPIC_DRIVERS,
    KAFKA_TOPIC_PAYMENTS,
)


def ensure_topics() -> None:
    admin = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, client_id="rideshare-topic-init")
    try:
        existing_topics = set(admin.list_topics())
        new_topics = [
            NewTopic(name=topic, num_partitions=4, replication_factor=1)
            for topic in TOPICS
            if topic not in existing_topics
        ]
        if new_topics:
            LOGGER.info("Creating Kafka topics: %s", ", ".join(topic.name for topic in new_topics))
            admin.create_topics(new_topics=new_topics, validate_only=False)
        else:
            LOGGER.info("Kafka topics already exist")
    finally:
        admin.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    while True:
        try:
            ensure_topics()
            return
        except Exception as exc:
            LOGGER.exception("Topic initialization failed: %s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()
