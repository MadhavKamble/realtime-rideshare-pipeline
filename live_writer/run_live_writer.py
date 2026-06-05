from __future__ import annotations

import json
import logging
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone

from kafka import KafkaConsumer

from ingestion.kafka_config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC_RIDES
from storage.redis_client import set_live_rides_last_5min, set_zone_demand


LOGGER = logging.getLogger(__name__)
WINDOW = timedelta(minutes=5)


def _parse_timestamp(raw_event: dict) -> datetime:
    timestamp_text = raw_event.get("event_timestamp")
    if not timestamp_text:
        return datetime.now(timezone.utc)
    try:
        parsed = datetime.fromisoformat(timestamp_text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.now(timezone.utc)


class LiveDashboardSeeder:
    def __init__(self) -> None:
        self._ride_timestamps: deque[datetime] = deque()
        self._zone_events: dict[str, deque[tuple[datetime, float]]] = defaultdict(deque)

    def _prune(self, current_time: datetime) -> None:
        cutoff = current_time - WINDOW
        while self._ride_timestamps and self._ride_timestamps[0] < cutoff:
            self._ride_timestamps.popleft()

        for zone, events in list(self._zone_events.items()):
            while events and events[0][0] < cutoff:
                events.popleft()
            if not events:
                del self._zone_events[zone]

    def ingest_ride(self, event: dict) -> None:
        current_time = _parse_timestamp(event)
        zone = event.get("city_zone", "unknown")
        surge_multiplier = float(event.get("surge_multiplier", 1.0))

        self._ride_timestamps.append(current_time)
        self._zone_events[zone].append((current_time, surge_multiplier))
        self._prune(current_time)

        set_live_rides_last_5min(len(self._ride_timestamps))

        zone_events = self._zone_events[zone]
        if zone_events:
            average_surge = sum(surge for _, surge in zone_events) / len(zone_events)
            set_zone_demand(zone, len(zone_events), round(average_surge, 2))


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC_RIDES,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="rideshare-live-dashboard-seeder",
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    seeder = LiveDashboardSeeder()

    while True:
        try:
            LOGGER.info("Connecting to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            consumer = build_consumer()
            LOGGER.info("Consuming ride events from %s", KAFKA_TOPIC_RIDES)
            for message in consumer:
                seeder.ingest_ride(message.value)
        except KeyboardInterrupt:
            LOGGER.info("Stopping live dashboard seeder")
            return
        except Exception as exc:
            LOGGER.exception("Live dashboard seeder error: %s", exc)
            time.sleep(5)


if __name__ == "__main__":
    main()