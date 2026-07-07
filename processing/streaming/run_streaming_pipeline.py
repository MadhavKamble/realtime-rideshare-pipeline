from __future__ import annotations

import time

from processing.streaming.bronze_to_silver_stream import (
    start_bronze_to_silver_drivers_stream,
    start_bronze_to_silver_payments_stream,
    start_bronze_to_silver_stream,
)
from processing.streaming.kafka_to_bronze import start_driver_stream, start_payment_stream, start_ride_stream


def main() -> None:
    ride_query = start_ride_stream()
    driver_query = start_driver_stream()
    payment_query = start_payment_stream()
    silver_ride_query = start_bronze_to_silver_stream()
    silver_driver_query = start_bronze_to_silver_drivers_stream()
    silver_payment_query = start_bronze_to_silver_payments_stream()

    queries = [ride_query, driver_query, payment_query, silver_ride_query, silver_driver_query, silver_payment_query]

    print("✓ Streaming pipeline started: Kafka -> Bronze -> Silver")
    try:
        while True:
            time.sleep(30)
            active = sum(query.isActive for query in queries)
            print(f"✓ Active streaming queries: {active}/{len(queries)}")
    except KeyboardInterrupt:
        for query in queries:
            query.stop()


if __name__ == "__main__":
    main()
