from __future__ import annotations

import time

from processing.streaming.bronze_to_silver_stream import start_bronze_to_silver_stream
from processing.streaming.kafka_to_bronze import start_driver_stream, start_payment_stream, start_ride_stream


def main() -> None:
    ride_query = start_ride_stream()
    driver_query = start_driver_stream()
    payment_query = start_payment_stream()
    silver_query = start_bronze_to_silver_stream()

    print("✓ Streaming pipeline started: Kafka -> Bronze -> Silver")
    try:
        while True:
            time.sleep(30)
            active = sum(query.isActive for query in [ride_query, driver_query, payment_query, silver_query])
            print(f"✓ Active streaming queries: {active}/4")
    except KeyboardInterrupt:
        for query in [ride_query, driver_query, payment_query, silver_query]:
            query.stop()


if __name__ == "__main__":
    main()