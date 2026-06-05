from __future__ import annotations

import threading
import time

from simulator.kafka_producer import _loop_emitter
from simulator.driver_generator import generate_driver_event
from simulator.payment_generator import generate_payment_event
from simulator.ride_generator import generate_ride_event
from ingestion.kafka_config import KAFKA_TOPIC_DRIVERS, KAFKA_TOPIC_PAYMENTS, KAFKA_TOPIC_RIDES


def main() -> None:
    workers = [
        threading.Thread(target=_loop_emitter, args=(1.0, generate_ride_event, KAFKA_TOPIC_RIDES, "ride"), daemon=True),
        threading.Thread(target=_loop_emitter, args=(1.8, generate_driver_event, KAFKA_TOPIC_DRIVERS, "driver"), daemon=True),
        threading.Thread(target=_loop_emitter, args=(2.2, generate_payment_event, KAFKA_TOPIC_PAYMENTS, "payment"), daemon=True),
    ]
    for worker in workers:
        worker.start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
