from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from simulator.config import CITY_ZONES


DRIVER_STATUSES = ["online", "offline", "on_trip"]


def _choose_zone() -> str:
    return random.choices(list(CITY_ZONES.keys()), weights=[2, 2, 3, 2, 4], k=1)[0]


def generate_driver_event() -> dict:
    now = datetime.now(timezone.utc)
    zone = _choose_zone()
    zone_details = CITY_ZONES[zone]
    return {
        "driver_id": str(uuid.uuid4()),
        "event_timestamp": now.isoformat(),
        "status": random.choices(DRIVER_STATUSES, weights=[0.62, 0.18, 0.20], k=1)[0],
        "current_zone": zone,
        "lat": round(random.gauss(zone_details["lat_center"], zone_details["lat_std"]), 6),
        "lon": round(random.gauss(zone_details["lon_center"], zone_details["lon_std"]), 6),
        "rating": round(random.uniform(3.6, 5.0), 2),
        "schema_version": "1.0",
    }
