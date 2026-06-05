from __future__ import annotations

import json

from storage.redis_client import get_client, get_zone_demand


def get_live_rides_last_5min() -> int:
    try:
        client = get_client()
        raw = client.get("live:rides_last_5min")
        if not raw:
            return 0
        return int(json.loads(raw)["count"])
    except Exception:
        return 0


def get_cached_zone(zone: str) -> dict | None:
    try:
        return get_zone_demand(zone)
    except Exception:
        return None
