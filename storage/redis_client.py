from __future__ import annotations

import json
import os

import redis
from dotenv import load_dotenv

load_dotenv()
_client = None


def get_client() -> redis.Redis:
    global _client
    if _client is None:
        _client = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 0)),
            decode_responses=True,
        )
    return _client


def set_zone_demand(zone: str, ride_count: int, surge_mult: float, ttl: int = 30) -> None:
    client = get_client()
    payload = json.dumps({"ride_count": ride_count, "surge_multiplier": surge_mult})
    client.setex(f"live:zone:{zone}:demand", ttl, payload)


def get_zone_demand(zone: str) -> dict | None:
    client = get_client()
    raw = client.get(f"live:zone:{zone}:demand")
    return json.loads(raw) if raw else None


def set_driver_location(driver_id: str, lat: float, lon: float, ttl: int = 15) -> None:
    client = get_client()
    payload = json.dumps({"lat": lat, "lon": lon})
    client.setex(f"driver:{driver_id}:location", ttl, payload)


def set_live_rides_last_5min(count: int, ttl: int = 60) -> None:
    client = get_client()
    client.setex("live:rides_last_5min", ttl, json.dumps({"count": count}))
