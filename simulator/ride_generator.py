from __future__ import annotations

import random
import uuid
from datetime import datetime, timezone

from simulator.config import (
    CITY_ZONES,
    LATE_EVENT_DELAY_MS_MAX,
    LATE_EVENT_DELAY_MS_MIN,
    LATE_EVENT_PROBABILITY,
    OFF_PEAK_DEMAND_MULTIPLIER,
    PEAK_DEMAND_MULTIPLIER,
    PEAK_HOURS_EVENING,
    PEAK_HOURS_MORNING,
    VEHICLE_BASE_FARE,
    VEHICLE_TYPES,
    VEHICLE_WEIGHTS,
    WEATHER_DEMAND_MULTIPLIER,
    WEATHER_STATES,
)


def get_current_demand_multiplier(hour: int, weather: str) -> float:
    if hour in PEAK_HOURS_MORNING or hour in PEAK_HOURS_EVENING:
        base = PEAK_DEMAND_MULTIPLIER
    elif hour in range(2, 6):
        base = OFF_PEAK_DEMAND_MULTIPLIER
    else:
        base = 1.0
    return base * WEATHER_DEMAND_MULTIPLIER[weather]


def compute_surge_multiplier(zone: str, hour: int, weather: str) -> float:
    demand = get_current_demand_multiplier(hour, weather)
    zone_factor = CITY_ZONES[zone]["demand_multiplier"]
    raw = (demand * zone_factor) / 2.0
    surge = 1.0 + min(raw - 1.0, 2.5)
    return round(max(1.0, min(3.5, surge)), 2)


def _choose_zone() -> str:
    return random.choices(list(CITY_ZONES.keys()), weights=[2, 2, 3, 2, 4], k=1)[0]


def _choose_vehicle_type() -> str:
    return random.choices(VEHICLE_TYPES, weights=VEHICLE_WEIGHTS, k=1)[0]


def _choose_weather(hour: int) -> str:
    if 6 <= hour < 12:
        return "clear"
    if 12 <= hour < 18:
        return "cloudy"
    return random.choices(WEATHER_STATES, weights=[0.55, 0.20, 0.25], k=1)[0]


def _sample_point(zone: str) -> tuple[float, float]:
    details = CITY_ZONES[zone]
    lat = random.gauss(details["lat_center"], details["lat_std"])
    lon = random.gauss(details["lon_center"], details["lon_std"])
    return round(lat, 6), round(lon, 6)


def generate_ride_event(weather: str | None = None) -> dict:
    now = datetime.now(timezone.utc)
    hour = now.hour
    zone = _choose_zone()
    vehicle_type = _choose_vehicle_type()
    current_weather = weather or _choose_weather(hour)
    surge_multiplier = compute_surge_multiplier(zone, hour, current_weather)
    pickup_lat, pickup_lon = _sample_point(zone)
    dropoff_lat, dropoff_lon = _sample_point(zone)
    distance_km = round(random.uniform(1.5, 18.0), 2)
    fare_base = round(distance_km * VEHICLE_BASE_FARE[vehicle_type], 2)
    event_delay_ms = random.randint(0, LATE_EVENT_DELAY_MS_MAX) if random.random() < LATE_EVENT_PROBABILITY else 0

    return {
        "ride_id": str(uuid.uuid4()),
        "event_timestamp": now.isoformat(),
        "driver_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "status": random.choices(["requested", "completed", "cancelled"], weights=[0.12, 0.80, 0.08], k=1)[0],
        "pickup_lat": pickup_lat,
        "pickup_lon": pickup_lon,
        "dropoff_lat": dropoff_lat,
        "dropoff_lon": dropoff_lon,
        "city_zone": zone,
        "distance_km": distance_km,
        "vehicle_type": vehicle_type,
        "fare_base_inr": fare_base,
        "surge_multiplier": surge_multiplier,
        "weather": current_weather,
        "event_delay_ms": event_delay_ms,
        "schema_version": "1.0",
    }
