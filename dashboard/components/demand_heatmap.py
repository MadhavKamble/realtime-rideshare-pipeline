from __future__ import annotations

import pandas as pd
import pydeck as pdk
import streamlit as st

from dashboard.data_connectors.redis_reader import get_cached_zone


DEMO_CITY_ZONES = {
    "airport": {"lat_center": 13.1986, "lon_center": 77.7066, "demand_multiplier": 1.8},
    "railway_station": {"lat_center": 12.9784, "lon_center": 77.5707, "demand_multiplier": 2.2},
    "cbd": {"lat_center": 12.9716, "lon_center": 77.5946, "demand_multiplier": 1.5},
    "mall": {"lat_center": 12.9352, "lon_center": 77.6245, "demand_multiplier": 1.2},
    "residential": {"lat_center": 13.0200, "lon_center": 77.5800, "demand_multiplier": 1.0},
}


def _load_heatmap_frame() -> tuple[pd.DataFrame, bool]:
    rows = []
    live_found = False

    for zone_name, zone in DEMO_CITY_ZONES.items():
        cached = get_cached_zone(zone_name)
        if cached:
            live_found = True
            ride_count = int(cached.get("ride_count", 0))
            surge_multiplier = float(cached.get("surge_multiplier", zone["demand_multiplier"]))
        else:
            ride_count = int(zone["demand_multiplier"] * 100)
            surge_multiplier = zone["demand_multiplier"]

        rows.append(
            {
                "lat_grid": zone["lat_center"],
                "lon_grid": zone["lon_center"],
                "ride_count": max(ride_count, 1),
                "city_zone": zone_name,
                "surge_multiplier": surge_multiplier,
            }
        )

    if not live_found:
        return _demo_heatmap_frame(), False
    return pd.DataFrame(rows), True


def _demo_heatmap_frame() -> pd.DataFrame:
    rows = []
    for zone_name, zone in DEMO_CITY_ZONES.items():
        base_weight = int(zone["demand_multiplier"] * 100)
        rows.append(
            {
                "lat_grid": zone["lat_center"],
                "lon_grid": zone["lon_center"],
                "ride_count": base_weight,
                "city_zone": zone_name,
            }
        )
    return pd.DataFrame(rows)


def render_heatmap() -> None:
    df, live_found = _load_heatmap_frame()
    if df.empty:
        st.info("No Gold heatmap data yet. Once the pipeline runs, demand will appear here.")
        return

    if live_found:
        st.caption("Live heatmap powered by Redis zone demand. Delta Gold remains the long-term source of truth.")
    else:
        st.caption("Demo heatmap based on simulator city zones. Live Gold data will replace this once the pipeline starts writing to Delta.")

    layer = pdk.Layer(
        "HeatmapLayer",
        data=df,
        get_position=["lon_grid", "lat_grid"],
        get_weight="ride_count",
        radius_pixels=60,
        intensity=1,
        threshold=0.3,
    )
    view = pdk.ViewState(latitude=12.97, longitude=77.59, zoom=11, pitch=0)
    st.pydeck_chart(pdk.Deck(layers=[layer], initial_view_state=view))
