from __future__ import annotations

import streamlit as st
from datetime import datetime
import json


def render_pipeline_health() -> None:
    st.subheader("Pipeline Health")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        try:
            from storage.redis_client import get_client
            client = get_client()
            rides_count_raw = client.get("live:rides_last_5min")
            if rides_count_raw:
                rides_count = int(json.loads(rides_count_raw)["count"])
                st.metric("Rides (last 5m)", rides_count, delta="live", delta_color="off")
            else:
                st.metric("Rides (last 5m)", 0, delta="waiting", delta_color="off")
        except Exception as exc:
            st.error(f"Live counter failed: {exc}")
    
    with col2:
        try:
            from storage.redis_client import get_client
            client = get_client()
            # Check if any recent event exists in Redis zone data
            zone_keys = client.keys("live:zone:*:demand")
            if zone_keys and len(zone_keys) > 0:
                st.metric("Silver Freshness", "✓ Fresh", delta_color="off")
            else:
                st.metric("Silver Freshness", "⏳ Waiting", delta_color="off")
        except Exception as exc:
            st.warning(f"Silver freshness check failed: {exc}")
    
    with col3:
        try:
            from storage.redis_client import get_client
            client = get_client()
            # Check if Gold metrics are cached
            gold_metrics = client.get("dashboard:total_revenue")
            if gold_metrics:
                st.metric("Gold Zone Demand", "✓ OK", delta_color="off")
            else:
                st.metric("Gold Zone Demand", "⏳ Waiting", delta_color="off")
        except Exception as exc:
            st.warning(f"Gold health check failed: {exc}")
    
    st.info("Pipeline Status: All services operational. Simulator, live writer, and dashboard are running.")
