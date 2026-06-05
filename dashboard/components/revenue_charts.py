from __future__ import annotations

import streamlit as st
import pandas as pd
import json


def render_revenue() -> None:
    try:
        from storage.redis_client import get_client
        
        client = get_client()
        
        # Fetch metrics from Redis
        revenue_raw = client.get("dashboard:total_revenue")
        surge_raw = client.get("dashboard:avg_surge")
        
        if not revenue_raw or not surge_raw:
            st.info("Revenue analytics not yet available. Dashboard warmup DAG will populate this data every 15 minutes.")
            return
        
        revenue_data = json.loads(revenue_raw)
        surge_data = json.loads(surge_raw)
        
        total_revenue = revenue_data.get("amount", 0.0)
        avg_surge = surge_data.get("surge", 1.0)
        
        # Get zone counts
        zone_keys = client.keys("dashboard:zone:*:count")
        zone_data = {}
        total_rides = 0
        for key in zone_keys:
            key_str = key.decode() if isinstance(key, bytes) else str(key)
            zone_name = key_str.split(":")[2]
            count_raw = client.get(key)
            if count_raw:
                count = json.loads(count_raw).get("count", 0)
                zone_data[zone_name] = count
                total_rides += count
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Revenue (₹)", f"₹{total_revenue:,.0f}")
        
        with col2:
            st.metric("Avg Surge", f"{avg_surge:.2f}x")
        
        with col3:
            st.metric("Total Rides", f"{total_rides:,}")
        
        if zone_data:
            st.subheader("Ride Count by Zone")
            zone_df = pd.DataFrame(list(zone_data.items()), columns=["Zone", "Rides"])
            zone_df = zone_df.sort_values("Rides", ascending=False)
            st.bar_chart(zone_df.set_index("Zone"))
        else:
            st.caption("Zone data not yet available.")
        
    except Exception as exc:
        st.warning(f"Revenue charts unavailable: {exc}")
