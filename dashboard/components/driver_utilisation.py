from __future__ import annotations

import streamlit as st
import pandas as pd
import json


def render_driver_utilisation() -> None:
    try:
        from storage.redis_client import get_client
        
        client = get_client()
        util_raw = client.get("dashboard:driver_utilisation")
        
        if not util_raw:
            st.info("Driver utilisation data not yet available. Pipeline will populate this once it runs.")
            return
        
        util_data = json.loads(util_raw)
        utilisation = util_data.get("utilisation_percent", 0.0)
        total_rides = util_data.get("total_rides", 0)
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Utilisation %", f"{utilisation:.1f}%")
        
        with col2:
            st.metric("Total Rides", f"{total_rides:,}")
        
        with col3:
            # Estimate cancellation from utilisation
            cancelled_pct = 100 - utilisation if utilisation < 100 else 0
            st.metric("Cancellation %", f"{cancelled_pct:.1f}%")
        
        st.subheader("Status Distribution")
        status_data = pd.DataFrame({
            "Status": ["Completed", "Cancelled"],
            "Count": [int(total_rides * utilisation / 100), int(total_rides * (100 - utilisation) / 100)]
        })
        st.bar_chart(status_data.set_index("Status"))
        
    except Exception as exc:
        st.warning(f"Driver utilisation unavailable: {exc}")
