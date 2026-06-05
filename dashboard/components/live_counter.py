from __future__ import annotations

import streamlit as st

from dashboard.data_connectors.redis_reader import get_live_rides_last_5min


def render_live_counter() -> None:
    count = get_live_rides_last_5min()
    st.metric("Rides in last 5 min", count)
    if count == 0:
        st.caption("Waiting for live events from the simulator and Redis cache.")
