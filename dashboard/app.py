from __future__ import annotations

import streamlit as st
import streamlit.components.v1 as components

from components.demand_heatmap import render_heatmap
from components.live_counter import render_live_counter
from components.revenue_charts import render_revenue
from components.driver_utilisation import render_driver_utilisation
from components.pipeline_health import render_pipeline_health


st.set_page_config(
    page_title="Rideshare Analytics",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# Auto-refresh every 30 seconds via JavaScript — no extra package required
components.html("<script>setTimeout(function(){window.location.reload()},30000);</script>", height=0)

st.title("🚗 Real-Time Ride-Sharing Analytics Platform")
st.caption("Live pipeline — data flowing from Kafka → Redis → Dashboard")

# Top row: Live KPIs
col1, col2, col3, col4 = st.columns(4)
with col1:
    render_live_counter()

# Pipeline health section
with st.expander("📊 Pipeline Health", expanded=True):
    render_pipeline_health()

# Tabbed interface
tab1, tab2, tab3 = st.tabs(["🗺️ Demand Heatmap", "💰 Revenue", "🧑‍✈️ Drivers"])

with tab1:
    st.subheader("Live Demand Heatmap")
    render_heatmap()

with tab2:
    st.subheader("Revenue Analytics")
    render_revenue()

with tab3:
    st.subheader("Driver Utilisation")
    render_driver_utilisation()

