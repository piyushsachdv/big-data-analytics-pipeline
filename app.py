from __future__ import annotations

import streamlit as st
from dashboard.components import apply_dashboard_styles


st.set_page_config(
    page_title="E-commerce Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)
apply_dashboard_styles()

with st.sidebar:
    st.markdown("## Sales Intelligence")
    st.caption("From raw transactions to explainable analytics")
    page = st.radio(
        "Navigation",
        [
            "Overview",
            "Data Explorer",
            "Pipeline Monitor",
            "Sales Analytics",
            "Customer Analytics",
            "Data Quality",
        ],
        label_visibility="collapsed",
    )
    st.divider()
    st.caption("Local analytics cluster · HDFS · YARN · Spark SQL")

if page == "Overview":
    from dashboard.overview import render
elif page == "Data Explorer":
    from dashboard.data_explorer import render
elif page == "Pipeline Monitor":
    from dashboard.pipeline_monitor import render
elif page == "Sales Analytics":
    from dashboard.sales_analytics import render
elif page == "Customer Analytics":
    from dashboard.customer_analytics import render
else:
    from dashboard.data_quality import render

render()
