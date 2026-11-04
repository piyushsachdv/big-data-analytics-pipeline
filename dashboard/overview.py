from __future__ import annotations

from html import escape

import plotly.express as px
import streamlit as st

from dashboard.components import PALETTE, empty_state, metric_row, page_header, status_pill, style_figure
from src.analytics.queries import bounded_pandas, table_df, table_exists
from src.monitoring.pipeline_logger import load_current_result, load_events
from utils.helpers import format_inr


def render() -> None:
    page_header("Business overview", "A clear snapshot of processed sales, customers, and data quality.")
    current = load_current_result()
    if not current:
        empty_state("No pipeline run is recorded yet. Open Pipeline Monitor to process a sample dataset.")
        st.markdown("### Pipeline architecture")
        st.code("CSV → HDFS Bronze → Spark on YARN → HDFS Silver/Gold → Hive catalog → Streamlit", language="text")
        return

    summary = current.get("summary", {})
    quality = current.get("quality", {})
    status = str(current.get("status", "WARNING"))
    st.markdown(
        '<div class="run-meta">'
        f"<span>{status_pill(status)}</span>"
        f"<span><strong>Dataset</strong> {escape(str(current.get('source_name', 'Unknown')))}</span>"
        f"<span><strong>Run</strong> <code>{escape(str(current.get('run_id', '—')))}</code></span>"
        f"<span><strong>YARN app</strong> <code>{escape(str(current.get('application_id') or 'Not submitted'))}</code></span>"
        "</div>",
        unsafe_allow_html=True,
    )

    metric_row(
        [
            ("Delivered revenue", format_inr(summary.get("delivered_revenue")), None),
            ("Unique orders", f"{int(summary.get('total_orders') or 0):,}", None),
            ("Unique customers", f"{int(summary.get('unique_customers') or 0):,}", None),
            ("Average delivered order", format_inr(summary.get("average_order_value")), None),
        ]
    )
    st.markdown(
        '<div class="overview-panel"><div class="overview-panel-title">Data quality summary</div>'
        '<div class="quality-summary">'
        f"<span><strong>Input</strong> {int(quality.get('input_records') or 0):,}</span>"
        f"<span><strong>Valid</strong> {int(quality.get('valid_records') or 0):,}</span>"
        f"<span><strong>Rejected</strong> {int(quality.get('rejected_records') or 0):,}</span>"
        f"<span><strong>Duplicates removed</strong> {int(quality.get('duplicates_removed') or 0):,}</span>"
        "</div></div>",
        unsafe_allow_html=True,
    )

    try:
        monthly_available = table_exists("gold_sales_by_month")
    except Exception as exc:
        monthly_available = False
        st.warning(f"Could not check the Gold monthly table in the Spark SQL catalog: {exc}")

    left, right = st.columns(2)
    with left:
        st.subheader("Revenue by month")
        if monthly_available:
            try:
                monthly = bounded_pandas(table_df("gold_sales_by_month").orderBy("order_year", "order_month"), 300)
                if monthly.empty:
                    empty_state("The Gold monthly table exists but contains no rows for this run.")
                else:
                    monthly["month"] = monthly["order_year"].astype(str) + "-" + monthly["order_month"].astype(str).str.zfill(2)
                    fig = px.line(monthly, x="month", y="delivered_revenue", markers=True, title="Delivered revenue")
                    fig.update_traces(line_color=PALETTE["blue"], line_width=3)
                    st.plotly_chart(style_figure(fig), use_container_width=True)
            except Exception as exc:
                st.warning(f"The Gold monthly table is registered but could not be read: {exc}")
        else:
            empty_state("The monthly Gold output is unavailable in the latest successful HDFS run. Run the pipeline and review its output verification stage.")
    with right:
        st.subheader("Revenue by category")
        try:
            if table_exists("gold_sales_by_category"):
                categories = bounded_pandas(table_df("gold_sales_by_category").orderBy("delivered_revenue", ascending=False), 30)
                if not categories.empty:
                    fig = px.bar(categories, x="category", y="delivered_revenue", title="Delivered revenue", color_discrete_sequence=[PALETTE["teal"]])
                    st.plotly_chart(style_figure(fig), use_container_width=True)
                else:
                    empty_state("The category Gold table exists but contains no rows.")
            else:
                empty_state("Category Gold output is unavailable in the latest successful HDFS run.")
        except Exception as exc:
            st.warning(f"Category Gold data could not be loaded: {exc}")

    left, right = st.columns(2)
    with left:
        st.subheader("Revenue by region")
        try:
            if table_exists("gold_sales_by_region"):
                regions = bounded_pandas(table_df("gold_sales_by_region").orderBy("delivered_revenue", ascending=False), 20)
                if not regions.empty:
                    fig = px.bar(regions, x="region", y="delivered_revenue", title="Delivered revenue", color_discrete_sequence=[PALETTE["blue"]])
                    st.plotly_chart(style_figure(fig), use_container_width=True)
                else:
                    empty_state("The regional Gold table exists but contains no rows.")
            else:
                empty_state("Regional Gold output is unavailable in the latest successful HDFS run.")
        except Exception as exc:
            st.warning(f"Regional Gold data could not be loaded: {exc}")
    with right:
        st.subheader("Order status")
        try:
            if table_exists("gold_order_status"):
                statuses = bounded_pandas(table_df("gold_order_status").orderBy("total_orders", ascending=False), 10)
                if not statuses.empty:
                    fig = px.pie(statuses, names="order_status", values="total_orders", hole=0.58, title="Orders by status")
                    st.plotly_chart(style_figure(fig), use_container_width=True)
                else:
                    empty_state("The order-status Gold table exists but contains no rows.")
            else:
                empty_state("Order-status Gold output is unavailable in the latest successful HDFS run.")
        except Exception as exc:
            st.warning(f"Order-status Gold data could not be loaded: {exc}")

    st.subheader("Recent pipeline activity")
    events = load_events(str(current.get("run_id")))
    if events:
        st.dataframe(
            [
                {
                    "Time (UTC)": event.get("timestamp"),
                    "Stage": event.get("stage"),
                    "State": event.get("state"),
                    "Message": event.get("message"),
                }
                for event in events[-8:]
            ],
            use_container_width=True,
            hide_index=True,
        )
