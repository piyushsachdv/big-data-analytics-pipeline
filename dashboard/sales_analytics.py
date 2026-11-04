from __future__ import annotations

from datetime import date

import plotly.express as px
import streamlit as st
from pyspark.sql import functions as F

from dashboard.components import PALETTE, empty_state, page_header, style_figure
from src.analytics.queries import bounded_pandas, monthly_breakdown, sales_breakdown, table_df


def _choices(column: str) -> list[str]:
    values = [row[0] for row in table_df("silver_transactions").select(column).distinct().orderBy(column).limit(200).collect()]
    return ["All", *[str(value) for value in values if value is not None]]


def render() -> None:
    page_header("Sales Analytics", "Filter delivered revenue and gross transaction value by date, category, region, and order status.")
    try:
        sample = table_df("silver_transactions").agg(
            F.min("order_date").alias("min_date"), F.max("order_date").alias("max_date")
        ).first()
    except Exception as exc:
        empty_state(f"Sales data is not available yet: {exc}")
        return
    if not sample["min_date"]:
        empty_state("The Silver table has no valid transaction dates.")
        return

    date_range = st.date_input(
        "Order date range",
        value=(sample["min_date"], sample["max_date"]),
        min_value=sample["min_date"],
        max_value=sample["max_date"],
    )
    start, end = date_range if isinstance(date_range, tuple) and len(date_range) == 2 else (date_range, date_range)
    filter_columns = st.columns(3)
    category = filter_columns[0].selectbox("Category", _choices("category"))
    region = filter_columns[1].selectbox("Region", _choices("region"))
    status = filter_columns[2].selectbox("Order status", _choices("order_status"))
    start = start if isinstance(start, date) else sample["min_date"]
    end = end if isinstance(end, date) else sample["max_date"]

    filters = dict(start_date=start, end_date=end, category=category, region=region, order_status=status)
    try:
        daily = bounded_pandas(sales_breakdown(group_column="order_date", **filters), 2_000)
        monthly = bounded_pandas(monthly_breakdown(**filters), 200)
        by_category = bounded_pandas(sales_breakdown(group_column="category", **filters), 100)
        by_product = bounded_pandas(sales_breakdown(group_column="product", **filters, limit=100), 100)
        by_region = bounded_pandas(sales_breakdown(group_column="region", **filters), 100)
        by_status = bounded_pandas(sales_breakdown(group_column="order_status", **filters), 20)
    except Exception as exc:
        st.error(f"Could not calculate sales for the selected filters: {exc}")
        return
    if daily.empty:
        empty_state("No transactions match the selected filters.")
        return

    left, right = st.columns(2)
    with left:
        fig = px.line(monthly, x="month", y="delivered_revenue", markers=True, title="Monthly delivered revenue", hover_data=["year"])
        fig.update_xaxes(type="category")
        fig.update_traces(line_color=PALETTE["blue"])
        st.plotly_chart(style_figure(fig), use_container_width=True)
    with right:
        fig = px.line(daily, x="order_date", y="delivered_revenue", markers=True, title="Daily delivered revenue")
        fig.update_traces(line_color=PALETTE["teal"])
        st.plotly_chart(style_figure(fig), use_container_width=True)

    left, right = st.columns(2)
    with left:
        fig = px.bar(by_category, x="category", y="delivered_revenue", title="Category sales", color_discrete_sequence=[PALETTE["teal"]])
        st.plotly_chart(style_figure(fig), use_container_width=True)
    with right:
        fig = px.bar(by_product.sort_values("delivered_revenue", ascending=False).head(15), x="product", y="delivered_revenue", title="Top products by delivered revenue", color_discrete_sequence=[PALETTE["blue"]])
        st.plotly_chart(style_figure(fig, height=380), use_container_width=True)
    left, right = st.columns(2)
    with left:
        fig = px.bar(by_region, x="region", y="delivered_revenue", title="Regional sales", color_discrete_sequence=[PALETTE["teal"]])
        st.plotly_chart(style_figure(fig), use_container_width=True)
    with right:
        fig = px.bar(by_status, x="order_status", y="total_orders", title="Order status counts", color_discrete_sequence=[PALETTE["blue"]])
        st.plotly_chart(style_figure(fig), use_container_width=True)
    st.caption("Delivered revenue counts only Delivered orders. Gross transaction value includes all valid order statuses.")
