from __future__ import annotations

import plotly.express as px
import streamlit as st
from pyspark.sql import functions as F

from dashboard.components import PALETTE, empty_state, metric_row, page_header, style_figure
from src.analytics.queries import bounded_pandas, sales_breakdown, table_df
from src.monitoring.pipeline_logger import load_current_result
from utils.helpers import format_inr


def render() -> None:
    page_header("Customer Analytics", "Explore repeat purchasing, customer value, and customer geography.")
    try:
        customers = table_df("gold_customer_summary")
        facts = customers.agg(
            F.countDistinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("orders_count") > 1, 1).otherwise(0)).alias("repeat_customers"),
            F.avg("orders_count").alias("average_orders_per_customer"),
        ).first()
    except Exception as exc:
        empty_state(f"Customer analytics are unavailable until the pipeline creates Gold tables: {exc}")
        return

    current = load_current_result() or {}
    total_customers = int(facts["unique_customers"] or 0)
    repeat_customers = int(facts["repeat_customers"] or 0)
    repeat_share = repeat_customers / total_customers * 100 if total_customers else 0.0
    metric_row(
        [
            ("Unique customers", f"{total_customers:,}", None),
            ("Repeat customers", f"{repeat_customers:,}", f"{repeat_share:.1f}% of customers"),
            ("Average orders per customer", f"{float(facts['average_orders_per_customer'] or 0):.2f}", None),
            ("Total delivered revenue", format_inr(current.get("summary", {}).get("delivered_revenue")), None),
        ]
    )

    frequency = bounded_pandas(
        customers.groupBy("orders_count").agg(F.countDistinct("customer_id").alias("customers"))
        .orderBy("orders_count").limit(50),
        50,
    )
    top_customers = bounded_pandas(
        customers.orderBy(F.col("delivered_revenue").desc()).select(
            "customer_id", "customer_name", "region", "orders_count", "delivered_revenue"
        ).limit(20),
        20,
    )
    by_region = bounded_pandas(
        customers.groupBy("region").agg(
            F.countDistinct("customer_id").alias("customers"),
            F.sum("delivered_revenue").alias("delivered_revenue"),
        ).orderBy("customers", ascending=False).limit(20),
        20,
    )
    purchases = bounded_pandas(sales_breakdown(group_column="order_date", limit=1_000), 1_000)

    left, right = st.columns(2)
    with left:
        fig = px.bar(frequency, x="orders_count", y="customers", title="Customer purchase frequency", color_discrete_sequence=[PALETTE["blue"]])
        st.plotly_chart(style_figure(fig), use_container_width=True)
    with right:
        fig = px.bar(by_region, x="region", y="customers", title="Customers by region", color_discrete_sequence=[PALETTE["teal"]])
        st.plotly_chart(style_figure(fig), use_container_width=True)
    fig = px.line(purchases, x="order_date", y="total_orders", title="Daily order trend", markers=True)
    fig.update_traces(line_color=PALETTE["blue"])
    st.plotly_chart(style_figure(fig), use_container_width=True)

    st.subheader("Top customers by delivered revenue")
    st.dataframe(top_customers, use_container_width=True, hide_index=True)
