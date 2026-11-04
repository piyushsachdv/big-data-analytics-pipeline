from __future__ import annotations

import streamlit as st
from pyspark.sql import functions as F

from dashboard.components import empty_state, page_header
from src.analytics.queries import get_local_spark, table_df
from src.monitoring.pipeline_logger import load_current_result


GOLD_TABLES = (
    "gold_sales_by_month",
    "gold_sales_by_day",
    "gold_sales_by_category",
    "gold_sales_by_region",
    "gold_product_performance",
    "gold_customer_summary",
    "gold_order_status",
)


def _selected_dataframe(selection: str, current: dict):
    if selection == "Original Bronze CSV":
        bronze_uri = current.get("paths", {}).get("bronze") or current.get("bronze", {}).get("bronze_uri")
        if not bronze_uri:
            raise ValueError("The latest run does not contain a Bronze HDFS path.")
        return get_local_spark().read.option("header", True).option("inferSchema", False).csv(bronze_uri)
    if selection == "Clean Silver records":
        return table_df("silver_transactions")
    if selection == "Rejected records":
        return table_df("rejected_transactions")
    return table_df(selection)


def render() -> None:
    page_header("Data Explorer", "Inspect source, cleaned, rejected, and aggregated records with bounded previews.")
    current = load_current_result()
    if not current or current.get("status") != "SUCCEEDED":
        empty_state("Run a successful pipeline before browsing HDFS and Hive datasets.")
        return

    choices = ["Original Bronze CSV", "Clean Silver records", "Rejected records", *GOLD_TABLES]
    selection = st.selectbox("Dataset", choices)
    try:
        frame = _selected_dataframe(selection, current)
        record_count = frame.count()
    except Exception as exc:
        st.error(f"Could not read {selection}: {exc}")
        return

    columns = st.columns(3)
    columns[0].metric("Records", f"{record_count:,}")
    columns[1].metric("Columns", len(frame.columns))
    columns[2].metric("Run", current.get("run_id", "—"))

    with st.expander("Schema and data types", expanded=False):
        st.dataframe(
            [{"Column": field.name, "Spark type": field.dataType.simpleString(), "Nullable": field.nullable} for field in frame.schema.fields],
            hide_index=True,
            use_container_width=True,
        )
    missing_exprs = [
        F.sum(F.when(F.col(name).isNull() | (F.trim(F.col(name).cast("string")) == ""), 1).otherwise(0)).alias(name)
        for name in frame.columns
    ]
    missing = frame.agg(*missing_exprs).first().asDict() if frame.columns else {}
    with st.expander("Missing values across this dataset", expanded=False):
        st.dataframe(
            [{"Column": name, "Missing values": int(value or 0)} for name, value in missing.items()],
            hide_index=True,
            use_container_width=True,
        )

    st.subheader("Sample rows")
    preview = frame.limit(100).toPandas()
    st.dataframe(preview, use_container_width=True, hide_index=True)
    export_rows = st.number_input("CSV export row limit", min_value=1, max_value=50_000, value=max(1, min(record_count, 5_000)))
    csv_bytes = frame.limit(int(export_rows)).toPandas().to_csv(index=False).encode("utf-8")
    st.download_button(
        "Download bounded CSV sample",
        data=csv_bytes,
        file_name=f"{selection.lower().replace(' ', '_')}_{int(export_rows)}.csv",
        mime="text/csv",
    )
