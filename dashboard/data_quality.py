from __future__ import annotations

import plotly.express as px
import streamlit as st
from pyspark.sql import functions as F

from dashboard.components import PALETTE, empty_state, metric_row, page_header, style_figure
from src.analytics.queries import bounded_pandas, table_df
from src.monitoring.pipeline_logger import load_current_result


RULES = [
    ("Duplicate Order_ID", "Keep one deterministic best-valid row; quarantine repeated IDs."),
    ("Missing Customer_ID", "Reject the transaction because customer-level analysis needs an ID."),
    ("Missing Product or Category", "Reject the transaction; no product or category is inferred."),
    ("Invalid Quantity", "Reject values that are missing, non-integer, zero, or negative."),
    ("Invalid Unit_Price", "Reject values that are missing, non-numeric, zero, or negative."),
    ("Invalid Order_Date", "Parse supported ISO and day/month date formats; reject unparseable dates."),
    ("Category casing and whitespace", "Trim whitespace and map known category spellings to canonical names."),
    ("Missing optional values", "Use the documented Unknown label for optional name, region, or payment fields."),
    ("Revenue treatment", "Delivered revenue includes only Delivered transactions; other statuses remain separate."),
]


def render() -> None:
    page_header("Data Quality", "Measured input, accepted, and rejected records from the latest successful Spark run.")
    current = load_current_result()
    if not current or current.get("status") != "SUCCEEDED":
        empty_state("Run the pipeline to produce data quality measurements and rejected records.")
        return

    quality = current.get("quality", {})
    input_rows = int(quality.get("input_records", 0))
    valid = int(quality.get("valid_records", 0))
    rejected = int(quality.get("rejected_records", 0))
    valid_rate = valid / input_rows * 100 if input_rows else 0.0
    metric_row(
        [
            ("Input records", f"{input_rows:,}", None),
            ("Valid records", f"{valid:,}", f"{valid_rate:.1f}% of input"),
            ("Rejected records", f"{rejected:,}", None),
            ("Duplicates removed", f"{int(quality.get('duplicates_removed', 0)):,}", None),
        ]
    )

    rule_counts = [
        ("Missing Order_ID", quality.get("missing_order_id", 0)),
        ("Missing Customer_ID", quality.get("missing_customer_id", 0)),
        ("Missing Product", quality.get("missing_product", 0)),
        ("Missing Category", quality.get("missing_category", 0)),
        ("Invalid Quantity", quality.get("invalid_quantity", 0)),
        ("Invalid Price", quality.get("invalid_price", 0)),
        ("Invalid Date", quality.get("invalid_date", 0)),
        ("Invalid Order Status", quality.get("invalid_order_status", 0)),
    ]
    left, right = st.columns(2)
    with left:
        fig = px.bar(
            [{"Issue": name, "Rows": int(value or 0)} for name, value in rule_counts],
            x="Issue", y="Rows", title="Validation findings (rules can overlap)",
            color_discrete_sequence=[PALETTE["blue"]],
        )
        st.plotly_chart(style_figure(fig, height=380), use_container_width=True)
    with right:
        try:
            rejected_frame = table_df("rejected_transactions")
            reasons = bounded_pandas(
                rejected_frame.groupBy("rejection_reason").agg(F.count(F.lit(1)).alias("records"))
                .orderBy(F.col("records").desc()).limit(30),
                30,
            )
            fig = px.bar(reasons, x="records", y="rejection_reason", orientation="h", title="Rejected records by reason", color_discrete_sequence=[PALETTE["teal"]])
            st.plotly_chart(style_figure(fig, height=380), use_container_width=True)
        except Exception as exc:
            st.warning(f"Rejected-record detail is unavailable: {exc}")

    with st.expander("Cleaning rules", expanded=False):
        st.dataframe([{"Rule": name, "Treatment": treatment} for name, treatment in RULES], use_container_width=True, hide_index=True)
    st.subheader("Rejected-record preview")
    try:
        rejected_frame = table_df("rejected_transactions")
        preview = bounded_pandas(rejected_frame, 100)
        st.dataframe(preview, use_container_width=True, hide_index=True)
        csv_bytes = rejected_frame.limit(10_000).toPandas().to_csv(index=False).encode("utf-8")
        st.download_button("Download rejected-record sample", csv_bytes, file_name="rejected_records_sample.csv", mime="text/csv")
    except Exception as exc:
        st.warning(f"Could not load the rejected-record sample: {exc}")
