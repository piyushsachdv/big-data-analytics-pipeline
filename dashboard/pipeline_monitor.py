from __future__ import annotations

from html import escape
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

from config.settings import DATA_ROOT, RUNTIME_ROOT
from dashboard.components import PALETTE, empty_state, page_header, status_pill, style_figure
from src.benchmarking import load_benchmarks, run_benchmark
from src.ingestion.ingest import DatasetValidationError, inspect_csv
from src.monitoring.pipeline_logger import load_current_result, load_events
from src.pipeline.orchestrator import PipelineExecutionError, run_pipeline
from utils.data_generator import ROWS, generate_dataset
from utils.helpers import new_run_id, safe_filename


SAMPLE_DATASETS = {
    "50,000-row demonstration dataset": DATA_ROOT / "raw" / "ecommerce_transactions_50000.csv",
    "1,000-row corrupted test fixture": DATA_ROOT / "sample" / "ecommerce_transactions_test_1000.csv",
    "1,000-row clean comparison fixture": DATA_ROOT / "sample" / "ecommerce_transactions_clean_1000.csv",
    "Hand-crafted edge cases": DATA_ROOT / "sample" / "ecommerce_edge_cases.csv",
}


def _store_upload(upload) -> Path:
    upload_dir = RUNTIME_ROOT / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    upload_id = new_run_id()
    target = upload_dir / f"{upload_id}_{safe_filename(upload.name)}"
    target.write_bytes(upload.getvalue())
    return target


def _stage_view(events: list[dict]) -> None:
    if not events:
        return
    st.dataframe(
        [
            {"Time (UTC)": event["timestamp"], "Stage": event["stage"], "State": event["state"], "Details": event["message"]}
            for event in events[-20:]
        ],
        use_container_width=True,
        hide_index=True,
    )


def _run_pipeline_button(source: Path) -> None:
    stage_area = st.empty()
    recent_events: list[dict] = []

    def callback(event: dict) -> None:
        recent_events.append(event)
        with stage_area.container():
            _stage_view(recent_events)

    try:
        with st.spinner("Pipeline is running. YARN and Spark status will appear as the application reports them."):
            result = run_pipeline(source, callback=callback)
        st.session_state["pipeline_result"] = result
        st.success(f"Pipeline completed. YARN application: {result['application_id']}")
    except PipelineExecutionError as exc:
        st.session_state["pipeline_result"] = exc.result
        _show_pipeline_failure(exc)


def _show_pipeline_failure(exc: PipelineExecutionError) -> None:
    result = exc.result
    log_path = Path(result["spark_log_path"]) if result.get("spark_log_path") else None
    log_text = ""
    if log_path and log_path.is_file():
        log_text = log_path.read_text(encoding="utf-8", errors="replace")
    technical = log_text or str(result.get("error") or exc)
    summary = _failure_summary(technical, str(result.get("error") or exc))
    failed_stage = str(result.get("failure_stage") or "unknown").replace("_", " ").title()
    st.error(f"Pipeline failed during **{failed_stage}**: {summary}")
    st.caption(f"YARN application: {result.get('application_id') or 'Not submitted'}")
    with st.expander("Technical traceback and Spark output", expanded=False):
        if log_text:
            st.caption(f"Full log is preserved at `{log_path}`. Showing the final 180 lines.")
            st.code("\n".join(log_text.splitlines()[-180:]), language="text")
        else:
            st.code(technical, language="text")


def _failure_summary(technical: str, fallback: str) -> str:
    if "ERROR XSDB6:" in technical:
        return "Spark could not open the embedded Derby metastore because another JVM already has it booted (Derby XSDB6)."
    if "Unable to instantiate org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient" in technical:
        cause = next((line.strip() for line in reversed(technical.splitlines()) if line.strip().startswith("Caused by:")), None)
        return f"Spark could not create a Hive metastore client. {cause or 'See the nested cause in the technical details.'}"
    return fallback.splitlines()[0] if fallback else "See the technical details for the failure."


def _show_latest(current: dict | None) -> None:
    recent = st.session_state.get("pipeline_result")
    if recent and recent.get("status") == "FAILED":
        current = recent
    if not current:
        empty_state("No pipeline run is recorded yet. Choose a dataset above and run the pipeline.")
        return
    st.subheader("Latest pipeline run")
    st.markdown(
        '<div class="run-meta">'
        f"<span>{status_pill(str(current.get('status', 'WARNING')))}</span>"
        f"<span><strong>Run</strong> <code>{escape(str(current.get('run_id', '—')))}</code></span>"
        f"<span><strong>YARN app</strong> <code>{escape(str(current.get('application_id') or 'Not submitted'))}</code></span>"
        "</div>",
        unsafe_allow_html=True,
    )
    cols = st.columns(2)
    cols[0].metric("Input records", f"{int(current.get('quality', {}).get('input_records', 0) or 0):,}")
    cols[1].metric("Valid records", f"{int(current.get('quality', {}).get('valid_records', 0) or 0):,}")
    if current.get("status") == "SUCCEEDED":
        st.caption(f"Source `{current.get('source_name')}` · HDFS Silver `{current.get('paths', {}).get('silver')}`")
        st.write("**Stage timings (seconds)**")
        st.json(current.get("timings", {}), expanded=False)
    if current.get("run_id"):
        with st.expander("Stage history", expanded=True):
            _stage_view(load_events(current["run_id"]))
    log_path = current.get("spark_log_path")
    if log_path and Path(log_path).is_file():
        with st.expander("Latest Spark/YARN log", expanded=False):
            st.code("\n".join(Path(log_path).read_text(encoding="utf-8", errors="replace").splitlines()[-50:]))


def _benchmark_panel() -> None:
    st.divider()
    st.subheader("Manual YARN benchmark")
    st.caption("Runs one selected size only when you click the button. Each run records measured durations and HDFS output size.")
    left, right = st.columns([1, 2])
    rows = left.selectbox("Synthetic row count", ROWS, index=0, format_func=lambda n: f"{n:,} rows")
    repetition = right.number_input("Repetition label", min_value=1, max_value=20, value=1, step=1)
    if st.button("Run selected benchmark", type="secondary"):
        with st.spinner(f"Generating {rows:,} deterministic rows and running the YARN pipeline…"):
            try:
                record = run_benchmark(int(rows), repetition=int(repetition))
                st.success(
                    f"Measured {record['dataset_rows']:,} rows in {record['total_seconds']:.2f} seconds "
                    f"({record['throughput_records_per_second']:.1f} records/second)."
                )
            except Exception as exc:
                st.error(f"Benchmark failed: {exc}")

    records = load_benchmarks()
    if not records:
        return
    frame = pd.DataFrame(records)
    left, right = st.columns(2)
    with left:
        fig = px.line(frame, x="dataset_rows", y="total_seconds", markers=True, title="Dataset size vs. total execution time", color_discrete_sequence=[PALETTE["blue"]])
        st.plotly_chart(style_figure(fig), use_container_width=True)
    with right:
        fig = px.line(frame, x="dataset_rows", y="throughput_records_per_second", markers=True, title="Dataset size vs. throughput", color_discrete_sequence=[PALETTE["teal"]])
        st.plotly_chart(style_figure(fig), use_container_width=True)
    st.dataframe(frame, use_container_width=True, hide_index=True)


def render() -> None:
    page_header("Pipeline Monitor", "Select, inspect, and run a CSV through HDFS Bronze, Spark on YARN, Silver/Gold, and Hive.")
    upload = st.file_uploader("Upload an e-commerce CSV", type=["csv"], help="Required business columns are checked before any HDFS write.")
    uploaded_path: Path | None = None
    if upload is not None:
        try:
            uploaded_path = _store_upload(upload)
            inspection = inspect_csv(uploaded_path, preview_rows=20)
            st.success(f"CSV schema accepted · {inspection.row_count:,} rows · {inspection.file_size_bytes:,} bytes")
            st.dataframe(pd.DataFrame(inspection.preview), use_container_width=True, hide_index=True)
        except DatasetValidationError as exc:
            st.error(str(exc))

    source_choice = st.selectbox("Or choose a project dataset", [*SAMPLE_DATASETS, "Generate synthetic dataset"])
    source_path: Path | None = uploaded_path
    if upload is None and source_choice in SAMPLE_DATASETS:
        source_path = SAMPLE_DATASETS[source_choice]
        if source_path.exists():
            try:
                inspection = inspect_csv(source_path, preview_rows=10)
                st.caption(f"{inspection.row_count:,} records · {inspection.file_size_bytes:,} bytes · {len(inspection.columns)} columns")
                st.dataframe(pd.DataFrame(inspection.preview), use_container_width=True, hide_index=True)
            except DatasetValidationError as exc:
                st.error(str(exc))
                source_path = None
        else:
            st.error(f"Expected sample dataset is missing: {source_path}")
            source_path = None
    elif upload is None and source_choice == "Generate synthetic dataset":
        rows = st.selectbox("Synthetic dataset size", ROWS, format_func=lambda n: f"{n:,} rows")
        if st.button("Generate deterministic CSV"):
            destination = DATA_ROOT / "benchmark" / f"synthetic_{int(rows)}_{new_run_id()}.csv"
            with st.spinner(f"Generating {int(rows):,} fictional transactions…"):
                source_path = generate_dataset(int(rows), destination)
            st.session_state["generated_source"] = str(source_path)
        if st.session_state.get("generated_source"):
            source_path = Path(st.session_state["generated_source"])
            if source_path.exists():
                st.success(f"Generated dataset ready: {source_path.name}")

    if source_path is not None and source_path.exists():
        if st.button("Run Pipeline", type="primary"):
            _run_pipeline_button(source_path)
    elif upload is not None:
        st.info("Fix the CSV schema issues above before submitting it.")

    _show_latest(load_current_result())
    _benchmark_panel()
