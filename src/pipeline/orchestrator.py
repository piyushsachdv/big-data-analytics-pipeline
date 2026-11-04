"""Validate, upload, submit, monitor, and verify one complete pipeline run."""

from __future__ import annotations

import json
import os
import queue
import re
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any, Callable

from config.settings import HIVE_DATABASE, PIPELINE_LOG_ROOT, PROJECT_ROOT, SETTINGS
from src.ingestion.ingest import ingest_to_bronze, validate_csv
from src.monitoring.pipeline_logger import append_event, run_directory, save_run_result
from src.monitoring.yarn_monitor import YarnMonitorError, application_status
from src.storage.hdfs_storage import HdfsError, HdfsStorage
from src.warehouse.metastore_lock import metastore_writer
from utils.helpers import new_run_id


class PipelineExecutionError(RuntimeError):
    def __init__(self, message: str, result: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.result = result or {}


EventCallback = Callable[[dict[str, Any]], None]


def _runtime_environment() -> dict[str, str]:
    env = os.environ.copy()
    runtime = SETTINGS.runtime_root
    spark_home = Path(
        subprocess.check_output(
            [sys.executable, "-c", "import pyspark; print(pyspark.__path__[0])"],
            text=True,
        ).strip()
    )
    hadoop_home = runtime / "hadoop"
    env.update(
        {
            "JAVA_HOME": env.get("JAVA_HOME", "/usr/lib/jvm/java-17-openjdk-amd64"),
            "HADOOP_HOME": str(hadoop_home),
            "HADOOP_CONF_DIR": str(hadoop_home / "etc" / "hadoop"),
            "YARN_CONF_DIR": str(hadoop_home / "etc" / "hadoop"),
            "SPARK_HOME": str(spark_home),
            "HIVE_CONF_DIR": str(spark_home / "conf"),
            "SPARK_LOCAL_IP": env.get("SPARK_LOCAL_IP", "127.0.0.1"),
            "PYSPARK_PYTHON": env.get("PYSPARK_PYTHON", str(PROJECT_ROOT / ".venv" / "bin" / "python")),
        }
    )
    env["PATH"] = os.pathsep.join(
        [str(hadoop_home / "bin"), str(hadoop_home / "sbin"), str(spark_home / "bin"), env.get("PATH", "")]
    )
    return env


def _read_yarn_status(application_id: str) -> dict[str, Any] | None:
    try:
        return application_status(application_id, timeout_seconds=10)
    except (YarnMonitorError, OSError, subprocess.TimeoutExpired):
        return None


def run_pipeline(
    source_path: str | Path,
    *,
    storage: HdfsStorage | None = None,
    callback: EventCallback | None = None,
    run_id: str | None = None,
    timeout_seconds: int | None = None,
) -> dict[str, Any]:
    source = Path(source_path).resolve()
    run_id = run_id or new_run_id()
    timeout = timeout_seconds or SETTINGS.command_timeout_seconds
    hdfs = storage or HdfsStorage(timeout_seconds=timeout)
    events: list[dict[str, Any]] = []
    result: dict[str, Any] = {
        "run_id": run_id,
        "source_name": source.name,
        "status": "RUNNING",
        "application_id": None,
        "stages": [],
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    }
    failure_stage = "validation"

    def emit(stage: str, state: str, message: str, **details: Any) -> None:
        event = append_event(run_id, stage, state, message, **details)
        events.append(event)
        result["stages"] = events
        if callback:
            callback(event)

    try:
        emit("validation", "RUNNING", "Checking CSV schema and format.")
        validate_csv(source)
        emit("validation", "SUCCEEDED", "CSV has all required columns and supported headers.")

        failure_stage = "bronze_ingestion"
        emit("bronze_ingestion", "RUNNING", "Copying the original CSV to HDFS Bronze.")
        ingest_start = time.perf_counter()
        bronze = ingest_to_bronze(source, run_id, hdfs)
        bronze["upload_seconds"] = round(time.perf_counter() - ingest_start, 3)
        emit("bronze_ingestion", "SUCCEEDED", "Raw CSV is present in HDFS Bronze.", **bronze)

        failure_stage = "spark_yarn_processing"
        run_dir = run_directory(run_id)
        stats_path = run_dir / "spark_stats.json"
        log_path = run_dir / "spark_submit.log"
        command = [
            SETTINGS.spark_submit,
            "--master",
            "yarn",
            "--deploy-mode",
            SETTINGS.spark_deploy_mode,
            "--name",
            "EcommerceAnalyticsPipeline",
            str(PROJECT_ROOT / "src" / "processing" / "spark_engine.py"),
            "--bronze-uri",
            str(bronze["bronze_uri"]),
            "--run-id",
            run_id,
            "--stats-file",
            str(stats_path),
            "--source-name",
            str(bronze["source_name"]),
        ]
        emit("spark_yarn_processing", "SUBMITTED", "Submitting the Spark application to YARN.", command=command)
        start = time.monotonic()
        output_queue: queue.Queue[str | None] = queue.Queue()
        app_id_match = re.compile(r"application_\d+_\d+")
        lines: list[str] = []
        app_id: str | None = None
        last_yarn_poll = 0.0
        last_state = "SUBMITTED"
        yarn_state: dict[str, Any] | None = None

        # Only one Spark driver may open the embedded Derby catalog at a time.
        # Keep the OS lock for the complete driver lifetime; it is released by
        # the kernel even if the Streamlit process exits unexpectedly.
        with metastore_writer(), log_path.open("w", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                command,
                cwd=PROJECT_ROOT,
                env=_runtime_environment(),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )

            def copy_output() -> None:
                assert process.stdout is not None
                for output_line in process.stdout:
                    output_queue.put(output_line)
                output_queue.put(None)

            reader = threading.Thread(target=copy_output, name=f"spark-log-{run_id}", daemon=True)
            reader.start()
            stream_finished = False
            while process.poll() is None or not stream_finished:
                if time.monotonic() - start > timeout:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    raise TimeoutError(f"Spark/YARN job exceeded {timeout} seconds")
                try:
                    line = output_queue.get(timeout=1)
                except queue.Empty:
                    line = ""
                if line is None:
                    stream_finished = True
                elif line:
                    log_file.write(line)
                    log_file.flush()
                    lines.append(line.rstrip())
                    if len(lines) > 400:
                        lines.pop(0)
                    if app_id is None:
                        match = app_id_match.search(line)
                        if match:
                            app_id = match.group(0)
                            result["application_id"] = app_id
                    if app_id and time.monotonic() - last_yarn_poll >= 3:
                        yarn_state = _read_yarn_status(app_id)
                        last_yarn_poll = time.monotonic()
                        if yarn_state:
                            actual_state = str(yarn_state.get("state", "")).upper()
                            if actual_state in {"ACCEPTED", "RUNNING"} and actual_state != last_state:
                                last_state = actual_state
                                emit(
                                    "spark_yarn_processing",
                                    actual_state,
                                    f"YARN application state is {actual_state}.",
                                    application_id=app_id,
                                    progress=yarn_state.get("progress"),
                                    tracking_url=yarn_state.get("tracking_url"),
                                )
                elif process.poll() is not None and output_queue.empty():
                    stream_finished = True
            return_code = process.wait()

        result["spark_log_path"] = str(log_path)
        if app_id:
            yarn_state = _read_yarn_status(app_id) or yarn_state
            result["yarn_status"] = yarn_state
        if return_code != 0:
            detail = "\n".join(lines[-40:])
            raise PipelineExecutionError(f"spark-submit exited with {return_code}:\n{detail}", result)
        if not app_id:
            raise PipelineExecutionError("spark-submit returned success without a YARN application ID.", result)
        if not yarn_state or yarn_state.get("state") != "FINISHED" or yarn_state.get("final_state") != "SUCCEEDED":
            raise PipelineExecutionError(f"YARN did not report a successful final state: {yarn_state}", result)
        if not stats_path.is_file():
            raise PipelineExecutionError("Spark exited successfully but did not write its execution summary.", result)
        spark_result = json.loads(stats_path.read_text(encoding="utf-8"))
        if spark_result.get("status") != "SUCCEEDED" or not spark_result.get("hive_validation", {}).get("matches_spark"):
            raise PipelineExecutionError("Spark/Hive output validation did not report success.", result)
        emit(
            "spark_yarn_processing",
            "SUCCEEDED",
            "Spark completed successfully on YARN.",
            application_id=app_id,
            final_state=yarn_state.get("final_state"),
            spark_job_seconds=spark_result.get("timings", {}).get("spark_job_seconds"),
        )

        failure_stage = "output_verification"
        emit("output_verification", "RUNNING", "Checking Silver, rejected, Gold, and Hive locations.")
        paths = spark_result["paths"]
        for key in ("silver", "rejected", "data_quality_metrics"):
            if not hdfs.exists(paths[key]):
                raise HdfsError(f"Expected HDFS {key} output is missing: {paths[key]}")
        missing_gold = [name for name, path in paths["gold"].items() if not hdfs.exists(path)]
        if missing_gold:
            raise HdfsError("Expected Gold outputs are missing: " + ", ".join(missing_gold))
        emit("hive_analytics", "SUCCEEDED", "Spark verified the Hive table revenue against its aggregation.", **spark_result["hive_validation"])
        emit("output_verification", "SUCCEEDED", "All expected HDFS output locations exist.", paths=paths)

        result.update(spark_result)
        result.update({"bronze": bronze, "events": events, "status": "SUCCEEDED", "failure_stage": None})
        result["yarn_status"] = yarn_state
        save_run_result(run_id, result)
        return result
    except Exception as exc:
        result["status"] = "FAILED"
        result["failure_stage"] = failure_stage
        result["error"] = str(exc)
        emit(failure_stage, "FAILED", str(exc))
        save_run_result(run_id, result, current=False)
        if isinstance(exc, PipelineExecutionError):
            exc.result = result
            raise
        raise PipelineExecutionError(f"Pipeline failed during {failure_stage}: {exc}", result) from exc
