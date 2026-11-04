"""Manual, measured single-node YARN benchmark runs."""

from __future__ import annotations

import json
import os
import platform
import time
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from config.settings import DATA_ROOT, METADATA_PATH, RUNTIME_ROOT, SETTINGS
from src.pipeline.orchestrator import EventCallback, run_pipeline
from src.storage.hdfs_storage import HdfsStorage
from src.warehouse.hive_manager import ensure_database, register_table
from src.warehouse.metastore_lock import metastore_writer
from utils.data_generator import generate_dataset
from utils.helpers import new_run_id


BENCHMARK_LOG = RUNTIME_ROOT / "logs" / "benchmark_results.jsonl"


def _memory_total_gib() -> float | None:
    try:
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                return round(int(line.split()[1]) / 1024 / 1024, 2)
    except (OSError, ValueError):
        return None
    return None


def _record_to_hive(record: dict[str, Any]) -> None:
    schema = StructType(
        [
            StructField("run_id", StringType(), False),
            StructField("dataset_rows", LongType(), False),
            StructField("repetition", IntegerType(), False),
            StructField("execution_mode", StringType(), False),
            StructField("ingestion_seconds", DoubleType(), False),
            StructField("cleaning_seconds", DoubleType(), False),
            StructField("transformation_seconds", DoubleType(), False),
            StructField("hive_registration_seconds", DoubleType(), False),
            StructField("total_seconds", DoubleType(), False),
            StructField("throughput_records_per_second", DoubleType(), False),
            StructField("output_storage_bytes", LongType(), False),
            StructField("spark_version", StringType(), False),
            StructField("python_version", StringType(), False),
            StructField("ram_gib", DoubleType(), True),
        ]
    )
    values = (
        record["run_id"], int(record["dataset_rows"]), int(record["repetition"]), record["execution_mode"],
        float(record["ingestion_seconds"]), float(record["cleaning_seconds"]),
        float(record["transformation_seconds"]), float(record["hive_registration_seconds"]),
        float(record["total_seconds"]), float(record["throughput_records_per_second"]),
        int(record["output_storage_bytes"]), record["spark_version"], record["python_version"], record["ram_gib"],
    )
    with metastore_writer():
        spark = (
            SparkSession.builder.master("local[1]")
            .appName("EcommerceBenchmarkCatalogWriter")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .enableHiveSupport()
            .getOrCreate()
        )
        try:
            path = f"{METADATA_PATH}/benchmark_results"
            spark.createDataFrame([values], schema).write.mode("append").parquet(path)
            ensure_database(spark)
            register_table(spark, "benchmark_results", path)
        finally:
            spark.stop()


def run_benchmark(
    rows: int,
    *,
    repetition: int = 1,
    seed: int = 20261104,
    callback: EventCallback | None = None,
    storage: HdfsStorage | None = None,
) -> dict[str, Any]:
    run_id = new_run_id()
    dataset_dir = DATA_ROOT / "benchmark"
    source = generate_dataset(
        rows,
        dataset_dir / f"ecommerce_{rows}_{run_id}.csv",
        seed=seed,
        inject_defects=True,
    )
    started = time.perf_counter()
    result = run_pipeline(source, storage=storage, callback=callback, run_id=run_id)
    total_seconds = round(time.perf_counter() - started, 3)
    paths = result["paths"]
    hdfs = storage or HdfsStorage(timeout_seconds=SETTINGS.command_timeout_seconds)
    sizes = [hdfs.size_bytes(paths[name]) for name in ("silver", "rejected")]
    sizes.extend(hdfs.size_bytes(location) for location in paths["gold"].values())
    output_bytes = sum(sizes)
    timings = result.get("timings", {})
    benchmark = {
        "run_id": run_id,
        "dataset_rows": int(rows),
        "repetition": int(repetition),
        "execution_mode": "YARN single-node pseudo-distributed",
        "started_at": result.get("started_at"),
        "ingestion_seconds": float(result.get("bronze", {}).get("upload_seconds", 0.0)),
        "cleaning_seconds": float(timings.get("cleaning_seconds", 0.0)),
        "transformation_seconds": float(timings.get("gold_aggregation_write_seconds", 0.0)),
        "hive_registration_seconds": float(timings.get("hive_registration_validation_seconds", 0.0)),
        "spark_execution_seconds": float(timings.get("spark_job_seconds", 0.0)),
        "total_seconds": total_seconds,
        "throughput_records_per_second": round(rows / total_seconds, 2) if total_seconds else 0.0,
        "output_storage_bytes": output_bytes,
        "spark_version": "4.2.1",
        "python_version": platform.python_version(),
        "ram_gib": _memory_total_gib(),
        "application_id": result.get("application_id"),
        "status": result.get("status"),
    }
    BENCHMARK_LOG.parent.mkdir(parents=True, exist_ok=True)
    with BENCHMARK_LOG.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(benchmark) + "\n")
    local_json = DATA_ROOT / "benchmark" / f"{run_id}.json"
    local_json.write_text(json.dumps(benchmark, indent=2), encoding="utf-8")
    hdfs.put_file(local_json, f"{METADATA_PATH}/benchmark_result_files/{run_id}.json")
    _record_to_hive(benchmark)
    return benchmark


def load_benchmarks() -> list[dict[str, Any]]:
    if not BENCHMARK_LOG.is_file():
        return []
    return [json.loads(line) for line in BENCHMARK_LOG.read_text(encoding="utf-8").splitlines() if line.strip()]
