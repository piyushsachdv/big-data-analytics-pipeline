"""YARN Spark application: raw Bronze CSV to typed Silver and analytical Gold."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

from config.settings import GOLD_PATH, HIVE_DATABASE, METADATA_PATH, REJECTED_PATH, SILVER_PATH
from src.processing.cleaning import clean_transactions
from src.processing.transformations import build_gold_tables, summary_metrics
from src.warehouse.hive_manager import ensure_database, register_csv_table, register_table


def _write_json(path: str, payload: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_suffix(target.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    temporary.replace(target)


def run_job(bronze_uri: str, run_id: str, stats_file: str, source_name: str) -> dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    job_start = time.perf_counter()
    paths = {
        "silver": f"{SILVER_PATH}/{run_id}",
        "rejected": f"{REJECTED_PATH}/{run_id}",
        "gold_root": f"{GOLD_PATH}/runs/{run_id}",
        "metadata": f"{METADATA_PATH}/{run_id}",
    }
    spark = (
        SparkSession.builder.appName("EcommerceAnalyticsPipeline")
        .master("yarn")
        .enableHiveSupport()
        .getOrCreate()
    )
    timings: dict[str, float] = {}
    try:
        stage_start = time.perf_counter()
        source = (
            spark.read.option("header", True)
            .option("encoding", "UTF-8")
            .option("mode", "PERMISSIVE")
            .csv(bronze_uri)
        )
        cleaned, rejected, quality = clean_transactions(source)
        cleaned.persist(StorageLevel.MEMORY_AND_DISK)
        rejected.persist(StorageLevel.MEMORY_AND_DISK)
        timings["cleaning_seconds"] = round(time.perf_counter() - stage_start, 3)

        stage_start = time.perf_counter()
        silver_with_partitions = cleaned.withColumn("order_year", F.year("order_date")).withColumn(
            "order_month", F.month("order_date")
        )
        silver_with_partitions.write.mode("errorifexists").partitionBy("order_year", "order_month").parquet(
            paths["silver"]
        )
        rejected.write.mode("errorifexists").parquet(paths["rejected"])
        timings["silver_write_seconds"] = round(time.perf_counter() - stage_start, 3)

        stage_start = time.perf_counter()
        gold_frames = build_gold_tables(cleaned)
        gold_paths: dict[str, str] = {}
        for table_name, frame in gold_frames.items():
            target = f"{paths['gold_root']}/{table_name}"
            frame.write.mode("errorifexists").parquet(target)
            gold_paths[table_name] = target
        timings["gold_aggregation_write_seconds"] = round(time.perf_counter() - stage_start, 3)

        stage_start = time.perf_counter()
        ensure_database(spark)
        register_csv_table(spark, "bronze_transactions", bronze_uri)
        register_table(spark, "silver_transactions", paths["silver"])
        register_table(spark, "rejected_transactions", paths["rejected"])
        for table_name, location in gold_paths.items():
            register_table(spark, table_name, location)

        quality_row = {**quality, "run_id": run_id, "source_name": source_name}
        quality_frame = spark.createDataFrame([quality_row])
        quality_path = f"{paths['metadata']}/data_quality_metrics"
        quality_frame.write.mode("errorifexists").parquet(quality_path)
        register_table(spark, "data_quality_metrics", quality_path)

        totals = summary_metrics(cleaned)
        hive_total = spark.sql(
            f"SELECT SUM(delivered_revenue) AS revenue FROM `{HIVE_DATABASE}`.`gold_sales_by_month`"
        ).first()["revenue"]
        expected_revenue = float(totals["delivered_revenue"])
        actual_revenue = float(hive_total or 0.0)
        if abs(actual_revenue - expected_revenue) > 0.01:
            raise RuntimeError(
                f"Hive/Spark delivered revenue mismatch: {actual_revenue} != {expected_revenue}"
            )
        timings["hive_registration_validation_seconds"] = round(time.perf_counter() - stage_start, 3)

        application_id = spark.sparkContext.applicationId
        timings["spark_job_seconds"] = round(time.perf_counter() - job_start, 3)
        run_history_path = f"{METADATA_PATH}/pipeline_runs"
        run_history = spark.createDataFrame(
            [
                (
                    run_id,
                    application_id,
                    source_name,
                    started_at.isoformat(),
                    "SUCCEEDED",
                    int(quality["input_records"]),
                    int(quality["valid_records"]),
                    int(quality["rejected_records"]),
                    float(totals["delivered_revenue"]),
                    float(timings["spark_job_seconds"]),
                )
            ],
            "run_id STRING, application_id STRING, source_name STRING, started_at STRING, "
            "status STRING, input_records LONG, valid_records LONG, rejected_records LONG, "
            "delivered_revenue DOUBLE, spark_job_seconds DOUBLE",
        )
        run_history.write.mode("append").parquet(run_history_path)
        register_table(spark, "pipeline_runs", run_history_path)
        result = {
            "run_id": run_id,
            "source_name": source_name,
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "application_id": application_id,
            "master": spark.sparkContext.master,
            "status": "SUCCEEDED",
            "quality": quality,
            "summary": totals,
            "timings": timings,
            "paths": {
                **paths,
                "bronze": bronze_uri,
                "gold": gold_paths,
                "data_quality_metrics": quality_path,
            },
            "hive_validation": {
                "delivered_revenue_from_hive": actual_revenue,
                "matches_spark": True,
            },
        }
        _write_json(stats_file, result)
        print("PIPELINE_RESULT=" + json.dumps(result, default=str), flush=True)
        return result
    finally:
        try:
            cleaned.unpersist()
            rejected.unpersist()
        except UnboundLocalError:
            pass
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze-uri", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--stats-file", required=True)
    parser.add_argument("--source-name", required=True)
    args = parser.parse_args()
    run_job(args.bronze_uri, args.run_id, args.stats_file, args.source_name)


if __name__ == "__main__":
    main()
