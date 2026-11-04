"""Central settings for the local e-commerce analytics pipeline."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_ROOT = PROJECT_ROOT / "data"
RUNTIME_ROOT = Path(os.getenv("BIGDATA_ROOT", PROJECT_ROOT / ".runtime")).resolve()
LOG_ROOT = RUNTIME_ROOT / "logs"
PIPELINE_LOG_ROOT = LOG_ROOT / "pipeline_runs"

HDFS_URI = os.getenv("HDFS_URI", "hdfs://localhost:9000")
HDFS_ECOMMERCE_ROOT = os.getenv("HDFS_ECOMMERCE_ROOT", "/ecommerce")
BRONZE_PATH = f"{HDFS_ECOMMERCE_ROOT}/bronze/transactions"
SILVER_PATH = f"{HDFS_ECOMMERCE_ROOT}/silver/transactions"
REJECTED_PATH = f"{HDFS_ECOMMERCE_ROOT}/silver/rejected"
GOLD_PATH = f"{HDFS_ECOMMERCE_ROOT}/gold"
METADATA_PATH = f"{HDFS_ECOMMERCE_ROOT}/metadata"
HIVE_DATABASE = os.getenv("HIVE_DATABASE", "ecommerce_analytics")

EXPECTED_COLUMNS = (
    "Order_ID",
    "Customer_ID",
    "Customer_Name",
    "Product",
    "Category",
    "Quantity",
    "Unit_Price",
    "Order_Date",
    "Region",
    "Payment_Method",
    "Order_Status",
)
REQUIRED_COLUMNS = (
    "Order_ID",
    "Customer_ID",
    "Product",
    "Category",
    "Quantity",
    "Unit_Price",
    "Order_Date",
    "Order_Status",
)
OPTIONAL_COLUMNS = ("Customer_Name", "Region", "Payment_Method")


@dataclass(frozen=True)
class RuntimeSettings:
    project_root: Path = PROJECT_ROOT
    runtime_root: Path = RUNTIME_ROOT
    hdfs_uri: str = HDFS_URI
    hdfs_ecommerce_root: str = HDFS_ECOMMERCE_ROOT
    hive_database: str = HIVE_DATABASE
    spark_submit: str = os.getenv("SPARK_SUBMIT", str(PROJECT_ROOT / ".venv" / "bin" / "spark-submit"))
    spark_master: str = os.getenv("SPARK_MASTER", "yarn")
    spark_deploy_mode: str = os.getenv("SPARK_DEPLOY_MODE", "client")
    command_timeout_seconds: int = int(os.getenv("PIPELINE_TIMEOUT_SECONDS", "3600"))


SETTINGS = RuntimeSettings()


def hdfs_uri(path: str) -> str:
    """Join an HDFS path to the configured namenode URI."""
    return f"{HDFS_URI.rstrip('/')}/{path.lstrip('/')}"
