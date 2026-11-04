"""Smoke-check the Spark runtime and its embedded Hive catalog."""

from pathlib import Path

from pyspark.sql import SparkSession


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / ".runtime"
WAREHOUSE = RUNTIME / "metastore" / "warehouse"
WAREHOUSE.mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder.master("local[2]")
    .appName("spark-hive-infrastructure-check")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.warehouse.dir", WAREHOUSE.as_uri())
    .enableHiveSupport()
    .getOrCreate()
)

database_created = False
try:
    spark.sql("CREATE DATABASE IF NOT EXISTS infrastructure_check")
    database_created = True
    spark.sql("USE infrastructure_check")
    spark.sql("CREATE TABLE IF NOT EXISTS smoke_result USING PARQUET AS SELECT 2 + 3 AS result")
    actual = spark.sql("SELECT result FROM smoke_result").first()["result"]
    if actual != 5:
        raise RuntimeError(f"Hive catalog query returned {actual!r}, expected 5")
    print(f"Spark {spark.version}; Hive catalog query returned {actual}")
finally:
    if database_created:
        spark.sql("DROP DATABASE IF EXISTS infrastructure_check CASCADE")
    spark.stop()
