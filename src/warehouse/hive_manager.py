"""Create and refresh Spark-managed Hive catalog tables over HDFS Parquet paths."""

from __future__ import annotations

from pyspark.sql import SparkSession

from config.settings import HIVE_DATABASE


def quote_location(location: str) -> str:
    return location.replace("'", "''")


def register_table(spark: SparkSession, table_name: str, location: str, file_format: str = "PARQUET") -> None:
    if not table_name.replace("_", "").isalnum():
        raise ValueError(f"Invalid Hive table name: {table_name}")
    safe_location = quote_location(location)
    spark.sql(f"DROP TABLE IF EXISTS `{HIVE_DATABASE}`.`{table_name}`")
    spark.sql(
        f"CREATE TABLE `{HIVE_DATABASE}`.`{table_name}` "
        f"USING {file_format} LOCATION '{safe_location}'"
    )


def register_csv_table(spark: SparkSession, table_name: str, location: str) -> None:
    if not table_name.replace("_", "").isalnum():
        raise ValueError(f"Invalid Hive table name: {table_name}")
    safe_location = quote_location(location)
    spark.sql(f"DROP TABLE IF EXISTS `{HIVE_DATABASE}`.`{table_name}`")
    spark.sql(
        f"CREATE TABLE `{HIVE_DATABASE}`.`{table_name}` USING CSV "
        f"OPTIONS (header 'true', inferSchema 'false', mode 'PERMISSIVE') "
        f"LOCATION '{safe_location}'"
    )


def ensure_database(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{HIVE_DATABASE}`")
