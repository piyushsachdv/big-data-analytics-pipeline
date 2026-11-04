from __future__ import annotations

import os

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = (
        SparkSession.builder.master("local[2]")
        .appName("EcommerceProjectTests")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", "file:/tmp/ecommerce-test-warehouse")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
