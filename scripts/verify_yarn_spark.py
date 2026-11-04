"""Run an end-to-end Spark/YARN/HDFS/Hive infrastructure check."""

from uuid import uuid4

from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.types import IntegerType, StructField, StructType


run_id = uuid4().hex[:12]
database = f"infra_yarn_check_{run_id}"
table = "sales_check"
output = f"hdfs://localhost:9000/tmp/spark-yarn-check/{run_id}"
schema = StructType(
    [StructField("order_number", IntegerType(), False), StructField("amount", IntegerType(), False)]
)

spark = (
    SparkSession.builder.appName("spark-yarn-infrastructure-check")
    .master("yarn")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .enableHiveSupport()
    .getOrCreate()
)

try:
    source = spark.createDataFrame([(1, 125), (2, 250), (3, 375)], schema)
    total = source.agg(spark_sum("amount").alias("total")).first()["total"]
    if total != 750:
        raise RuntimeError(f"Unexpected Spark aggregate: {total!r}")

    source.write.mode("errorifexists").parquet(output)
    spark.sql(f"CREATE DATABASE `{database}`")
    spark.sql(f"CREATE TABLE `{database}`.`{table}` USING PARQUET LOCATION '{output}'")
    result = spark.sql(
        f"SELECT COUNT(*) AS rows, SUM(amount) AS total FROM `{database}`.`{table}`"
    ).first()
    if result["rows"] != 3 or result["total"] != 750:
        raise RuntimeError(f"Unexpected Hive query result: {result}")

    print(f"Spark application: {spark.sparkContext.applicationId}")
    print(f"Master: {spark.sparkContext.master}")
    print(f"Hive query result: rows={result['rows']}, total={result['total']}")
    print(f"Parquet output: {output}")
finally:
    spark.sql(f"DROP DATABASE IF EXISTS `{database}` CASCADE")
    spark.stop()
