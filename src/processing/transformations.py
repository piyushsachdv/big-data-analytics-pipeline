"""Simple, explainable Spark aggregations for the Gold layer."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_business_metrics(transactions: DataFrame) -> DataFrame:
    return transactions.withColumn(
        "delivered_revenue",
        F.when(F.col("order_status") == "Delivered", F.col("transaction_value"))
        .otherwise(F.lit(0).cast("decimal(20,2)")),
    )


def build_gold_tables(transactions: DataFrame) -> dict[str, DataFrame]:
    sales = add_business_metrics(transactions).withColumn("order_year", F.year("order_date")).withColumn(
        "order_month", F.month("order_date")
    )
    monthly = sales.groupBy("order_year", "order_month").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("transaction_value").alias("gross_transaction_value"),
        F.sum("delivered_revenue").alias("delivered_revenue"),
    )
    daily = sales.groupBy("order_date").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("delivered_revenue").alias("delivered_revenue"),
    )
    category = sales.groupBy("category").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("transaction_value").alias("gross_transaction_value"),
        F.sum("delivered_revenue").alias("delivered_revenue"),
        F.sum("quantity").alias("units_sold"),
    )
    region = sales.groupBy("region").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("delivered_revenue").alias("delivered_revenue"),
    )
    product = sales.groupBy("product", "category").agg(
        F.sum("quantity").alias("units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("delivered_revenue").alias("delivered_revenue"),
    )
    customer = sales.groupBy("customer_id", "customer_name", "region").agg(
        F.countDistinct("order_id").alias("orders_count"),
        F.sum("delivered_revenue").alias("delivered_revenue"),
        F.max("order_date").alias("last_order_date"),
    ).withColumn(
        "is_repeat_customer", F.col("orders_count") > 1
    )
    order_status = sales.groupBy("order_status").agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("transaction_value").alias("gross_transaction_value"),
        F.sum("delivered_revenue").alias("delivered_revenue"),
    )
    return {
        "gold_sales_by_month": monthly,
        "gold_sales_by_day": daily,
        "gold_sales_by_category": category,
        "gold_sales_by_region": region,
        "gold_product_performance": product,
        "gold_customer_summary": customer,
        "gold_order_status": order_status,
    }


def summary_metrics(transactions: DataFrame) -> dict[str, float | int]:
    sales = add_business_metrics(transactions)
    row = sales.agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("transaction_value").alias("gross_transaction_value"),
        F.sum("delivered_revenue").alias("delivered_revenue"),
        F.sum(F.when(F.col("order_status") == "Delivered", 1).otherwise(0)).alias("delivered_orders"),
        F.sum(F.when(F.col("order_status") == "Cancelled", 1).otherwise(0)).alias("cancelled_orders"),
        F.sum(F.when(F.col("order_status") == "Returned", 1).otherwise(0)).alias("returned_orders"),
        F.sum(F.when(F.col("order_status") == "Processing", 1).otherwise(0)).alias("processing_orders"),
        F.sum(F.when(F.col("order_status") == "Shipped", 1).otherwise(0)).alias("shipped_orders"),
    ).first().asDict()
    result: dict[str, float | int] = {}
    for key, value in row.items():
        result[key] = float(value) if key.endswith("value") or key.endswith("revenue") else int(value or 0)
    delivered_orders = int(result.get("delivered_orders", 0))
    delivered_revenue = float(result.get("delivered_revenue", 0.0))
    result["average_order_value"] = delivered_revenue / delivered_orders if delivered_orders else 0.0
    return result
