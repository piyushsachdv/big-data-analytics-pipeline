from datetime import date
from decimal import Decimal

from pyspark.sql.types import DateType, DecimalType, IntegerType, StringType, StructField, StructType

from src.processing.transformations import build_gold_tables, summary_metrics


def test_gold_aggregations_and_delivered_revenue(spark):
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("product", StringType(), False),
        StructField("category", StringType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("unit_price", DecimalType(18, 2), False),
        StructField("order_date", DateType(), False),
        StructField("region", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("order_status", StringType(), False),
        StructField("transaction_value", DecimalType(20, 2), False),
    ])
    rows = [
        ("o1", "c1", "A", "Book", "Books", 2, Decimal("10.00"), date(2026, 1, 2), "North", "UPI", "Delivered", Decimal("20.00")),
        ("o2", "c2", "B", "Book", "Books", 1, Decimal("20.00"), date(2026, 1, 2), "South", "UPI", "Cancelled", Decimal("20.00")),
    ]
    frame = spark.createDataFrame(rows, schema)
    metrics = summary_metrics(frame)
    assert metrics["total_orders"] == 2
    assert metrics["delivered_revenue"] == 20.0
    assert metrics["gross_transaction_value"] == 40.0
    assert metrics["average_order_value"] == 20.0
    gold = build_gold_tables(frame)["gold_sales_by_category"].first()
    assert gold.total_orders == 2
    assert float(gold.delivered_revenue) == 20.0
    assert float(gold.gross_transaction_value) == 40.0
