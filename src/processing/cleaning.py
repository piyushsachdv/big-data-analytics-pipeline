"""Spark DataFrame validation and deterministic transaction cleaning."""

from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from config.settings import EXPECTED_COLUMNS, OPTIONAL_COLUMNS, REQUIRED_COLUMNS


CATEGORY_NAMES = {
    "electronics": "Electronics",
    "fashion": "Fashion",
    "home and kitchen": "Home and Kitchen",
    "sports": "Sports",
    "beauty": "Beauty",
    "books": "Books",
}
ORDER_STATUSES = ("Delivered", "Processing", "Shipped", "Cancelled", "Returned")
PAYMENT_NAMES = {
    "upi": "UPI",
    "credit card": "Credit Card",
    "debit card": "Debit Card",
    "net banking": "Net Banking",
    "cash on delivery": "Cash on Delivery",
}


def _clean_text(column: str):
    return F.trim(F.regexp_replace(F.col(column).cast("string"), r"\s+", " "))


def _canonical(column: str, values: dict[str, str]):
    normalized = F.lower(_clean_text(column))
    expression = F.lit(None).cast("string")
    for source, target in values.items():
        expression = F.when(normalized == source, F.lit(target)).otherwise(expression)
    return F.coalesce(expression, F.initcap(normalized))


def normalize_source_columns(frame: DataFrame) -> DataFrame:
    """Map case-insensitive CSV headers to the known business schema."""
    source_names = {str(name).strip().casefold(): str(name) for name in frame.columns}
    missing = [name for name in REQUIRED_COLUMNS if name.casefold() not in source_names]
    if missing:
        raise ValueError("Spark input is missing required columns: " + ", ".join(missing))

    expressions = []
    for name in EXPECTED_COLUMNS:
        source = source_names.get(name.casefold())
        expressions.append(
            _clean_text(source).alias(name.casefold())
            if source is not None
            else F.lit(None).cast("string").alias(name.casefold())
        )
    return frame.select(*expressions)


def clean_transactions(frame: DataFrame) -> tuple[DataFrame, DataFrame, dict[str, Any]]:
    """Return typed valid records, quarantined records, and measured quality counts."""
    raw = normalize_source_columns(frame)
    raw_columns = list(raw.columns)
    parsed_date = F.coalesce(
        *[
            F.try_to_timestamp(F.col("order_date"), F.lit(pattern)).cast("date")
            for pattern in ("yyyy-MM-dd", "dd/MM/yyyy", "MM/dd/yyyy", "dd-MM-yyyy", "d/M/yyyy")
        ]
    )
    candidates = (
        raw.withColumn("order_id", _clean_text("order_id"))
        .withColumn("customer_id", _clean_text("customer_id"))
        .withColumn("customer_name", _clean_text("customer_name"))
        .withColumn("product", _clean_text("product"))
        .withColumn("category", _canonical("category", CATEGORY_NAMES))
        .withColumn("quantity_value", F.expr("try_cast(quantity AS INT)"))
        .withColumn("price_value", F.expr("try_cast(unit_price AS DECIMAL(18,2))"))
        .withColumn("order_date_value", parsed_date)
        .withColumn("region_value", F.initcap(F.lower(_clean_text("region"))))
        .withColumn("payment_value", _canonical("payment_method", PAYMENT_NAMES))
        .withColumn("status_value", _canonical("order_status", {s.casefold(): s for s in ORDER_STATUSES}))
    )

    invalid_rules = {
        "missing_order_id": F.col("order_id").isNull() | (F.col("order_id") == ""),
        "missing_customer_id": F.col("customer_id").isNull() | (F.col("customer_id") == ""),
        "missing_product": F.col("product").isNull() | (F.col("product") == ""),
        "missing_category": F.col("category").isNull() | (F.col("category") == ""),
        "invalid_quantity": F.col("quantity_value").isNull() | (F.col("quantity_value") <= 0),
        "invalid_price": F.col("price_value").isNull() | (F.col("price_value") <= 0),
        "invalid_date": F.col("order_date_value").isNull(),
        "invalid_order_status": ~F.col("status_value").isin(*ORDER_STATUSES),
    }
    invalid_count = sum(F.when(rule, 1).otherwise(0) for rule in invalid_rules.values())
    reasons = F.concat_ws(
        "; ",
        *[F.when(rule, F.lit(reason)) for reason, rule in invalid_rules.items()],
    )
    candidates = candidates.withColumn("_invalid_count", invalid_count).withColumn("_reasons", reasons)
    candidates = candidates.withColumn(
        "_source_hash",
        F.sha2(F.to_json(F.struct(*[F.col(name) for name in raw_columns])), 256),
    ).withColumn(
        "_dedupe_key",
        F.when(F.col("order_id").isNull() | (F.col("order_id") == ""), F.col("_source_hash"))
        .otherwise(F.col("order_id")),
    )
    dedupe_window = Window.partitionBy("_dedupe_key").orderBy(
        F.col("_invalid_count").asc(), F.col("_source_hash").asc()
    )
    candidates = candidates.withColumn("_duplicate_rank", F.row_number().over(dedupe_window))
    duplicates = F.col("_duplicate_rank") > 1
    rejected = candidates.filter(duplicates | (F.col("_reasons") != ""))
    rejected = rejected.select(
        *[F.col(name) for name in raw_columns],
        F.when(duplicates, F.concat_ws("; ", F.col("_reasons"), F.lit("duplicate_order_id")))
        .otherwise(F.col("_reasons"))
        .alias("rejection_reason"),
    )

    valid = candidates.filter((F.col("_duplicate_rank") == 1) & (F.col("_reasons") == ""))
    cleaned = valid.select(
        "order_id",
        "customer_id",
        F.when(F.col("customer_name").isNull() | (F.col("customer_name") == ""), "Unknown")
        .otherwise(F.col("customer_name"))
        .alias("customer_name"),
        "product",
        "category",
        F.col("quantity_value").alias("quantity"),
        F.col("price_value").alias("unit_price"),
        F.col("order_date_value").alias("order_date"),
        F.when(F.col("region_value").isNull() | (F.col("region_value") == ""), "Unknown")
        .otherwise(F.col("region_value"))
        .alias("region"),
        F.when(F.col("payment_value").isNull() | (F.col("payment_value") == ""), "Unknown")
        .otherwise(F.col("payment_value"))
        .alias("payment_method"),
        F.col("status_value").alias("order_status"),
    ).withColumn("transaction_value", (F.col("quantity") * F.col("unit_price")).cast("decimal(20,2)"))

    metric_exprs = [F.count(F.lit(1)).alias("input_records")]
    for name, condition in invalid_rules.items():
        metric_exprs.append(F.sum(F.when(condition, 1).otherwise(0)).alias(name))
    raw_metrics = candidates.agg(*metric_exprs).first().asDict()
    duplicate_count = candidates.filter(duplicates).count()
    valid_count = cleaned.count()
    rejected_count = rejected.count()
    metrics: dict[str, Any] = {
        "input_records": int(raw_metrics.get("input_records") or 0),
        "valid_records": int(valid_count),
        "rejected_records": int(rejected_count),
        "duplicates_removed": int(duplicate_count),
    }
    metrics.update({key: int(raw_metrics.get(key) or 0) for key in invalid_rules})
    return cleaned, rejected, metrics
