from pathlib import Path

from pyspark.sql import functions as F

from src.processing.cleaning import clean_transactions


ROOT = Path(__file__).resolve().parents[1]


def test_edge_cases_are_normalized_quarantined_and_deduplicated(spark):
    source = spark.read.option("header", True).csv(str(ROOT / "data/sample/ecommerce_edge_cases.csv"))
    valid, rejected, metrics = clean_transactions(source)

    assert metrics["input_records"] == 11
    assert metrics["valid_records"] == 6
    assert metrics["duplicates_removed"] == 1
    assert metrics["rejected_records"] == 5
    normalized = valid.filter(F.col("order_id") == "ORD-EDGE-007").first()
    assert normalized.category == "Beauty"
    assert normalized.region == "South"
    assert normalized.order_date.isoformat() == "2026-01-06"
    assert rejected.filter(F.col("rejection_reason").contains("invalid_quantity")).count() == 1


def test_clean_fixture_has_no_rejections(spark):
    source = spark.read.option("header", True).csv(str(ROOT / "data/sample/ecommerce_transactions_clean_1000.csv"))
    valid, rejected, metrics = clean_transactions(source)
    assert metrics["input_records"] == 1000
    assert metrics["valid_records"] == 1000
    assert metrics["rejected_records"] == 0
    assert rejected.count() == 0
    assert valid.select("order_id").distinct().count() == 1000
