from pathlib import Path

import pytest

from src.ingestion.ingest import DatasetValidationError, inspect_csv, validate_csv


ROOT = Path(__file__).resolve().parents[1]


def test_starter_csv_inspection_counts_rows_and_preserves_schema():
    path = ROOT / "data/sample/ecommerce_transactions_test_1000.csv"
    info = inspect_csv(path, preview_rows=3)
    assert info.row_count == 1005
    assert info.columns[0] == "Order_ID"
    assert len(info.preview) == 3


def test_schema_validation_rejects_missing_required_column(tmp_path):
    path = tmp_path / "bad.csv"
    path.write_text("Order_ID,Product\n1,Book\n", encoding="utf-8")
    with pytest.raises(DatasetValidationError, match="Missing required columns"):
        validate_csv(path)


def test_schema_validation_rejects_unrecognized_columns(tmp_path):
    path = tmp_path / "extra.csv"
    path.write_text(
        "Order_ID,Customer_ID,Product,Category,Quantity,Unit_Price,Order_Date,Order_Status,Secret\n"
        "1,c1,Book,Books,1,10,2026-01-01,Delivered,x\n",
        encoding="utf-8",
    )
    with pytest.raises(DatasetValidationError, match="Unsupported columns"):
        validate_csv(path)
