"""CSV schema checks, bounded previews, and raw Bronze ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from config.settings import BRONZE_PATH, OPTIONAL_COLUMNS, REQUIRED_COLUMNS
from src.storage.hdfs_storage import HdfsStorage
from utils.helpers import safe_filename


class DatasetValidationError(ValueError):
    pass


@dataclass(frozen=True)
class DatasetInspection:
    path: str
    file_size_bytes: int
    row_count: int
    columns: list[str]
    dtypes: dict[str, str]
    missing_values: dict[str, int]
    preview: list[dict[str, object]]


def validate_csv(path: str | Path) -> list[str]:
    source = Path(path)
    if not source.exists() or not source.is_file():
        raise DatasetValidationError(f"CSV file does not exist: {source}")
    if source.suffix.lower() != ".csv":
        raise DatasetValidationError("Unsupported input: upload a .csv file.")
    try:
        header = list(pd.read_csv(source, nrows=0, encoding="utf-8-sig").columns)
    except (UnicodeDecodeError, pd.errors.ParserError, OSError) as exc:
        raise DatasetValidationError(f"Could not read the CSV header: {exc}") from exc
    canonical = {str(column).strip().casefold(): str(column).strip() for column in header}
    missing = [name for name in REQUIRED_COLUMNS if name.casefold() not in canonical]
    if missing:
        raise DatasetValidationError("Missing required columns: " + ", ".join(missing))
    allowed = {column.casefold() for column in (*REQUIRED_COLUMNS, *OPTIONAL_COLUMNS)}
    unexpected = [column for column in header if str(column).strip().casefold() not in allowed]
    if unexpected:
        raise DatasetValidationError("Unsupported columns would be discarded: " + ", ".join(map(str, unexpected)))
    return header


def inspect_csv(path: str | Path, preview_rows: int = 100) -> DatasetInspection:
    source = Path(path)
    columns = validate_csv(source)
    try:
        preview_frame = pd.read_csv(source, nrows=preview_rows, encoding="utf-8-sig")
        counts = pd.read_csv(source, usecols=[columns[0]], dtype="string", encoding="utf-8-sig").shape[0]
    except (UnicodeDecodeError, pd.errors.ParserError, OSError, ValueError) as exc:
        raise DatasetValidationError(f"Could not inspect CSV rows: {exc}") from exc
    return DatasetInspection(
        path=str(source.resolve()),
        file_size_bytes=source.stat().st_size,
        row_count=int(counts),
        columns=[str(name) for name in preview_frame.columns],
        dtypes={str(name): str(dtype) for name, dtype in preview_frame.dtypes.items()},
        missing_values={str(name): int(value) for name, value in preview_frame.isna().sum().items()},
        preview=preview_frame.head(preview_rows).where(pd.notna(preview_frame), None).to_dict("records"),
    )


def ingest_to_bronze(
    path: str | Path,
    run_id: str,
    storage: HdfsStorage,
) -> dict[str, object]:
    source = Path(path)
    header = validate_csv(source)
    target = f"{BRONZE_PATH}/{run_id}/{safe_filename(source.name)}"
    uploaded_uri = storage.put_file(source, target)
    return {
        "source_name": safe_filename(source.name),
        "source_path": str(source.resolve()),
        "source_size_bytes": source.stat().st_size,
        "source_columns": header,
        "bronze_uri": uploaded_uri,
    }
