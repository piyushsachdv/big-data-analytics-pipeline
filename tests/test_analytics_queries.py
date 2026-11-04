from types import SimpleNamespace

import pytest

from src.analytics import queries


def test_dashboard_session_uses_in_memory_catalog_builder(monkeypatch):
    calls = []

    class Builder:
        def master(self, value):
            calls.append(("master", value))
            return self

        def appName(self, value):
            return self

        def config(self, name, value):
            calls.append((name, value))
            return self

        def getOrCreate(self):
            return SimpleNamespace(sparkContext=SimpleNamespace(setLogLevel=lambda _level: None))

    monkeypatch.setattr(queries, "SparkSession", SimpleNamespace(builder=Builder()))
    queries.get_local_spark.cache_clear()
    try:
        queries.get_local_spark()
    finally:
        queries.get_local_spark.cache_clear()
    assert ("spark.sql.catalogImplementation", "in-memory") in calls
    assert not any(call[0] == "enableHiveSupport" for call in calls if isinstance(call, tuple))


def test_table_df_reads_the_latest_successful_run_path(monkeypatch):
    result = {
        "status": "SUCCEEDED",
        "paths": {"gold": {"gold_sales_by_month": "/gold/run-a/monthly"}},
    }
    calls = []

    class Reader:
        def parquet(self, path):
            calls.append(path)
            return "monthly dataframe"

    monkeypatch.setattr(queries, "load_current_result", lambda: result)
    monkeypatch.setattr(queries, "get_local_spark", lambda: SimpleNamespace(read=Reader()))
    assert queries.table_df("gold_sales_by_month") == "monthly dataframe"
    assert calls == ["/gold/run-a/monthly"]


def test_table_exists_checks_hdfs_path_without_catalog(monkeypatch):
    result = {"status": "SUCCEEDED", "paths": {"silver": "/silver/run-a"}}
    calls = []

    class Path:
        def __init__(self, value):
            calls.append(("path", value))

        def getFileSystem(self, _conf):
            return self

        def exists(self, _path):
            calls.append(("exists", True))
            return True

    jvm = SimpleNamespace(org=SimpleNamespace(apache=SimpleNamespace(hadoop=SimpleNamespace(fs=SimpleNamespace(Path=Path)))))
    context = SimpleNamespace(_jvm=jvm, _jsc=SimpleNamespace(hadoopConfiguration=lambda: object()))
    monkeypatch.setattr(queries, "load_current_result", lambda: result)
    monkeypatch.setattr(queries, "get_local_spark", lambda: SimpleNamespace(sparkContext=context))
    assert queries.table_exists("silver_transactions") is True
    assert calls == [("path", "/silver/run-a"), ("exists", True)]


def test_table_exists_rejects_invalid_identifier(monkeypatch):
    monkeypatch.setattr(queries, "get_local_spark", lambda: pytest.fail("Spark should not be opened"))
    with pytest.raises(ValueError, match="Invalid table name"):
        queries.table_exists("sales; DROP TABLE x")


def test_table_paths_require_latest_successful_run(monkeypatch):
    monkeypatch.setattr(queries, "load_current_result", lambda: {"status": "FAILED", "paths": {"gold": {"gold_sales_by_month": "/gold/partial"}}})
    assert queries._table_location("gold_sales_by_month") is None


def test_derby_failure_summary_keeps_root_cause_concise():
    from dashboard.pipeline_monitor import _failure_summary

    summary = _failure_summary(
        "Unable to instantiate SessionHiveMetaStoreClient\nCaused by: ERROR XSDB6: Another instance of Derby may have already booted the database metastore_db",
        "raw stack",
    )
    assert "another JVM" in summary
    assert "XSDB6" in summary
