"""Unit tests for ingestion/dlt/pipeline.py -- no Docker, no network.

Covers sources.yml loading/dispatch and the sql connector's missing-driver
error message. Registry schema validation itself (pydantic field errors,
line numbers) is tests/test_registry.py; per-family extraction correctness
against real data is tests/test_connectors_*.py and
tests/integration/test_multi_engine.py (-m integration, needs Docker).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent


def _load_pipeline_module(name: str = "dlt_pipeline_test"):
    """Import ingestion/dlt/pipeline.py by path -- same technique as
    tests/test_scripts.py's `_load_script_module`, and for the same reason
    pipeline.py itself avoids `import ingestion.dlt.pipeline`: the parent
    `ingestion/__init__.py` imports submodules that don't exist on disk."""
    spec = importlib.util.spec_from_file_location(
        name, REPO_ROOT / "ingestion" / "dlt" / "pipeline.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def pipeline():
    return _load_pipeline_module()


class TestLoadSources:
    def test_real_sources_yml_parses(self, pipeline):
        sources = pipeline.load_sources()
        names = {s.name for s in sources}
        assert names == {
            "sqlite_chinook",
            "pg_chinook",
            "mysql_chinook",
            "mssql_chinook",
            "pg_chinook_customers_incremental",
            "disease_symptoms_csv",
            "sample_products_json",
            "jsonplaceholder",
            "open_brewery_db",
        }
        by_name = {s.name: s for s in sources}
        assert by_name["pg_chinook"].family == "sql"
        assert by_name["pg_chinook"].dialect == "postgresql"
        assert by_name["pg_chinook"].secret_ref == "CHINOOK_POSTGRES_URL"
        assert by_name["pg_chinook"].schema_name == "public"
        assert by_name["pg_chinook"].resources == []
        assert by_name["disease_symptoms_csv"].family == "files"
        assert by_name["jsonplaceholder"].family == "rest"
        assert by_name["jsonplaceholder"].network is True

    def test_get_source_unknown_raises(self, pipeline):
        with pytest.raises(pipeline.SourcesConfigError, match="no_such_source"):
            pipeline.get_source("no_such_source", sources=[])

    def test_get_source_resolves_by_name(self, pipeline):
        sources = pipeline.load_sources()
        source = pipeline.get_source("pg_chinook", sources=sources)
        assert source.name == "pg_chinook"


class TestPrefixedTableName:
    def test_prefix_format(self, pipeline):
        assert pipeline.prefixed_table_name("pg_chinook", "album") == "pg_chinook_album"


class TestExtractDispatch:
    """extract_source dispatches by family -- not re-testing each connector's
    own extraction logic here (see test_connectors_*.py), just that the
    dispatch table routes correctly and wraps unexpected errors."""

    def test_unknown_family_is_rejected_by_the_registry_itself(self, pipeline):
        # The registry's own pydantic Literal already rejects this at load
        # time -- dispatch never sees an invalid family.
        with pytest.raises(Exception):  # noqa: B017, PT011 - pydantic ValidationError
            pipeline.SourceConfig(
                name="x", family="ftp", connector="y", resources=["a"]
            )

    def test_files_connector_errors_are_wrapped(self, pipeline, tmp_path):
        source = pipeline.SourceConfig(
            name="missing_file",
            family="files",
            connector="local_file",
            path="does/not/exist.csv",
            format="csv",
        )
        with pytest.raises(pipeline.ConnectorError, match="missing_file"):
            pipeline.extract_source(source, work_dir=tmp_path)


class TestMissingDriver:
    def test_missing_driver_names_the_package(self, pipeline, monkeypatch):
        from connectors import sql as sql_connector

        monkeypatch.setattr(sql_connector, "find_spec", lambda name: None)
        with pytest.raises(sql_connector.MissingDriverError, match="psycopg2"):
            sql_connector.check_driver("postgresql")

    def test_sqlite_needs_no_driver_package(self, pipeline, monkeypatch):
        from connectors import sql as sql_connector

        # sqlite3 is stdlib -- check_driver must not even look it up.
        monkeypatch.setattr(
            sql_connector,
            "find_spec",
            lambda name: (_ for _ in ()).throw(AssertionError("should not be called")),
        )
        sql_connector.check_driver("sqlite")  # does not raise

    def test_installed_driver_passes(self, pipeline, monkeypatch):
        from connectors import sql as sql_connector

        monkeypatch.setattr(sql_connector, "find_spec", lambda name: object())
        sql_connector.check_driver("sqlite")
        sql_connector.check_driver("postgresql")
