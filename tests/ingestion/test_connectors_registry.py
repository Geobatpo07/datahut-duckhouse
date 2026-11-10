"""Unit tests for ``ingestion.connectors.registry``'s family -> connector
dispatch, and the ``sql`` connector's missing-driver error message.
Per-family extraction correctness against real data is
``tests/ingestion/test_connectors_*.py`` and
``tests/integration/test_multi_engine.py`` (-m integration, needs Docker).
"""

from __future__ import annotations

from importlib.util import find_spec

import pytest

from ingestion import extract_source
from ingestion.config.models import SourceConfig
from ingestion.connectors import registry as connector_registry


class TestExtractDispatch:
    """``ingestion.extract_source`` dispatches by family -- not re-testing
    each connector's own extraction logic here, just that the dispatch
    routes correctly and wraps unexpected errors."""

    def test_unknown_family_is_rejected_by_the_registry_itself(self):
        # The registry's own pydantic Literal already rejects this at load
        # time -- dispatch never sees an invalid family.
        with pytest.raises(Exception):  # noqa: B017, PT011 - pydantic ValidationError
            SourceConfig(name="x", family="ftp", connector="y", resources=["a"])

    def test_files_connector_errors_propagate(self, tmp_path):
        from ingestion.connectors.files import FileNotFoundInRegistryError

        source = SourceConfig(
            name="missing_file",
            family="files",
            connector="local_file",
            path="does/not/exist.csv",
            format="csv",
        )
        with pytest.raises(FileNotFoundInRegistryError, match="missing_file"):
            extract_source(source, work_dir=tmp_path)

    def test_connector_registry_has_the_three_builtin_families(self):
        for family in ("sql", "files", "rest"):
            assert connector_registry.get(family) is not None
        with pytest.raises(ValueError, match="ftp"):
            connector_registry.get("ftp")


class TestMissingDriver:
    def test_missing_driver_names_the_package(self, monkeypatch):
        from ingestion.connectors import sql as sql_connector

        monkeypatch.setattr(sql_connector, "find_spec", lambda name: None)
        with pytest.raises(sql_connector.MissingDriverError, match="psycopg2"):
            sql_connector.check_driver("postgresql")

    def test_sqlite_needs_no_driver_package(self, monkeypatch):
        from ingestion.connectors import sql as sql_connector

        # sqlite3 is stdlib -- check_driver must not even look it up.
        monkeypatch.setattr(
            sql_connector,
            "find_spec",
            lambda name: (_ for _ in ()).throw(AssertionError("should not be called")),
        )
        sql_connector.check_driver("sqlite")  # does not raise

    def test_installed_driver_passes(self):
        from ingestion.connectors import sql as sql_connector

        if find_spec("psycopg2") is None:
            pytest.skip("requires optional 'ingest' dependencies")
        sql_connector.check_driver("sqlite")
        sql_connector.check_driver("postgresql")
