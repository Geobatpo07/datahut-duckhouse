"""Unit tests for ingestion/dlt/pipeline.py -- no Docker, no network.

Covers sources.yml parsing, table-name prefixing, and the missing-driver
error message (step 5 "unitaires rapides"). The actual extraction/load
round trip against real Postgres/MySQL containers is covered by
tests/integration/test_multi_engine.py (-m integration, needs Docker).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent


def _load_pipeline_module():
    """Import ingestion/dlt/pipeline.py by path -- same technique as
    tests/test_scripts.py's `_load_script_module`, and for the same reason
    pipeline.py itself avoids `import ingestion.dlt.pipeline`: the parent
    `ingestion/__init__.py` imports submodules that don't exist on disk."""
    spec = importlib.util.spec_from_file_location(
        "dlt_pipeline_test", REPO_ROOT / "ingestion" / "dlt" / "pipeline.py"
    )
    module = importlib.util.module_from_spec(spec)
    # dataclasses' field-type resolution looks the module up in sys.modules
    # by __module__ name -- it must be registered before exec_module runs.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def pipeline():
    return _load_pipeline_module()


def _write_sources_yml(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "sources.yml"
    path.write_text(content)
    return path


class TestLoadSources:
    def test_real_sources_yml_parses(self, pipeline):
        sources = pipeline.load_sources()
        names = {s.name for s in sources}
        assert names == {
            "sqlite_chinook",
            "pg_chinook",
            "mysql_chinook",
            "mssql_chinook",
        }
        by_name = {s.name: s for s in sources}
        assert by_name["pg_chinook"].dialect == "postgresql"
        assert by_name["pg_chinook"].connection_env == "CHINOOK_POSTGRES_URL"
        assert by_name["pg_chinook"].schema == "public"
        assert by_name["pg_chinook"].tables == ()

    def test_unknown_dialect_rejected(self, pipeline, tmp_path):
        path = _write_sources_yml(
            tmp_path,
            "sources:\n  - name: bad\n    dialect: oracle\n    connection_env: X\n",
        )
        with pytest.raises(pipeline.SourcesConfigError, match="oracle"):
            pipeline.load_sources(path)

    def test_malformed_entry_rejected(self, pipeline, tmp_path):
        path = _write_sources_yml(
            tmp_path, "sources:\n  - name: incomplete\n    dialect: sqlite\n"
        )
        with pytest.raises(pipeline.SourcesConfigError):
            pipeline.load_sources(path)

    def test_empty_file_yields_no_sources(self, pipeline, tmp_path):
        path = _write_sources_yml(tmp_path, "sources: []\n")
        assert pipeline.load_sources(path) == []

    def test_get_source_unknown_raises(self, pipeline):
        with pytest.raises(pipeline.SourcesConfigError, match="no_such_source"):
            pipeline.get_source("no_such_source", sources=[])


class TestConnectionUrl:
    def test_raises_when_env_var_unset(self, pipeline, monkeypatch):
        monkeypatch.delenv("DOES_NOT_EXIST_ENV", raising=False)
        source = pipeline.Source(
            name="x",
            dialect="sqlite",
            connection_env="DOES_NOT_EXIST_ENV",
            schema=None,
            tables=(),
        )
        with pytest.raises(pipeline.SourcesConfigError, match="DOES_NOT_EXIST_ENV"):
            _ = source.connection_url

    def test_reads_env_var(self, pipeline, monkeypatch):
        monkeypatch.setenv("SOME_TEST_URL", "sqlite:///tmp/test.db")
        source = pipeline.Source(
            name="x",
            dialect="sqlite",
            connection_env="SOME_TEST_URL",
            schema=None,
            tables=(),
        )
        assert source.connection_url == "sqlite:///tmp/test.db"


class TestPrefixedTableName:
    def test_prefix_format(self, pipeline):
        assert pipeline.prefixed_table_name("pg_chinook", "album") == "pg_chinook_album"


class TestMissingDriver:
    def test_missing_driver_names_the_package(self, pipeline, monkeypatch):
        monkeypatch.setattr(pipeline, "find_spec", lambda name: None)
        with pytest.raises(pipeline.MissingDriverError, match="psycopg2"):
            pipeline._check_driver("postgresql")

    def test_sqlite_needs_no_driver_package(self, pipeline, monkeypatch):
        # sqlite3 is stdlib -- _check_driver must not even look it up.
        monkeypatch.setattr(
            pipeline,
            "find_spec",
            lambda name: (_ for _ in ()).throw(AssertionError("should not be called")),
        )
        pipeline._check_driver("sqlite")  # does not raise

    def test_installed_driver_passes(self, pipeline):
        pipeline._check_driver("sqlite")
        # psycopg2-binary is installed via the `ingest` extra in this env.
        pipeline._check_driver("postgresql")
