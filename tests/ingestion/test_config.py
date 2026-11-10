"""Unit tests for ``ingestion.config`` (``models.py``/``registry.py``/
``rules.py``) -- no Docker, no network. Exercises validation and the
line/field error reporting that ``dhd ingest validate`` surfaces.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from ingestion.config import registry as _registry_module


@pytest.fixture(scope="module")
def registry():
    return _registry_module


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "sources.yml"
    path.write_text(content)
    return path


class TestValidEntries:
    def test_minimal_sql_source(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: sql
    connector: sql_database
    secret_ref: X_URL
    dialect: postgresql
""",
        )
        sources = registry.load_sources(path)
        assert len(sources) == 1
        assert sources[0].write_disposition == "replace"  # default
        assert sources[0].schema_contract == "evolve"  # default
        assert sources[0].prefix == "x"

    def test_target_prefix_overrides_name(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: files
    connector: local_file
    path: a.csv
    format: csv
    target_prefix: custom
""",
        )
        assert registry.load_sources(path)[0].prefix == "custom"

    def test_files_defaults_resources_to_name(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: my_file
    family: files
    connector: local_file
    path: a.csv
    format: csv
""",
        )
        assert registry.load_sources(path)[0].resources == ["my_file"]


class TestFamilySpecificValidation:
    def test_sql_without_dialect_is_rejected(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: sql
    connector: sql_database
    secret_ref: X_URL
""",
        )
        with pytest.raises(registry.SourcesConfigError, match="dialect"):
            registry.load_sources(path)

    def test_sql_unknown_dialect_is_rejected(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: sql
    connector: sql_database
    secret_ref: X_URL
    dialect: oracle
""",
        )
        with pytest.raises(registry.SourcesConfigError, match="oracle"):
            registry.load_sources(path)

    def test_sql_without_secret_ref_is_rejected(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: sql
    connector: sql_database
    dialect: sqlite
""",
        )
        with pytest.raises(registry.SourcesConfigError, match="secret_ref"):
            registry.load_sources(path)

    def test_files_without_path_is_rejected(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: files
    connector: local_file
    format: csv
""",
        )
        with pytest.raises(registry.SourcesConfigError, match="path"):
            registry.load_sources(path)

    def test_files_unknown_format_is_rejected(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: files
    connector: local_file
    path: a.xml
    format: xml
""",
        )
        with pytest.raises(registry.SourcesConfigError, match="xml"):
            registry.load_sources(path)

    def test_rest_without_base_url_is_rejected(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: rest
    connector: rest_api
    resources: [a]
""",
        )
        with pytest.raises(registry.SourcesConfigError, match="base_url"):
            registry.load_sources(path)

    def test_rest_without_resources_is_rejected(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: rest
    connector: rest_api
    base_url: https://example.test/
""",
        )
        with pytest.raises(registry.SourcesConfigError, match="resources"):
            registry.load_sources(path)

    def test_merge_without_primary_key_is_rejected(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: sql
    connector: sql_database
    secret_ref: X_URL
    dialect: sqlite
    write_disposition: merge
""",
        )
        with pytest.raises(registry.SourcesConfigError, match="primary_key"):
            registry.load_sources(path)


class TestIncrementalWriteDispositionInteraction:
    def test_incremental_with_append_is_rejected(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: sql
    connector: sql_database
    secret_ref: X_URL
    dialect: sqlite
    write_disposition: append
    incremental:
      column: updated_at
      type: timestamp
""",
        )
        with pytest.raises(registry.SourcesConfigError, match="duplicates rows"):
            registry.load_sources(path)

    def test_incremental_with_replace_warns_but_does_not_reject(
        self, registry, tmp_path
    ):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: sql
    connector: sql_database
    secret_ref: X_URL
    dialect: sqlite
    write_disposition: replace
    incremental:
      column: updated_at
      type: timestamp
""",
        )
        warnings: list[str] = []
        sources = registry.load_sources(path, collect_warnings=warnings)
        assert len(sources) == 1
        assert len(warnings) == 1
        assert "silent data loss" in warnings[0]
        assert "line 3" in warnings[0]  # the "  - name: x" line (see idx math above)

    def test_incremental_with_merge_is_silent(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: sql
    connector: sql_database
    secret_ref: X_URL
    dialect: sqlite
    write_disposition: merge
    primary_key: [id]
    incremental:
      column: updated_at
      type: timestamp
""",
        )
        warnings: list[str] = []
        sources = registry.load_sources(path, collect_warnings=warnings)
        assert len(sources) == 1
        assert warnings == []


class TestErrorMessagesAreReadable:
    def test_reports_line_number_and_field(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: ok_source
    family: files
    connector: local_file
    path: a.csv
    format: csv
  - name: bad_source
    family: sql
    connector: sql_database
""",
        )
        with pytest.raises(registry.SourcesConfigError) as excinfo:
            registry.load_sources(path)
        message = str(excinfo.value)
        assert "bad_source" in message
        # bad_source's mapping starts on line 8 (1-based) in the text above.
        assert "line 8" in message

    def test_unknown_field_is_rejected(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: files
    connector: local_file
    path: a.csv
    format: csv
    tables: [oops]
""",
        )
        with pytest.raises(registry.SourcesConfigError, match="tables"):
            registry.load_sources(path)

    def test_duplicate_name_is_rejected(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: dup
    family: files
    connector: local_file
    path: a.csv
    format: csv
  - name: dup
    family: files
    connector: local_file
    path: b.csv
    format: csv
""",
        )
        with pytest.raises(registry.SourcesConfigError, match="duplicate"):
            registry.load_sources(path)

    def test_reports_every_error_not_just_the_first(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: bad1
    family: sql
    connector: sql_database
  - name: bad2
    family: files
    connector: local_file
""",
        )
        with pytest.raises(registry.SourcesConfigError) as excinfo:
            registry.load_sources(path)
        message = str(excinfo.value)
        assert "bad1" in message
        assert "bad2" in message


class TestGetSource:
    def test_unknown_source_raises(self, registry):
        with pytest.raises(registry.SourcesConfigError, match="no_such"):
            registry.get_source("no_such", sources=[])

    def test_resolves_from_given_list(self, registry, tmp_path):
        path = _write(
            tmp_path,
            """
sources:
  - name: x
    family: files
    connector: local_file
    path: a.csv
    format: csv
""",
        )
        sources = registry.load_sources(path)
        assert registry.get_source("x", sources=sources).name == "x"


class TestLoadRealSourcesYml:
    def test_real_sources_yml_parses(self):
        from ingestion.config.registry import load_sources

        sources = load_sources()
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

    def test_prefixed_table_name_format(self):
        from ingestion.config.registry import prefixed_table_name

        assert prefixed_table_name("pg_chinook", "album") == "pg_chinook_album"
