"""Connector test (task 4, "un test d'intégration par connecteur"): the
``sql`` family against fixed, local, no-Docker data (the SQLite Chinook
fixture). Postgres/MySQL variants (same connector, real Docker containers)
are tests/integration/test_multi_engine.py -- not duplicated here.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

pytest.importorskip("dlt", reason="requires optional 'ingest' dependencies")

from ingestion.config.models import SourceConfig
from ingestion.config.registry import get_source
from ingestion.connectors import sql

REPO_ROOT = Path(__file__).parent.parent.parent


@pytest.fixture(autouse=True)
def _sqlite_env():
    os.environ.setdefault(
        "CHINOOK_SQLITE_URL",
        f"sqlite:///{REPO_ROOT / 'tests' / 'data' / 'Chinook_Sqlite.sqlite'}",
    )


class TestSqlConnectorAgainstFixedData:
    def test_extracts_every_table(self, tmp_path):
        source = get_source("sqlite_chinook")
        written = sql.build_resources(source, work_dir=tmp_path)
        assert len(written) == 11  # Chinook's fixed table count
        assert "sqlite_chinook_Album" in written

    def test_single_resource_selection(self, tmp_path):
        source = get_source("sqlite_chinook")
        narrowed = SourceConfig(
            name=source.name,
            family="sql",
            connector="sql_database",
            secret_ref=source.secret_ref,
            dialect=source.dialect,
            resources=["Artist"],
        )
        written = sql.build_resources(narrowed, work_dir=tmp_path)
        assert set(written) == {"sqlite_chinook_Artist"}

    def test_row_count_matches_known_fixture_size(self, tmp_path):
        import pyarrow.parquet as papq

        source = get_source("sqlite_chinook")
        written = sql.build_resources(source, work_dir=tmp_path)
        # Fixed by the pinned Chinook fixture (docs/THIRD_PARTY_CHINOOK.md) --
        # same count asserted in scripts/bench_writes.py's DOGFOOD numbers.
        assert papq.read_table(written["sqlite_chinook_Artist"]).num_rows == 275


class TestIncrementalCursorStatePersistence:
    """Regression test for a bug found live in task 5's incremental
    experiment (docs/DLT_EVAL.md): a timestamp cursor stored via
    ``engine.state.write_last_value`` round-trips through JSON as a plain
    string, which ``dlt.sources.incremental`` then rejected against a
    ``timestamp[us, tz=UTC]`` column ("Could not coerce start_value" --
    object of type str). ``_decode_cursor`` fixes this by parsing it back
    before reuse."""

    def test_timestamp_cursor_survives_a_second_run(self, tmp_path, monkeypatch):
        monkeypatch.setenv("DHD_INGEST_STATE_DIR", str(tmp_path / "_state"))

        source = SourceConfig(
            name="incremental_test",
            family="sql",
            connector="sql_database",
            secret_ref="CHINOOK_SQLITE_URL",
            dialect="sqlite",
            resources=["Employee"],
            incremental={"column": "HireDate", "type": "timestamp"},
        )
        # Run 1: populates the state file with a real datetime, JSON-encoded.
        run1 = sql.build_resources(source, work_dir=tmp_path / "out1")
        assert "incremental_test_Employee" in run1
        # Run 2 must not raise -- this is exactly where the bug surfaced (a
        # plain str initial_value rejected against a timestamp column).
        # Nothing changed since run 1, so an empty result here is correct.
        sql.build_resources(source, work_dir=tmp_path / "out2")
