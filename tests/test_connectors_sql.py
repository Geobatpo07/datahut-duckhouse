"""Connector test (task 4, "un test d'intégration par connecteur"): the
``sql`` family against fixed, local, no-Docker data (the SQLite Chinook
fixture). Postgres/MySQL variants (same connector, real Docker containers)
are tests/integration/test_multi_engine.py -- not duplicated here.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent


def _load_pipeline_module():
    spec = importlib.util.spec_from_file_location(
        "dlt_pipeline_sql_connector_test",
        REPO_ROOT / "ingestion" / "dlt" / "pipeline.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def pipeline():
    return _load_pipeline_module()


@pytest.fixture(autouse=True)
def _sqlite_env():
    os.environ.setdefault(
        "CHINOOK_SQLITE_URL",
        f"sqlite:///{REPO_ROOT / 'tests' / 'data' / 'Chinook_Sqlite.sqlite'}",
    )


class TestSqlConnectorAgainstFixedData:
    def test_extracts_every_table(self, pipeline, tmp_path):
        source = pipeline.get_source("sqlite_chinook")
        written = pipeline.extract_source(source, work_dir=tmp_path)
        assert len(written) == 11  # Chinook's fixed table count
        assert "sqlite_chinook_Album" in written

    def test_single_resource_selection(self, pipeline, tmp_path):
        source = pipeline.get_source("sqlite_chinook")
        narrowed = pipeline.SourceConfig(
            name=source.name,
            family="sql",
            connector="sql_database",
            secret_ref=source.secret_ref,
            dialect=source.dialect,
            resources=["Artist"],
        )
        written = pipeline.extract_source(narrowed, work_dir=tmp_path)
        assert set(written) == {"sqlite_chinook_Artist"}

    def test_row_count_matches_known_fixture_size(self, pipeline, tmp_path):
        import pyarrow.parquet as papq

        source = pipeline.get_source("sqlite_chinook")
        written = pipeline.extract_source(source, work_dir=tmp_path)
        # Fixed by the pinned Chinook fixture (docs/THIRD_PARTY_CHINOOK.md) --
        # same count asserted in scripts/bench_writes.py's DOGFOOD numbers.
        assert papq.read_table(written["sqlite_chinook_Artist"]).num_rows == 275
