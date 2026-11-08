"""Connector test (task 4): the ``rest`` family against the two real
registry sources. Marked ``network`` -- excluded by default (needs a real
external API) -- run explicitly with ``-m network``.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent

pytestmark = pytest.mark.network


def _load_pipeline_module():
    spec = importlib.util.spec_from_file_location(
        "dlt_pipeline_rest_connector_test",
        REPO_ROOT / "ingestion" / "dlt" / "pipeline.py",
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def pipeline():
    return _load_pipeline_module()


class TestRestConnectorAgainstRealApis:
    def test_jsonplaceholder_single_page(self, pipeline, tmp_path):
        import pyarrow.parquet as papq

        source = pipeline.get_source("jsonplaceholder")
        written = pipeline.extract_source(source, work_dir=tmp_path)
        assert set(written) == {"jsonplaceholder_users", "jsonplaceholder_posts"}
        users = papq.read_table(written["jsonplaceholder_users"])
        assert users.num_rows == 10
        assert "email" in users.column_names

    def test_open_brewery_db_paginated(self, pipeline, tmp_path):
        import pyarrow.parquet as papq

        source = pipeline.get_source("open_brewery_db")
        written = pipeline.extract_source(source, work_dir=tmp_path)
        breweries = papq.read_table(written["open_brewery_db_breweries"])
        # Capped at maximum_page=5 in sources.yml -- more than one page's
        # worth of rows proves pagination actually ran, not just page 1.
        assert breweries.num_rows > 50
        assert "name" in breweries.column_names


class TestRestSourcesActuallyLoadIntoIceberg:
    """Point 3 of the follow-up review: extraction alone (above) isn't the
    same as `dhd ingest-dlt` actually committing the result into Iceberg --
    exercised here through the real CLI end to end, against the real
    public APIs. `ingest-dlt` now always writes `replace`/`append` through
    the Flight server (follow-up review, step 0), so this needs a reachable
    one -- the shared in-process `flight_server` fixture (conftest.py),
    queried through the default (flight) engine."""

    def test_jsonplaceholder_loads_and_is_queryable(self, flight_server):
        from click.testing import CliRunner

        from datahut_duckhouse import cli as cli_mod

        runner = CliRunner()
        r = runner.invoke(cli_mod.cli, ["ingest-dlt", "--source", "jsonplaceholder"])
        assert r.exit_code == 0, r.output
        assert "jsonplaceholder_users" in r.output
        assert "jsonplaceholder_posts" in r.output

        r = runner.invoke(
            cli_mod.cli,
            [
                "query",
                "SELECT count(*) AS n FROM jsonplaceholder_users",
                "--format",
                "json",
            ],
        )
        assert r.exit_code == 0, r.output
        assert '"n": 10' in r.output

    def test_open_brewery_db_loads_and_rerun_does_not_duplicate(self, flight_server):
        from click.testing import CliRunner

        from datahut_duckhouse import cli as cli_mod

        runner = CliRunner()
        for _ in range(2):  # write_disposition: replace -- must not duplicate
            r = runner.invoke(
                cli_mod.cli, ["ingest-dlt", "--source", "open_brewery_db"]
            )
            assert r.exit_code == 0, r.output

        r = runner.invoke(
            cli_mod.cli,
            [
                "query",
                "SELECT count(*) AS n FROM open_brewery_db_breweries",
                "--format",
                "json",
            ],
        )
        assert r.exit_code == 0, r.output
        count = json.loads(r.output)[0]["n"]
        assert count > 50  # same pagination floor as the extraction-only test above
