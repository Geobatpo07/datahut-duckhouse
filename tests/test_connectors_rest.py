"""Connector test (task 4): the ``rest`` family against the two real
registry sources. Marked ``network`` -- excluded by default (needs a real
external API) -- run explicitly with ``-m network``.
"""

from __future__ import annotations

import importlib.util
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
