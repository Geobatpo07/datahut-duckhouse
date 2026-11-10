"""Proves ``DirectSink.write`` is actually wired through
``with_commit_retry`` (not just that the helper works in isolation --
that's ``test_commit_retry.py``). ``catalog_guard`` is stubbed out here:
this test's job is the retry wiring, not the guard (covered by
``test_engine.py::TestCatalogGuard``)."""

from __future__ import annotations

import time
from unittest.mock import Mock

import pyarrow as pa
import pytest
from pyiceberg.exceptions import CommitFailedException

from ingestion.sinks import direct_sink as direct_sink_module
from ingestion.sinks.base import CommitConflictError
from ingestion.sinks.direct_sink import DirectSink


@pytest.fixture(autouse=True)
def _skip_catalog_guard(monkeypatch):
    monkeypatch.setattr(direct_sink_module, "check_same_catalog", lambda *a, **kw: None)


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    monkeypatch.setattr(time, "sleep", lambda _: None)


def _sink(engine) -> DirectSink:
    return DirectSink(engine, flight_backend_for_guard=Mock())


def test_merge_retries_a_conflicting_commit_then_succeeds():
    engine = Mock()
    engine.list_tables.return_value = ["t"]
    engine.upload_table.side_effect = [
        CommitFailedException("conflict"),
        CommitFailedException("conflict"),
        None,
    ]

    verb = _sink(engine).write("t", pa.table({"id": [1]}), "merge", primary_key=["id"])

    assert verb == "merged"
    assert engine.upload_table.call_count == 3


def test_create_on_a_new_table_also_retries():
    engine = Mock()
    engine.list_tables.return_value = []
    engine.create_table.side_effect = [CommitFailedException("conflict"), None]

    verb = _sink(engine).write("t", pa.table({"id": [1]}), "merge")

    assert verb == "created"
    assert engine.create_table.call_count == 2


def test_gives_up_past_the_retry_budget(monkeypatch):
    monkeypatch.setenv("DHD_COMMIT_RETRY_ATTEMPTS", "2")
    engine = Mock()
    engine.list_tables.return_value = ["t"]
    engine.upload_table.side_effect = CommitFailedException("conflict")

    with pytest.raises(CommitConflictError):
        _sink(engine).write("t", pa.table({"id": [1]}), "merge", primary_key=["id"])
    assert engine.upload_table.call_count == 2
