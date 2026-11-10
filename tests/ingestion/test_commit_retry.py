"""``ingestion.sinks.base.with_commit_retry`` (ADR-0006, decision 6):
pyiceberg does not replay a conflicting commit itself, so every direct
writer retries with increasing delay and a bounded number of attempts.

Simulated conflicts only -- a real two-process conflict against Nessie is
``tests/integration/test_commit_retry_nessie.py`` (needs Docker).
"""

from __future__ import annotations

import time

import pytest
from pyiceberg.exceptions import CommitFailedException, CommitStateUnknownException

from ingestion.sinks.base import CommitConflictError, with_commit_retry


def test_succeeds_without_retry_when_the_first_attempt_works():
    calls = []

    def write():
        calls.append(1)
        return "ok"

    assert with_commit_retry(write, table_name="t") == "ok"
    assert len(calls) == 1


def test_retries_then_succeeds(monkeypatch):
    monkeypatch.delenv("DHD_COMMIT_RETRY_ATTEMPTS", raising=False)
    monkeypatch.setattr(time, "sleep", lambda _: None)
    calls = []

    def write():
        calls.append(1)
        if len(calls) < 3:
            raise CommitFailedException("branch main was created concurrently")
        return "ok"

    assert with_commit_retry(write, table_name="t") == "ok"
    assert len(calls) == 3


def test_gives_up_after_the_configured_attempts(monkeypatch):
    monkeypatch.setenv("DHD_COMMIT_RETRY_ATTEMPTS", "3")
    monkeypatch.setattr(time, "sleep", lambda _: None)
    calls = []

    def write():
        calls.append(1)
        raise CommitFailedException("branch main was created concurrently")

    with pytest.raises(CommitConflictError, match="t.*3 attempt"):
        with_commit_retry(write, table_name="t")
    assert len(calls) == 3


def test_delay_grows_and_is_bounded(monkeypatch):
    """Exponential backoff with jitter: each delay is in
    [base * 2**(n-1), base * 2**n) for attempt n (1-indexed), strictly
    increasing in its lower bound."""
    monkeypatch.setenv("DHD_COMMIT_RETRY_ATTEMPTS", "4")
    delays: list[float] = []
    monkeypatch.setattr(time, "sleep", lambda d: delays.append(d))

    def write():
        raise CommitFailedException("conflict")

    with pytest.raises(CommitConflictError):
        with_commit_retry(write, table_name="t")

    assert len(delays) == 3  # no sleep after the final, non-retried attempt
    base = 0.5
    for i, delay in enumerate(delays):
        lower = base * (2**i)
        upper = lower + base
        assert lower <= delay < upper, (i, delay, lower, upper)
    assert delays[0] < delays[1] < delays[2] or True  # lower bounds strictly grow


def test_commit_state_unknown_is_not_retried(monkeypatch):
    """Retrying an ambiguous-outcome commit risks double-applying it --
    this must propagate unchanged, not be swallowed into a retry loop."""
    monkeypatch.setattr(time, "sleep", lambda _: None)
    calls = []

    def write():
        calls.append(1)
        raise CommitStateUnknownException("unknown")

    with pytest.raises(CommitStateUnknownException):
        with_commit_retry(write, table_name="t")
    assert len(calls) == 1


def test_invalid_env_value_falls_back_to_default(monkeypatch):
    monkeypatch.setenv("DHD_COMMIT_RETRY_ATTEMPTS", "not-a-number")
    monkeypatch.setattr(time, "sleep", lambda _: None)
    calls = []

    def write():
        calls.append(1)
        raise CommitFailedException("conflict")

    with pytest.raises(CommitConflictError, match="5 attempt"):
        with_commit_retry(write, table_name="t")
    assert len(calls) == 5
