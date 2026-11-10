"""``ingestion.sinks.router`` (ADR-0006, decision 5): auto|flight|direct
routing, the capacity check, and the "big load on flight" warning -- the
full decision matrix, each combination on its own."""

from __future__ import annotations

import pytest

from ingestion.sinks.router import (
    DirectSinkUnavailableError,
    UnknownSinkError,
    flight_size_warning,
    resolve_sink,
)


def _capacity(possible: bool, reason: str = "unreachable"):
    return lambda: (possible, "" if possible else reason)


class TestResolveSinkMatrix:
    """Every (source_sink, capacity) combination."""

    def test_flight_explicit_with_capacity_true(self):
        sink, reason = resolve_sink("flight", capacity_check=_capacity(True))
        assert sink == "flight"
        assert "explicit" in reason

    def test_flight_explicit_with_capacity_false(self):
        """flight is honored regardless of capacity -- capacity only
        matters for auto/direct."""
        sink, reason = resolve_sink("flight", capacity_check=_capacity(False))
        assert sink == "flight"
        assert "explicit" in reason

    def test_direct_explicit_with_capacity_true(self):
        sink, reason = resolve_sink("direct", capacity_check=_capacity(True))
        assert sink == "direct"
        assert "explicit" in reason

    def test_direct_explicit_with_capacity_false_raises(self):
        with pytest.raises(DirectSinkUnavailableError, match="storage unreachable"):
            resolve_sink(
                "direct", capacity_check=_capacity(False, "storage unreachable")
            )

    def test_auto_with_capacity_true_picks_direct(self):
        sink, reason = resolve_sink("auto", capacity_check=_capacity(True))
        assert sink == "direct"
        assert "auto" in reason

    def test_auto_with_capacity_false_picks_flight(self):
        sink, reason = resolve_sink(
            "auto", capacity_check=_capacity(False, "DNS failure")
        )
        assert sink == "flight"
        assert "DNS failure" in reason

    def test_unknown_sink_raises(self):
        with pytest.raises(UnknownSinkError):
            resolve_sink("bogus", capacity_check=_capacity(True))


class TestCapacityCheckIsNeverConsultedForDirectSinkErrorMessage:
    def test_direct_unavailable_error_names_the_workstation_rule(self):
        with pytest.raises(DirectSinkUnavailableError, match="docs/CATALOG.md"):
            resolve_sink("direct", capacity_check=_capacity(False, "x"))


class TestFlightSizeWarning:
    def test_below_threshold_no_warning(self):
        assert flight_size_warning(999) is None

    def test_at_threshold_warns(self):
        warning = flight_size_warning(1_000_000)
        assert warning is not None
        assert "1,000,000" in warning

    def test_above_threshold_warns(self):
        assert flight_size_warning(5_000_000) is not None

    def test_respects_env_override(self, monkeypatch):
        monkeypatch.setenv("DHD_SINK_WARN_ROWS", "10")
        assert flight_size_warning(5) is None
        assert flight_size_warning(10) is not None

    def test_invalid_env_value_falls_back_to_default(self, monkeypatch):
        monkeypatch.setenv("DHD_SINK_WARN_ROWS", "not-a-number")
        assert flight_size_warning(999) is None
        assert flight_size_warning(1_000_000) is not None


class TestCheckDirectCapacity:
    """The real (non-stubbed) check_direct_capacity, against fake
    endpoints -- no Docker needed, just sockets that definitely refuse."""

    def test_unreachable_rest_catalog_uri(self, monkeypatch):
        from ingestion.sinks.router import check_direct_capacity

        # Port 1 on loopback: nothing listens there, so the connection is
        # refused immediately -- deterministic and fast, unlike relying on
        # a hostname failing to resolve (DNS behavior varies by host/CI).
        monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "rest")
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://127.0.0.1:1/iceberg")
        monkeypatch.delenv("S3_ENDPOINT", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
        possible, reason = check_direct_capacity()
        assert possible is False
        assert "catalog unreachable" in reason

    def test_sql_catalog_type_never_probes_the_uri(self, monkeypatch):
        """ICEBERG_CATALOG_URI may be set (e.g. left over from a .env) but
        unused when ICEBERG_CATALOG_TYPE isn't "rest" -- must not be probed."""
        from ingestion.sinks.router import check_direct_capacity

        monkeypatch.delenv("ICEBERG_CATALOG_TYPE", raising=False)
        monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://127.0.0.1:1/iceberg")
        monkeypatch.delenv("S3_ENDPOINT", raising=False)
        monkeypatch.delenv("AWS_ENDPOINT_URL_S3", raising=False)
        possible, reason = check_direct_capacity()
        assert possible is True
        assert reason == ""

    def test_unreachable_s3_endpoint_with_a_remote_warehouse(self, monkeypatch):
        from ingestion.sinks.router import check_direct_capacity

        monkeypatch.delenv("ICEBERG_CATALOG_TYPE", raising=False)
        monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "s3://duckhouse-warehouse/")
        monkeypatch.setenv("S3_ENDPOINT", "http://127.0.0.1:1")
        possible, reason = check_direct_capacity()
        assert possible is False
        assert "storage unreachable" in reason

    def test_s3_endpoint_set_but_warehouse_is_local_is_not_probed(self, monkeypatch):
        """S3_ENDPOINT may be set in the environment (e.g. a repo-root
        .env) while completely unused by a local/solo warehouse -- must
        not make capacity falsely report "not possible"."""
        from ingestion.sinks.router import check_direct_capacity

        monkeypatch.delenv("ICEBERG_CATALOG_TYPE", raising=False)
        monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
        monkeypatch.setenv("ICEBERG_WAREHOUSE", "")
        monkeypatch.setenv("ICEBERG_WAREHOUSE_PATH", "/tmp/some/local/warehouse")
        monkeypatch.setenv("S3_ENDPOINT", "http://127.0.0.1:1")
        possible, reason = check_direct_capacity()
        assert possible is True
        assert reason == ""
