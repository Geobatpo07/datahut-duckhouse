"""``iceberg_catalog.factory``'s mode detection and the versioning gate
(ADR-0006, decisions 2 and 4)."""

from __future__ import annotations

import pytest

from iceberg_catalog import (
    VersioningUnavailableError,
    is_deployment_mode,
    require_versioning,
)
from iceberg_catalog.factory import catalog_mode


def test_defaults_to_solo_mode(monkeypatch):
    monkeypatch.delenv("ICEBERG_CATALOG_TYPE", raising=False)
    assert catalog_mode() == "solo"
    assert is_deployment_mode() is False


def test_sql_type_is_solo_mode(monkeypatch):
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "sql")
    assert catalog_mode() == "solo"


def test_rest_type_is_deployment_mode(monkeypatch):
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "rest")
    assert catalog_mode() == "deployment"
    assert is_deployment_mode() is True


def test_require_versioning_passes_in_deployment_mode(monkeypatch):
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "rest")
    require_versioning()  # must not raise


def test_require_versioning_raises_in_solo_mode(monkeypatch):
    monkeypatch.delenv("ICEBERG_CATALOG_TYPE", raising=False)
    with pytest.raises(VersioningUnavailableError):
        require_versioning()
