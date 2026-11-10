"""``iceberg_catalog.versioning`` (ADR-0006, decision 4): Nessie's REST API
v2 client. HTTP is mocked here (canned responses/errors against a fake
``urlopen``) -- the real wire shapes (create/delete/reassign/merge,
including a genuine 409 conflict's exact message) were verified live
against a running Nessie first; see
``scripts/verify_versioning_nessie.py`` for the real-Nessie
reproduction this module's parsing is modeled on.
"""

from __future__ import annotations

import io
import json
import urllib.error

import pytest

from iceberg_catalog.factory import VersioningUnavailableError
from iceberg_catalog.versioning import (
    MergeConflictError,
    NessieApiError,
    Reference,
    ReferenceAlreadyExistsError,
    ReferenceNotFoundError,
    UnmergedWorkError,
    create_branch,
    create_tag,
    delete_branch,
    get_reference,
    is_fully_merged,
    list_references,
    merge,
    rollback,
)


@pytest.fixture(autouse=True)
def _deployment_mode(monkeypatch):
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "rest")
    monkeypatch.setenv("ICEBERG_CATALOG_URI", "http://nessie:19120/iceberg")


class _FakeResponse:
    def __init__(self, status: int, body: dict | None):
        self.status = status
        self._body = json.dumps(body).encode() if body is not None else b""

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


def _http_error(code: int, body: dict | None):
    raw = json.dumps(body).encode() if body is not None else b""
    return urllib.error.HTTPError(
        "http://nessie:19120/api/v2/x", code, "err", {}, io.BytesIO(raw)
    )


def _mock_urlopen(monkeypatch, responder):
    """``responder(request) -> _FakeResponse`` or raises an HTTPError."""
    import urllib.request

    monkeypatch.setattr(urllib.request, "urlopen", responder)


def test_require_versioning_blocks_before_any_http_call(monkeypatch):
    monkeypatch.setenv("ICEBERG_CATALOG_TYPE", "sql")
    calls = []
    _mock_urlopen(monkeypatch, lambda *a, **kw: calls.append(1))
    with pytest.raises(VersioningUnavailableError):
        get_reference("main")
    assert not calls


def test_base_url_missing_catalog_uri_raises(monkeypatch):
    monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
    with pytest.raises(NessieApiError, match="ICEBERG_CATALOG_URI"):
        get_reference("main")


class TestGetReference:
    def test_parses_a_branch(self, monkeypatch):
        _mock_urlopen(
            monkeypatch,
            lambda *a, **kw: _FakeResponse(
                200, {"reference": {"type": "BRANCH", "name": "main", "hash": "ab12"}}
            ),
        )
        ref = get_reference("main")
        assert ref == Reference(type="BRANCH", name="main", hash="ab12")

    def test_404_raises_not_found(self, monkeypatch):
        def raise_404(*a, **kw):
            raise _http_error(404, {"message": "not found"})

        _mock_urlopen(monkeypatch, raise_404)
        with pytest.raises(ReferenceNotFoundError):
            get_reference("ghost")


class TestListReferences:
    def test_parses_multiple(self, monkeypatch):
        _mock_urlopen(
            monkeypatch,
            lambda *a, **kw: _FakeResponse(
                200,
                {
                    "references": [
                        {"type": "BRANCH", "name": "main", "hash": "aa"},
                        {"type": "TAG", "name": "v1", "hash": "bb"},
                    ]
                },
            ),
        )
        refs = list_references()
        assert refs == [
            Reference(type="BRANCH", name="main", hash="aa"),
            Reference(type="TAG", name="v1", hash="bb"),
        ]


class TestCreateBranchAndTag:
    def test_create_branch_success(self, monkeypatch):
        calls = []

        def responder(request, timeout=None):
            calls.append(request.full_url)
            if "/trees/main" in request.full_url and request.get_method() == "GET":
                return _FakeResponse(
                    200, {"reference": {"type": "BRANCH", "name": "main", "hash": "aa"}}
                )
            return _FakeResponse(
                200, {"reference": {"type": "BRANCH", "name": "dev", "hash": "aa"}}
            )

        _mock_urlopen(monkeypatch, responder)
        ref = create_branch("dev", from_branch="main")
        assert ref.name == "dev"
        assert any("name=dev&type=BRANCH" in url for url in calls)

    def test_create_branch_already_exists(self, monkeypatch):
        def responder(request, timeout=None):
            if request.get_method() == "GET":
                return _FakeResponse(
                    200, {"reference": {"type": "BRANCH", "name": "main", "hash": "aa"}}
                )
            raise _http_error(409, {"message": "already exists"})

        _mock_urlopen(monkeypatch, responder)
        with pytest.raises(ReferenceAlreadyExistsError):
            create_branch("dev", from_branch="main")

    def test_create_tag_uses_type_tag(self, monkeypatch):
        calls = []

        def responder(request, timeout=None):
            calls.append((request.full_url, request.get_method()))
            if request.get_method() == "GET":
                return _FakeResponse(
                    200, {"reference": {"type": "BRANCH", "name": "main", "hash": "aa"}}
                )
            return _FakeResponse(
                200, {"reference": {"type": "TAG", "name": "v1", "hash": "aa"}}
            )

        _mock_urlopen(monkeypatch, responder)
        ref = create_tag("v1", from_branch="main")
        assert ref.type == "TAG"
        assert any("type=TAG" in url for url, method in calls if method == "POST")


class TestMerge:
    def test_success(self, monkeypatch):
        def responder(request, timeout=None):
            if request.get_method() == "GET":
                name = request.full_url.rsplit("/", 1)[-1]
                return _FakeResponse(
                    200,
                    {
                        "reference": {
                            "type": "BRANCH",
                            "name": name,
                            "hash": "h-" + name,
                        }
                    },
                )
            return _FakeResponse(200, {"wasSuccessful": True, "wasApplied": True})

        _mock_urlopen(monkeypatch, responder)
        merge("dev", into="main")  # must not raise

    def test_conflict_lists_the_table(self, monkeypatch):
        def responder(request, timeout=None):
            if request.get_method() == "GET":
                name = request.full_url.rsplit("/", 1)[-1]
                return _FakeResponse(
                    200,
                    {
                        "reference": {
                            "type": "BRANCH",
                            "name": name,
                            "hash": "h-" + name,
                        }
                    },
                )
            raise _http_error(
                409,
                {
                    "status": 409,
                    "message": "The following keys have been changed in conflict: 'ns.t'",
                    "errorCode": "REFERENCE_CONFLICT",
                },
            )

        _mock_urlopen(monkeypatch, responder)
        with pytest.raises(MergeConflictError) as exc_info:
            merge("dev", into="main")
        assert exc_info.value.conflicting_keys == ["ns.t"]

    def test_conflict_with_multiple_tables(self, monkeypatch):
        def responder(request, timeout=None):
            if request.get_method() == "GET":
                name = request.full_url.rsplit("/", 1)[-1]
                return _FakeResponse(
                    200,
                    {
                        "reference": {
                            "type": "BRANCH",
                            "name": name,
                            "hash": "h-" + name,
                        }
                    },
                )
            raise _http_error(
                409,
                {
                    "message": "The following keys have been changed in conflict: "
                    "'ns.t1', 'ns.t2'",
                },
            )

        _mock_urlopen(monkeypatch, responder)
        with pytest.raises(MergeConflictError) as exc_info:
            merge("dev", into="main")
        assert exc_info.value.conflicting_keys == ["ns.t1", "ns.t2"]


class TestIsFullyMerged:
    def test_already_merged(self, monkeypatch):
        ref = Reference(type="BRANCH", name="dev", hash="same-hash")

        def responder(request, timeout=None):
            if request.get_method() == "GET":
                return _FakeResponse(
                    200,
                    {
                        "reference": {
                            "type": "BRANCH",
                            "name": "main",
                            "hash": "main-hash",
                        }
                    },
                )
            return _FakeResponse(
                200, {"wasApplied": False, "commonAncestor": "same-hash"}
            )

        _mock_urlopen(monkeypatch, responder)
        assert is_fully_merged(ref) is True

    def test_not_merged(self, monkeypatch):
        ref = Reference(type="BRANCH", name="dev", hash="dev-head")

        def responder(request, timeout=None):
            if request.get_method() == "GET":
                return _FakeResponse(
                    200,
                    {
                        "reference": {
                            "type": "BRANCH",
                            "name": "main",
                            "hash": "main-hash",
                        }
                    },
                )
            return _FakeResponse(
                200, {"wasApplied": True, "commonAncestor": "older-than-dev-head"}
            )

        _mock_urlopen(monkeypatch, responder)
        assert is_fully_merged(ref) is False


class TestDeleteBranch:
    def test_refuses_main(self, monkeypatch):
        with pytest.raises(NessieApiError, match="main"):
            delete_branch("main")

    def test_refuses_unmerged_without_force(self, monkeypatch):
        def responder(request, timeout=None):
            if request.get_method() == "GET":
                return _FakeResponse(
                    200,
                    {
                        "reference": {
                            "type": "BRANCH",
                            "name": "dev",
                            "hash": "dev-head",
                        }
                    },
                )
            return _FakeResponse(200, {"wasApplied": True, "commonAncestor": "older"})

        _mock_urlopen(monkeypatch, responder)
        with pytest.raises(UnmergedWorkError, match="--force"):
            delete_branch("dev")

    def test_force_deletes_unmerged(self, monkeypatch):
        calls = []

        def responder(request, timeout=None):
            calls.append(request.get_method())
            if request.get_method() == "GET":
                return _FakeResponse(
                    200,
                    {
                        "reference": {
                            "type": "BRANCH",
                            "name": "dev",
                            "hash": "dev-head",
                        }
                    },
                )
            return _FakeResponse(
                200,
                {"reference": {"type": "BRANCH", "name": "dev", "hash": "dev-head"}},
            )

        _mock_urlopen(monkeypatch, responder)
        delete_branch("dev", force=True)
        assert "DELETE" in calls

    def test_deletes_a_fully_merged_branch_without_force(self, monkeypatch):
        def responder(request, timeout=None):
            if request.get_method() == "GET":
                return _FakeResponse(
                    200,
                    {"reference": {"type": "BRANCH", "name": "dev", "hash": "same"}},
                )
            if request.get_method() == "POST":
                return _FakeResponse(
                    200, {"wasApplied": False, "commonAncestor": "same"}
                )
            return _FakeResponse(
                200, {"reference": {"type": "BRANCH", "name": "dev", "hash": "same"}}
            )

        _mock_urlopen(monkeypatch, responder)
        delete_branch("dev")  # must not raise


class TestRollback:
    def test_builds_assign_request(self, monkeypatch):
        calls = []

        def responder(request, timeout=None):
            calls.append((request.full_url, request.get_method(), request.data))
            if request.get_method() == "GET":
                return _FakeResponse(
                    200,
                    {
                        "reference": {
                            "type": "BRANCH",
                            "name": "main",
                            "hash": "current",
                        }
                    },
                )
            return _FakeResponse(
                200,
                {"reference": {"type": "BRANCH", "name": "main", "hash": "old-hash"}},
            )

        _mock_urlopen(monkeypatch, responder)
        rollback("main", to_hash="old-hash")
        put_calls = [c for c in calls if c[1] == "PUT"]
        assert len(put_calls) == 1
        url, _, body = put_calls[0]
        assert "main@current" in url
        assert json.loads(body)["hash"] == "old-hash"
