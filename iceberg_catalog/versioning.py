"""Nessie's own REST API v2 (ADR-0006, decision 4) -- branch/tag/merge/
rollback. A plain stdlib HTTP client (``urllib.request``), no extra
dependency: this is a handful of small, synchronous JSON calls, not
enough to justify adding ``requests``/``httpx`` as a direct project
dependency just for this.

Distinct from pyiceberg's own catalog API (``load_catalog(...).catalog``),
which only manages per-table snapshot references, not whole-repository
branches/tags/merge/rollback -- verified live (docs/spikes/
NESSIE_SPIKES.md, S0): Nessie's versioning lives at ``.../api/v2``, a
different base path than the Iceberg REST endpoint
(``ICEBERG_CATALOG_URI``, ``.../iceberg``) pyiceberg talks to.

Every public function calls :func:`iceberg_catalog.factory.
require_versioning` first: these commands only make sense in deployment
mode (a Nessie catalog) -- solo mode (a local SQLite catalog, no
reference model) has nothing to branch.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass

from iceberg_catalog.factory import require_versioning

_DEFAULT_TIMEOUT = 10.0


class NessieApiError(RuntimeError):
    """A Nessie REST API v2 call failed in a way callers should surface
    cleanly, not as a raw ``urllib`` traceback."""


class ReferenceNotFoundError(NessieApiError):
    """No branch or tag by that name."""


class ReferenceAlreadyExistsError(NessieApiError):
    """A branch or tag by that name already exists."""


class MergeConflictError(NessieApiError):
    """A merge failed because of a real content conflict (ADR-0006,
    decision 4: ``dhd merge`` never resolves this automatically)."""

    def __init__(self, message: str, conflicting_keys: list[str]) -> None:
        super().__init__(message)
        self.conflicting_keys = conflicting_keys


class UnmergedWorkError(NessieApiError):
    """``dhd branch delete`` refused: the branch has work not yet merged
    into ``main``, and ``--force`` was not given."""


@dataclass(frozen=True)
class Reference:
    type: str  # "BRANCH" | "TAG"
    name: str
    hash: str


def _base_url() -> str:
    """Nessie's versioning API base -- derived from ``ICEBERG_CATALOG_URI``
    (the Iceberg REST endpoint, e.g. ``http://nessie:19120/iceberg``) by
    stripping its ``/iceberg`` suffix, not a separately-configured value:
    both paths are served by the same Nessie process (docs/spikes/
    NESSIE_SPIKES.md, S0).
    """
    catalog_uri = os.environ.get("ICEBERG_CATALOG_URI", "")
    base = (
        catalog_uri[: -len("/iceberg")]
        if catalog_uri.endswith("/iceberg")
        else catalog_uri
    )
    if not base:
        raise NessieApiError(
            "ICEBERG_CATALOG_URI is not set -- versioning needs a reachable "
            "Nessie catalog (deployment mode)."
        )
    return f"{base.rstrip('/')}/api/v2"


def _request(
    method: str, path: str, *, body: dict | None = None
) -> tuple[int, dict | None]:
    url = f"{_base_url()}{path}"
    data = json.dumps(body).encode() if body is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=_DEFAULT_TIMEOUT) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else None)
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        payload = None
        if raw:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {"message": raw.decode(errors="replace")}
        return exc.code, payload
    except urllib.error.URLError as exc:
        raise NessieApiError(f"Nessie unreachable at {url}: {exc}") from exc


def _reference_from(payload: dict) -> Reference:
    return Reference(type=payload["type"], name=payload["name"], hash=payload["hash"])


def get_reference(name: str) -> Reference:
    require_versioning()
    status, body = _request("GET", f"/trees/{name}")
    if status == 404:
        raise ReferenceNotFoundError(f"reference '{name}' does not exist")
    if status != 200 or body is None:
        raise NessieApiError(f"GET /trees/{name} -> {status}: {body}")
    return _reference_from(body["reference"])


def list_references() -> list[Reference]:
    require_versioning()
    status, body = _request("GET", "/trees")
    if status != 200 or body is None:
        raise NessieApiError(f"GET /trees -> {status}: {body}")
    return [_reference_from(r) for r in body["references"]]


def _create_reference(name: str, ref_type: str, *, from_branch: str) -> Reference:
    source = get_reference(from_branch)
    status, body = _request(
        "POST",
        f"/trees?name={name}&type={ref_type}",
        body={"type": "BRANCH", "name": source.name, "hash": source.hash},
    )
    if status == 409:
        kind = "branch" if ref_type == "BRANCH" else "tag"
        raise ReferenceAlreadyExistsError(f"{kind} '{name}' already exists")
    if status != 200 or body is None:
        raise NessieApiError(f"create {ref_type} '{name}' -> {status}: {body}")
    return _reference_from(body["reference"])


def create_branch(name: str, *, from_branch: str = "main") -> Reference:
    """``dhd branch create``."""
    return _create_reference(name, "BRANCH", from_branch=from_branch)


def create_tag(name: str, *, from_branch: str) -> Reference:
    """``dhd tag``."""
    return _create_reference(name, "TAG", from_branch=from_branch)


def is_fully_merged(ref: Reference, *, into: str = "main") -> bool:
    """Whether every commit on ``ref`` is already reachable from ``into``
    -- a dry-run merge's ``commonAncestor`` equals ``ref``'s own hash
    exactly when nothing on ``ref`` is still unmerged (verified live: an
    unmerged branch's dry-run reports ``commonAncestor`` as an *older*
    commit than ``ref.hash``, with ``wasApplied: true`` and the
    conflicting/mergeable keys in ``details``; an already-merged branch
    reports ``commonAncestor == ref.hash`` and ``wasApplied: false``).
    """
    target = get_reference(into)
    status, body = _request(
        "POST",
        f"/trees/{into}@{target.hash}/history/merge",
        body={"fromRefName": ref.name, "fromHash": ref.hash, "dryRun": True},
    )
    if status != 200 or body is None:
        # A dry run that itself fails (e.g. a real conflict) is certainly
        # not "fully merged".
        return False
    return body.get("commonAncestor") == ref.hash


def delete_branch(name: str, *, force: bool = False) -> None:
    """``dhd branch delete``. Refuses a branch with unmerged work unless
    ``force`` -- the files of that unmerged work are left in storage
    (``nessie-gc`` never visits a deleted branch's commits -- docs/
    spikes/NESSIE_SPIKES_2.md, T2), so the caller should know that.
    """
    require_versioning()
    if name == "main":
        raise NessieApiError("refusing to delete 'main'.")
    ref = get_reference(name)
    if not force and not is_fully_merged(ref):
        raise UnmergedWorkError(
            f"branch '{name}' has unmerged work -- refusing to delete "
            f"without --force. The files of that unmerged work are left "
            f"in storage: nessie-gc does not visit a deleted branch's "
            f"commits (docs/spikes/NESSIE_SPIKES_2.md, T2). Merge it "
            f"first, or pass --force if you mean to abandon it."
        )
    status, body = _request("DELETE", f"/trees/{name}@{ref.hash}")
    if status != 200:
        raise NessieApiError(f"delete '{name}' -> {status}: {body}")


def merge(from_branch: str, *, into: str = "main") -> None:
    """``dhd merge``. Fails on any conflict -- never resolves one
    automatically (ADR-0006, decision 4; docs/spikes/NESSIE_SPIKES_2.md,
    T3: Nessie's own ``FORCE`` resolution overwrites instead of combining,
    so it is never used here, nor exposed).
    """
    require_versioning()
    target = get_reference(into)
    source = get_reference(from_branch)
    status, body = _request(
        "POST",
        f"/trees/{into}@{target.hash}/history/merge",
        body={"fromRefName": source.name, "fromHash": source.hash},
    )
    if status == 409 and body is not None:
        message = body.get("message", "merge conflict")
        keys = [
            key.strip(" '")
            for key in message.partition(":")[2].split(",")
            if key.strip(" '")
        ]
        raise MergeConflictError(message, keys)
    if status != 200 or body is None or not body.get("wasSuccessful", False):
        raise NessieApiError(f"merge '{from_branch}' -> '{into}': {status}: {body}")


def rollback(branch: str, *, to_hash: str) -> None:
    """``dhd rollback``: reassign ``branch``'s HEAD to ``to_hash`` --
    Nessie's own ``assignReference``. ``to_hash`` must be a commit
    reachable from ``branch``'s own history (a past hash of the same
    branch, from ``dhd branch log`` or an earlier ``dhd branch`` output).
    """
    require_versioning()
    current = get_reference(branch)
    status, body = _request(
        "PUT",
        f"/trees/{branch}@{current.hash}?type={current.type}",
        body={"type": current.type, "name": branch, "hash": to_hash},
    )
    if status != 200:
        raise NessieApiError(f"rollback '{branch}' to {to_hash} -> {status}: {body}")
