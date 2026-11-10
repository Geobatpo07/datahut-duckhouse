"""Tiny local cursor-state store for incremental extraction (task 3).

Not dlt's own pipeline state (this project already avoids that -- see
``engine/dlt_engine.py``'s module docstring: dlt's filesystem-destination
state bookkeeping doesn't work reliably on Windows). One JSON file per
``(source, resource)`` pair, holding the highest cursor value seen so far.
Read before extracting (to bound the query), written after (from the
actually-extracted data, not from dlt's own incremental tracking) so this
has no dependency on running inside a full dlt pipeline.

The directory is configurable (``DHD_INGEST_STATE_DIR``) -- a safe default
under the repo's ``ingestion/`` tree works on every platform including
Windows, where a dotfile under the user's home directory is unconventional.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

_DEFAULT_STATE_DIR = Path(__file__).resolve().parent.parent / "_state"


def _state_dir() -> Path:
    override = os.getenv("DHD_INGEST_STATE_DIR")
    return Path(override) if override else _DEFAULT_STATE_DIR


def _state_path(source_name: str, resource_name: str) -> Path:
    return _state_dir() / f"{source_name}__{resource_name}.json"


def read_last_value(source_name: str, resource_name: str) -> Any | None:
    path = _state_path(source_name, resource_name)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())["last_value"]
    except (OSError, json.JSONDecodeError, KeyError):
        return None


def write_last_value(source_name: str, resource_name: str, value: Any) -> None:
    path = _state_path(source_name, resource_name)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"last_value": value}, default=str))


def clear_source(source_name: str) -> None:
    """Drop every tracked cursor for ``source_name`` (``--full-refresh``):
    the next extraction has no ``last_value`` to bound it, so it pulls
    everything from scratch, same as a brand-new source's first run."""
    for path in _state_dir().glob(f"{source_name}__*.json"):
        path.unlink(missing_ok=True)
