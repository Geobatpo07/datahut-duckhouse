"""Tiny local cursor-state store for incremental extraction (task 3).

Not dlt's own pipeline state (this project already avoids that -- see
``pipeline.py``'s module docstring: dlt's filesystem-destination state
bookkeeping doesn't work reliably on Windows). One JSON file per
``(source, resource)`` pair under ``_state/``, holding the highest cursor
value seen so far. Read before extracting (to bound the query), written
after (from the actually-extracted data, not from dlt's own incremental
tracking) so this has no dependency on running inside a full dlt pipeline.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

STATE_DIR = Path(__file__).parent.parent / "_state"


def _state_path(source_name: str, resource_name: str) -> Path:
    return STATE_DIR / f"{source_name}__{resource_name}.json"


def read_last_value(source_name: str, resource_name: str) -> Any | None:
    path = _state_path(source_name, resource_name)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())["last_value"]
    except (OSError, json.JSONDecodeError, KeyError):
        return None


def write_last_value(source_name: str, resource_name: str, value: Any) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    path = _state_path(source_name, resource_name)
    path.write_text(json.dumps({"last_value": value}, default=str))
