"""Regression guard for the segfault diagnosed on fix/segfault-diagnosis.

Root cause (verified against a real Dockerized flight-server + real MinIO,
see docs/adr/0001-canonical-hybrid-backend.md's "Segfault root cause" note
and docs/UPSTREAM_ISSUE.md): any Flight action that round-trips a
cloudpickled ``xorq.vendor.ibis`` ``Schema``/``Annotable`` object -- which
``dhd create-table``'s confirmation step and `dhd query` both do, via
``get_schema_using_query`` -- segfaults the client when the client's Python
*minor* version differs from the flight-server image's. Matching versions
(verified in both directions: host Python 3.11 <-> container Python 3.11,
and two separate Python-3.12 processes on localhost) never crash; a 3.12
client against the 3.11-pinned image is 100% reproducible (exit 139 /
``Windows fatal exception: access violation`` on Windows).

There is no fix available inside this repo for cloudpickle's cross-version
fragility itself (that lives upstream, in xorq's vendored ibis + cloudpickle
-- see docs/UPSTREAM_ISSUE.md). The actionable mitigation is keeping the
dev/CI Python version in lockstep with the image's pinned
``ARG PYTHON_VERSION`` in .devcontainer/flight-server.Dockerfile. This test
turns a silent future segfault into a loud, actionable assertion the moment
that lockstep breaks, instead of waiting for someone to hit the crash by
hand.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

DOCKERFILE = (
    Path(__file__).resolve().parent.parent
    / ".devcontainer"
    / "flight-server.Dockerfile"
)


def _pinned_python_version() -> str:
    text = DOCKERFILE.read_text()
    match = re.search(r"ARG PYTHON_VERSION=(\d+\.\d+)", text)
    assert match, f"Could not find 'ARG PYTHON_VERSION=X.Y' in {DOCKERFILE}"
    return match.group(1)


def test_host_python_matches_flight_server_image():
    pinned = _pinned_python_version()
    host = f"{sys.version_info.major}.{sys.version_info.minor}"
    assert host == pinned, (
        f"Host Python is {host}, but {DOCKERFILE} pins PYTHON_VERSION="
        f"{pinned}. Running `dhd` (or anything using xorq.flight) against a "
        f"real Dockerized flight-server with mismatched Python minor "
        f"versions segfaults the client when a Flight action round-trips a "
        f"cloudpickled ibis Schema -- see docs/UPSTREAM_ISSUE.md and "
        f"docs/adr/0001-canonical-hybrid-backend.md. Fix: either pin this "
        f"host to Python {pinned} (e.g. `.python-version` + `uv sync`) or "
        f"rebuild the flight-server image with `--build-arg "
        f"PYTHON_VERSION={host}`, then re-verify with "
        f"`uv run python scripts/repro_segfault.py`."
    )
