"""Matrix sanity check for the Python versions this project builds/tests on.

History (chore/python-matrix): this test used to require the host's Python
*exactly* equal the flight-server Dockerfile's single ``ARG
PYTHON_VERSION`` default, as a stand-in for the segfault guard documented in
docs/adr/0001-canonical-hybrid-backend.md's "Segfault root cause" note. That
made CI red on every host whose Python minor version differed from
whichever single default the Dockerfile happened to pin -- which stopped
being the right check the moment the project started supporting *two*
Python versions (3.11 and 3.13) with one Dockerfile, built either way via
``--build-arg PYTHON_VERSION=<version>`` (see README's Python-version
matrix section).

The actual runtime protection against the cross-version cloudpickle
segfault now lives in ``datahut_duckhouse.flight_version_guard`` (every
``dhd`` command that talks to the Flight server checks the server's
version before issuing anything risky, and refuses cleanly on a mismatch --
see ``tests/test_flight_version_guard.py``). This test's job shrank to a
narrower, still-useful one: catch *matrix drift* -- the host running an
untested Python version, or the Dockerfile's default drifting to one --
before either reaches CI red for the wrong reason.
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

# The versions this project actually builds the flight-server image for and
# runs the test suite on (see README, ci.yml, integration.yml). Kept as an
# explicit, separate list from pyproject.toml's `requires-python` range
# (">=3.11,<3.14"), which is deliberately a permissive floor/ceiling, not a
# claim that every version in between is built or tested.
SUPPORTED_PYTHON_VERSIONS = ("3.11", "3.13")


def _dockerfile_default_python_version() -> str:
    text = DOCKERFILE.read_text()
    match = re.search(r"ARG PYTHON_VERSION=(\d+\.\d+)", text)
    assert match, f"Could not find 'ARG PYTHON_VERSION=X.Y' in {DOCKERFILE}"
    return match.group(1)


def test_host_python_is_in_the_supported_matrix():
    host = f"{sys.version_info.major}.{sys.version_info.minor}"
    assert host in SUPPORTED_PYTHON_VERSIONS, (
        f"Host Python is {host}, which is not one of the versions this "
        f"project builds/tests on ({', '.join(SUPPORTED_PYTHON_VERSIONS)}). "
        f"Either switch to one of those (e.g. `uv venv --python "
        f"{SUPPORTED_PYTHON_VERSIONS[-1]}`), or add it to "
        f"SUPPORTED_PYTHON_VERSIONS here *and* to ci.yml / integration.yml's "
        f"matrices and the README's Python-version section once it's "
        f"actually been built and tested against a real flight-server "
        f"image, not just added to pyproject.toml's requires-python range."
    )


def test_flight_server_dockerfile_default_is_in_the_supported_matrix():
    default = _dockerfile_default_python_version()
    assert default in SUPPORTED_PYTHON_VERSIONS, (
        f"{DOCKERFILE} defaults ARG PYTHON_VERSION to {default}, which is "
        f"not one of the versions this project builds/tests on "
        f"({', '.join(SUPPORTED_PYTHON_VERSIONS)}). This does NOT need to "
        f"equal the host's own Python version -- a client and server on "
        f"different supported versions is fine as long as both sides are "
        f"actually the same version at *runtime*, which "
        f"datahut_duckhouse.flight_version_guard checks and refuses on a "
        f"mismatch (see tests/test_flight_version_guard.py). This test only "
        f"catches the default drifting to an untested version."
    )
