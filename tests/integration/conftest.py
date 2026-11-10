"""Fixtures for the multi-engine Chinook integration tests.

Needs:
    docker compose -f docker-compose.test.yml up -d
    uv run python scripts/fetch_chinook.py
    uv run pytest -m integration

Each `require_*` fixture skips with a clear message (naming the exact
command to fix it) instead of failing when its source isn't reachable,
rather than erroring the whole module if only one engine is unavailable.
"""

from __future__ import annotations

import importlib.util
import os
import socket
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent.parent

# Match docker-compose.test.yml's throwaway local test credentials/ports --
# not secrets, just this suite's convenience defaults. Real env vars (e.g.
# in CI) still take precedence via setdefault.
_ENV_DEFAULTS = {
    "CHINOOK_SQLITE_URL": "sqlite:///tests/data/Chinook_Sqlite.sqlite",
    "CHINOOK_POSTGRES_URL": "postgresql+psycopg2://chinook:chinook@localhost:5433/chinook",
    "CHINOOK_MYSQL_URL": "mysql+pymysql://chinook:chinook@localhost:3307/Chinook",
}


@pytest.fixture(scope="session", autouse=True)
def _default_chinook_env():
    for key, value in _ENV_DEFAULTS.items():
        os.environ.setdefault(key, value)


def _load_pipeline_module():
    spec = importlib.util.spec_from_file_location(
        "dlt_pipeline_integration", REPO_ROOT / "ingestion" / "dlt" / "pipeline.py"
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def pipeline():
    return _load_pipeline_module()


def _port_open(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


@pytest.fixture(scope="session")
def require_sqlite():
    if not (REPO_ROOT / "tests" / "data" / "Chinook_Sqlite.sqlite").exists():
        pytest.skip(
            "tests/data/Chinook_Sqlite.sqlite missing -- run "
            "`uv run python scripts/fetch_chinook.py`"
        )


@pytest.fixture(scope="session")
def require_postgres():
    if not _port_open("localhost", 5433):
        pytest.skip(
            "chinook-postgres not reachable on localhost:5433 -- run "
            "`docker compose -f docker-compose.test.yml up -d`"
        )


@pytest.fixture(scope="session")
def require_mysql():
    if not _port_open("localhost", 3307):
        pytest.skip(
            "chinook-mysql not reachable on localhost:3307 -- run "
            "`docker compose -f docker-compose.test.yml up -d`"
        )


_REQUIRES = {
    "sqlite_chinook": "require_sqlite",
    "pg_chinook": "require_postgres",
    "mysql_chinook": "require_mysql",
}


@pytest.fixture(params=list(_REQUIRES))
def source_name(request):
    request.getfixturevalue(_REQUIRES[request.param])
    return request.param


@pytest.fixture
def source(pipeline, source_name):
    return pipeline.get_source(source_name)
