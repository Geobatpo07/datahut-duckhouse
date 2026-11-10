"""Docker-based end-to-end smoke test: bring up the stack, round-trip a table.

Reusable from CI (.github/workflows/integration.yml) and locally:

    uv run python scripts/smoke_test.py

Brings up minio/xorq-service/flight-server/xorq-admin (not trino -- it
crash-loops on Trino 483's catalog config, see README Known gaps), waits for
them to report healthy, then runs the full create/insert/query/list round
trip with `--engine pyiceberg` (the verified-working path against Docker;
the default `flight` engine's schema-returning calls segfault against a real
Dockerized Flight server + real MinIO -- see README Known gaps -- so they are
deliberately not exercised here), plus a `list-tables` sanity check on the
default engine. Tears the stack down on exit either way.
"""

import shutil
import subprocess
import sys
import time
from pathlib import Path

SERVICES = ["minio", "minio-setup", "xorq", "flight-server", "xorq-admin"]
ENV_PATH = Path(".env")
ENV_EXAMPLE_PATH = Path(".devcontainer/.env.example")


def run(cmd: list[str], **kwargs) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(cmd)}")
    return subprocess.run(cmd, check=True, **kwargs)


def ensure_env_file() -> None:
    if not ENV_PATH.exists():
        shutil.copy(ENV_EXAMPLE_PATH, ENV_PATH)
        print(f"Created {ENV_PATH} from {ENV_EXAMPLE_PATH}")


def read_env_value(key: str) -> str:
    for line in ENV_PATH.read_text().splitlines():
        line = line.strip()
        if line.startswith("#") or "=" not in line:
            continue
        name, _, value = line.partition("=")
        if name.strip() == key:
            return value.strip()
    raise KeyError(f"{key} not found in {ENV_PATH}")


def wait_healthy(timeout: int = 180) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(
            ["docker", "compose", "ps", "--format", "{{.Service}} {{.Health}}"],
            capture_output=True,
            text=True,
            check=True,
        )
        statuses = {}
        for line in result.stdout.splitlines():
            parts = line.split(maxsplit=1)
            if parts:
                statuses[parts[0]] = parts[1] if len(parts) > 1 else ""
        watched = {s: statuses.get(s, "") for s in SERVICES if s != "minio-setup"}
        if all(status == "healthy" for status in watched.values()):
            print(f"All services healthy: {watched}")
            return
        print(f"Waiting for health: {watched}")
        time.sleep(5)
    raise TimeoutError(f"Services did not become healthy within {timeout}s")


def main() -> int:
    import os

    ensure_env_file()
    run(["docker", "compose", "up", "--build", "-d", *SERVICES])
    try:
        wait_healthy()

        table = f"smoke_{int(time.time())}"
        # The pyiceberg engine doesn't read .env (see README), so pass the same
        # S3 credentials docker compose gave the MinIO/flight-server containers --
        # reading them back from .env keeps this correct whether that file is the
        # committed .env.example defaults (CI) or a customized local one.
        pe_env = {
            **os.environ,
            "ICEBERG_WAREHOUSE": "s3://duckhouse-warehouse/",
            "S3_ENDPOINT": "http://localhost:9000",
            "AWS_ACCESS_KEY_ID": read_env_value("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": read_env_value("AWS_SECRET_ACCESS_KEY"),
        }

        run(
            [
                "uv",
                "run",
                "dhd",
                "--engine",
                "pyiceberg",
                "create-table",
                table,
                "data/sales.csv",
            ],
            env=pe_env,
        )
        run(
            [
                "uv",
                "run",
                "dhd",
                "--engine",
                "pyiceberg",
                "insert",
                table,
                "data/sales_new.csv",
                "--mode",
                "append",
            ],
            env=pe_env,
        )
        query = run(
            [
                "uv",
                "run",
                "dhd",
                "--engine",
                "pyiceberg",
                "query",
                f"SELECT * FROM {table}",
            ],
            env=pe_env,
            capture_output=True,
            text=True,
        )
        print(query.stdout)
        if "Dana" not in query.stdout:
            raise RuntimeError("query result missing the appended row")

        listed = run(
            ["uv", "run", "dhd", "--engine", "pyiceberg", "list-tables"],
            env=pe_env,
            capture_output=True,
            text=True,
        )
        if table not in listed.stdout:
            raise RuntimeError(f"{table} missing from list-tables")

        # Default engine (flight): list-tables doesn't round-trip a Schema, so
        # it's safe to exercise -- a regression here means the Flight server
        # itself isn't reachable, not the known segfault.
        run(["uv", "run", "dhd", "list-tables"])

        print("Smoke test passed.")
        return 0
    finally:
        run(["docker", "compose", "down"])


if __name__ == "__main__":
    sys.exit(main())
