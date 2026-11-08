"""Download the Chinook SQLite sample database, pinned to a fixed commit.

Not vendored as a binary in the repo -- fetched on demand into ``tests/data/``
so ``dhd demo`` and the SQLite integration test work from a fresh clone with
no Docker required. See docs/THIRD_PARTY_CHINOOK.md for the license and the
pinned commit.

    uv run python scripts/fetch_chinook.py
"""

from __future__ import annotations

import sys
import urllib.request
from pathlib import Path

CHINOOK_COMMIT = "7f67772503d71ba90f19283c38e93923addb43fa"
SQLITE_URL = (
    "https://raw.githubusercontent.com/lerocha/chinook-database/"
    f"{CHINOOK_COMMIT}/ChinookDatabase/DataSources/Chinook_Sqlite.sqlite"
)
DEST = Path("tests/data/Chinook_Sqlite.sqlite")


def fetch(dest: Path = DEST, *, force: bool = False) -> Path:
    if dest.exists() and not force:
        print(f"{dest} already present, skipping download.")
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {SQLITE_URL} -> {dest}")
    try:
        urllib.request.urlretrieve(SQLITE_URL, dest)
    except OSError as exc:
        raise RuntimeError(
            f"Could not download the Chinook SQLite database from "
            f"{SQLITE_URL}: {exc}"
        ) from exc
    size = dest.stat().st_size
    print(f"Downloaded {dest} ({size} bytes).")
    return dest


def main() -> int:
    fetch(force="--force" in sys.argv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
