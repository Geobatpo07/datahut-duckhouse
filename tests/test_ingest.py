"""Tests for the serverless ``pyiceberg`` ingestion engine (``dhd --engine pyiceberg``).

These run fully offline: a local ``file://`` Iceberg warehouse in a temp dir,
its own SQLite catalog, an in-process DuckDB for reads. No Flight server, no
xorq, no MinIO.
"""

from __future__ import annotations

import pytest
from click.testing import CliRunner

from datahut_duckhouse import cli as cli_mod


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def pyiceberg_env(monkeypatch, tmp_path):
    """Point the engine at a throwaway local warehouse."""
    monkeypatch.setenv("ICEBERG_WAREHOUSE", "")  # empty, not unset (see test_cli)
    monkeypatch.setenv("ICEBERG_WAREHOUSE_PATH", str(tmp_path / "wh"))
    monkeypatch.setenv("ICEBERG_NAMESPACE", "default")
    monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
    monkeypatch.delenv("DHD_ENGINE", raising=False)


@pytest.fixture
def people_csv(tmp_path):
    p = tmp_path / "people.csv"
    p.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
    return str(p)


def _run(runner, *args):
    return runner.invoke(cli_mod.cli, ["--engine", "pyiceberg", *args])


def test_create_list_query_cycle(runner, pyiceberg_env, people_csv):
    r = _run(runner, "create-table", "people", people_csv)
    assert r.exit_code == 0, r.output
    assert "3 rows" in r.output

    r = _run(runner, "list-tables")
    assert r.output.strip() == "people"

    r = _run(runner, "query", "SELECT count(*) AS n FROM people", "--format", "json")
    assert r.exit_code == 0, r.output
    assert '"n": 3' in r.output


def test_insert_append_then_overwrite(runner, pyiceberg_env, people_csv):
    _run(runner, "create-table", "people", people_csv)

    r = _run(runner, "insert", "people", people_csv)
    assert r.exit_code == 0, r.output
    r = _run(runner, "query", "SELECT count(*) AS n FROM people", "--format", "json")
    assert '"n": 6' in r.output

    r = _run(runner, "insert", "people", people_csv, "--mode", "overwrite")
    assert r.exit_code == 0, r.output
    r = _run(runner, "query", "SELECT count(*) AS n FROM people", "--format", "json")
    assert '"n": 3' in r.output


def test_query_limit(runner, pyiceberg_env, tmp_path):
    src = tmp_path / "nums.csv"
    src.write_text("n\n" + "\n".join(str(i) for i in range(20)) + "\n")
    _run(runner, "create-table", "nums", str(src))
    r = _run(runner, "query", "SELECT * FROM nums", "--limit", "5")
    assert r.exit_code == 0, r.output
    assert "(5 rows)" in r.output


def test_create_existing_is_rejected(runner, pyiceberg_env, people_csv):
    _run(runner, "create-table", "people", people_csv)
    r = _run(runner, "create-table", "people", people_csv)
    assert r.exit_code != 0
    assert "already exists" in r.output


def test_insert_missing_table_is_rejected(runner, pyiceberg_env, people_csv):
    r = _run(runner, "insert", "ghost", people_csv)
    assert r.exit_code != 0
    assert "does not exist" in r.output


def test_list_tables_empty(runner, pyiceberg_env):
    r = _run(runner, "list-tables")
    assert r.exit_code == 0
    assert "no tables" in r.output


def test_engine_selected_via_env(runner, pyiceberg_env, monkeypatch, people_csv):
    monkeypatch.setenv("DHD_ENGINE", "pyiceberg")
    r = runner.invoke(cli_mod.cli, ["create-table", "people", people_csv])
    assert r.exit_code == 0, r.output
    r = runner.invoke(cli_mod.cli, ["list-tables"])
    assert r.output.strip() == "people"
