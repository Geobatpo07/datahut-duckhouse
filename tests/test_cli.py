"""
Tests for the ``dhd`` CLI (datahut_duckhouse.cli).

Two layers:

* ``TestCliBehaviour`` — click-level: argument parsing, error messages, source
  loading, the Nessie ``branch`` stubs, the "server not reachable" path. The
  Flight backend is replaced by a stand-in, but only for the *transport* — the
  CLI logic under test is real.
* ``TestCliEndToEnd`` — spins a real in-process Flight server backed by a real
  ``HybridBackend`` on a temp local warehouse, and drives it through the actual
  CLI. This is the path the roadmap Phase 2 cares about: network + Iceberg +
  reflected DuckDB views, no mocking of the xorq API.
"""

from __future__ import annotations

import socket
import time

import pyarrow as pa
import pytest
from click.testing import CliRunner

from datahut_duckhouse import cli as cli_mod
from datahut_duckhouse.connection import FlightUnavailableError


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def people_csv(tmp_path):
    p = tmp_path / "people.csv"
    p.write_text("id,name\n1,Alice\n2,Bob\n3,Charlie\n")
    return str(p)


class _FakeBackend:
    """Minimal stand-in for the Flight-backed ibis backend."""

    def __init__(self):
        self._tables: dict[str, pa.Table] = {}
        self.con = self  # upload_table lives on `.con` on the real object

    def list_tables(self):
        return list(self._tables)

    def create_table(self, name, data):
        self._tables[name] = data

    def upload_table(self, name, data, mode="append"):
        if mode == "overwrite":
            self._tables[name] = data
        else:
            self._tables[name] = pa.concat_tables([self._tables[name], data])

    def sql(self, query):  # only exercised in the e2e class
        raise AssertionError("sql() should not be called in behaviour tests")


class TestCliBehaviour:
    @pytest.fixture(autouse=True)
    def _patch_connect(self, monkeypatch):
        self.backend = _FakeBackend()
        monkeypatch.setattr(cli_mod, "connect", lambda: self.backend)

    def test_list_tables_empty(self, runner):
        result = runner.invoke(cli_mod.cli, ["list-tables"])
        assert result.exit_code == 0
        assert "no tables" in result.output

    def test_create_then_list(self, runner, people_csv):
        assert (
            runner.invoke(cli_mod.cli, ["create-table", "people", people_csv]).exit_code
            == 0
        )
        result = runner.invoke(cli_mod.cli, ["list-tables"])
        assert result.output.strip() == "people"

    def test_create_existing_is_rejected(self, runner, people_csv):
        runner.invoke(cli_mod.cli, ["create-table", "people", people_csv])
        result = runner.invoke(cli_mod.cli, ["create-table", "people", people_csv])
        assert result.exit_code != 0
        assert "already exists" in result.output

    def test_insert_missing_table_is_rejected(self, runner, people_csv):
        result = runner.invoke(cli_mod.cli, ["insert", "ghost", people_csv])
        assert result.exit_code != 0
        assert "does not exist" in result.output

    def test_insert_append_and_overwrite(self, runner, people_csv):
        runner.invoke(cli_mod.cli, ["create-table", "people", people_csv])
        runner.invoke(cli_mod.cli, ["insert", "people", people_csv])
        assert self.backend._tables["people"].num_rows == 6
        runner.invoke(
            cli_mod.cli, ["insert", "people", people_csv, "--mode", "overwrite"]
        )
        assert self.backend._tables["people"].num_rows == 3

    def test_unknown_source_extension(self, runner, tmp_path):
        bad = tmp_path / "data.txt"
        bad.write_text("nope")
        result = runner.invoke(cli_mod.cli, ["create-table", "t", str(bad)])
        assert result.exit_code != 0
        assert "Unsupported source type" in result.output

    def test_missing_source_file(self, runner):
        result = runner.invoke(cli_mod.cli, ["create-table", "t", "nope.csv"])
        assert result.exit_code != 0
        assert "not found" in result.output

    @pytest.mark.parametrize(
        "args",
        [["branch", "create", "x"], ["branch", "list"], ["branch", "switch", "x"]],
    )
    def test_branch_is_a_documented_stub(self, runner, args):
        result = runner.invoke(cli_mod.cli, args)
        assert result.exit_code != 0
        assert "Nessie" in result.output and "Phase 3" in result.output

    def test_server_unreachable_is_a_clean_error(self, runner, monkeypatch):
        def boom():
            raise FlightUnavailableError("No Flight server at localhost:8815")

        monkeypatch.setattr(cli_mod, "connect", boom)
        result = runner.invoke(cli_mod.cli, ["list-tables"])
        assert result.exit_code != 0
        assert "No Flight server" in result.output


@pytest.fixture(scope="module")
def flight_server(tmp_path_factory):
    """A real in-process Flight server backed by a real HybridBackend."""
    import os

    d = tmp_path_factory.mktemp("dhd_e2e")
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    old = {
        k: os.environ.get(k)
        for k in (
            "ICEBERG_WAREHOUSE",
            "ICEBERG_WAREHOUSE_PATH",
            "DUCKDB_PATH",
            "FLIGHT_SERVER_HOST",
            "FLIGHT_SERVER_PORT",
        )
    }
    os.environ.pop("ICEBERG_WAREHOUSE", None)
    os.environ["ICEBERG_WAREHOUSE_PATH"] = str(d / "wh")
    os.environ["DUCKDB_PATH"] = str(d / "duckhouse.duckdb")
    os.environ["FLIGHT_SERVER_HOST"] = "127.0.0.1"
    os.environ["FLIGHT_SERVER_PORT"] = str(port)

    from flight_server.app.xorq_config import get_flight_server

    server = get_flight_server()
    server.serve(block=False)
    time.sleep(1)

    yield port

    try:
        server.close()
    except Exception:  # noqa: BLE001 - best effort teardown
        pass
    for k, v in old.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v


class TestCliEndToEnd:
    def test_full_ingest_and_query_cycle(self, runner, flight_server, tmp_path):
        src = tmp_path / "sales.csv"
        src.write_text("id,amount\n1,10\n2,20\n3,30\n")
        more = tmp_path / "more.csv"
        more.write_text("id,amount\n4,40\n")

        r = runner.invoke(cli_mod.cli, ["create-table", "sales", str(src)])
        assert r.exit_code == 0, r.output

        r = runner.invoke(cli_mod.cli, ["list-tables"])
        assert "sales" in r.output

        r = runner.invoke(cli_mod.cli, ["insert", "sales", str(more)])
        assert r.exit_code == 0, r.output

        r = runner.invoke(
            cli_mod.cli,
            [
                "query",
                "SELECT count(*) AS n, sum(amount) AS total FROM sales",
                "--format",
                "json",
            ],
        )
        assert r.exit_code == 0, r.output
        assert '"n": 4' in r.output
        assert '"total": 100' in r.output

    def test_query_limit(self, runner, flight_server, tmp_path):
        src = tmp_path / "nums.csv"
        src.write_text("n\n" + "\n".join(str(i) for i in range(20)) + "\n")
        runner.invoke(cli_mod.cli, ["create-table", "nums", str(src)])
        r = runner.invoke(cli_mod.cli, ["query", "SELECT * FROM nums", "--limit", "5"])
        assert r.exit_code == 0, r.output
        assert "(5 rows)" in r.output
