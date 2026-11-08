"""Tests for ``ingestion/cli.py`` -- the ``dhd ingest`` command group
(step B of the ``feat/dhd-ingest`` restructuring).

Two layers, same split as ``tests/test_cli.py``:

* ``TestOptionsAndExitCodes`` -- click-level: argument parsing, exit codes,
  --all/--fail-fast, secret redaction. ``ingestion``'s own functions are
  replaced by stand-ins (monkeypatched as attributes on the ``ingestion``
  module, which is exactly how ``ingestion/cli.py`` calls them) so these
  run with no registry file, no Flight server, no network.
* ``TestRealFlightServer`` -- the real registry, the real ``sqlite_chinook``
  source, and a real in-process Flight server (the shared ``flight_server``
  fixture): dry-run writes nothing (verified by snapshot count), a real
  run is visible via `dhd query`, --table narrows extraction, and
  `dhd ingest list`/`dhd ingest runs` reflect it.
"""

from __future__ import annotations

import os

import pytest
from click.testing import CliRunner

import ingestion
from datahut_duckhouse import cli as cli_mod


@pytest.fixture
def runner():
    return CliRunner()


def _fake_source(**overrides):
    defaults = {
        "name": "s",
        "family": "files",
        "connector": "local_file",
        "path": "x.csv",
        "format": "csv",
    }
    defaults.update(overrides)
    return ingestion.SourceConfig(**defaults)


class TestOptionsAndExitCodes:
    def test_run_needs_either_source_or_all(self, runner):
        r = runner.invoke(cli_mod.cli, ["ingest", "run"])
        assert r.exit_code != 0
        assert "SOURCE_NAME or --all" in r.output

    def test_run_rejects_both_source_and_all(self, runner, monkeypatch):
        monkeypatch.setattr(ingestion, "list_sources", lambda: [_fake_source()])
        r = runner.invoke(cli_mod.cli, ["ingest", "run", "s", "--all"])
        assert r.exit_code != 0
        assert "not both" in r.output

    def test_run_unknown_source_exits_2(self, runner, monkeypatch):
        monkeypatch.setattr(ingestion, "list_sources", lambda: [])
        r = runner.invoke(cli_mod.cli, ["ingest", "run", "ghost"])
        assert r.exit_code == 2
        assert "ghost" in r.output

    def test_run_missing_secret_exits_2(self, runner, monkeypatch):
        source = _fake_source(
            family="sql",
            connector="sql_database",
            dialect="sqlite",
            secret_ref="X_URL",
        )
        monkeypatch.setattr(ingestion, "list_sources", lambda: [source])

        def _boom_connect(*a, **kw):
            raise AssertionError("should not connect for a config error")

        monkeypatch.setattr(cli_mod, "connect", _boom_connect)
        monkeypatch.setattr(
            ingestion,
            "run_source",
            lambda *a, **kw: (_ for _ in ()).throw(
                ingestion.SourcesConfigError("env var 'X_URL' is not set")
            ),
        )
        r = runner.invoke(cli_mod.cli, ["ingest", "run", "s"])
        assert r.exit_code == 2
        assert "X_URL" in r.output

    def test_run_success_reports_each_table(self, runner, monkeypatch):
        source = _fake_source()
        monkeypatch.setattr(ingestion, "list_sources", lambda: [source])
        monkeypatch.setattr(cli_mod, "connect", lambda: object())
        result = ingestion.RunResult(
            source="s",
            tables=[ingestion.TableResult(table="s_t", verb="created", rows=3)],
        )
        monkeypatch.setattr(ingestion, "run_source", lambda *a, **kw: result)
        r = runner.invoke(cli_mod.cli, ["ingest", "run", "s"])
        assert r.exit_code == 0, r.output
        assert "created 's_t' - 3 rows" in r.output

    def test_all_continues_past_one_failure_and_reports_summary(
        self, runner, monkeypatch
    ):
        sources = [_fake_source(name="a"), _fake_source(name="b")]
        monkeypatch.setattr(ingestion, "list_sources", lambda: sources)
        monkeypatch.setattr(cli_mod, "connect", lambda: object())

        def _run_source(name, **kw):
            if name == "a":
                raise ingestion.ConnectorError("'a': boom")
            return ingestion.RunResult(
                source=name,
                tables=[ingestion.TableResult(table="b_t", verb="created", rows=1)],
            )

        monkeypatch.setattr(ingestion, "run_source", _run_source)
        r = runner.invoke(cli_mod.cli, ["ingest", "run", "--all"])
        assert r.exit_code == 1  # at least one source failed
        assert "1/2 source(s) succeeded" in r.output
        assert "a: FAILED" in r.output
        assert "created 'b_t'" in r.output  # b still ran

    def test_fail_fast_stops_at_the_first_failure(self, runner, monkeypatch):
        sources = [_fake_source(name="a"), _fake_source(name="b")]
        monkeypatch.setattr(ingestion, "list_sources", lambda: sources)
        monkeypatch.setattr(cli_mod, "connect", lambda: object())
        calls: list[str] = []

        def _run_source(name, **kw):
            calls.append(name)
            raise ingestion.ConnectorError(f"'{name}': boom")

        monkeypatch.setattr(ingestion, "run_source", _run_source)
        r = runner.invoke(cli_mod.cli, ["ingest", "run", "--all", "--fail-fast"])
        assert r.exit_code == 1
        assert calls == ["a"]  # never reached "b"

    def test_family_filter_narrows_all(self, runner, monkeypatch):
        sources = [
            _fake_source(name="a", family="files"),
            _fake_source(
                name="b", family="rest", base_url="https://x", resources=["r"]
            ),
        ]
        monkeypatch.setattr(ingestion, "list_sources", lambda: sources)
        monkeypatch.setattr(cli_mod, "connect", lambda: object())
        calls: list[str] = []

        def _run_source(name, **kw):
            calls.append(name)
            return ingestion.RunResult(source=name, tables=[])

        monkeypatch.setattr(ingestion, "run_source", _run_source)
        r = runner.invoke(cli_mod.cli, ["ingest", "run", "--all", "--family", "files"])
        assert r.exit_code == 0, r.output
        assert calls == ["a"]

    def test_secret_value_never_appears_in_output(self, runner, monkeypatch):
        """A connector failure that happens to echo the raw secret value is
        redacted before it reaches the run log or the CLI (point: "aucun
        secret dans les logs... messages; test avec une fausse valeur de
        secret")."""
        monkeypatch.setenv("FAKE_SECRET_URL", "postgresql://u:TOPSECRET123@host/db")
        source = _fake_source(
            name="s",
            family="sql",
            connector="sql_database",
            dialect="sqlite",
            secret_ref="FAKE_SECRET_URL",
        )
        monkeypatch.setattr(ingestion, "list_sources", lambda: [source])
        monkeypatch.setattr(cli_mod, "connect", lambda: object())

        def _run_source(name, **kw):
            try:
                raise RuntimeError(
                    "connection refused: postgresql://u:TOPSECRET123@host/db"
                )
            except RuntimeError as exc:
                raise ingestion.ConnectorError(source.redact(str(exc))) from exc

        monkeypatch.setattr(ingestion, "run_source", _run_source)
        r = runner.invoke(cli_mod.cli, ["ingest", "run", "s"])
        assert r.exit_code == 1
        assert "TOPSECRET123" not in r.output
        assert "***" in r.output

    def test_dry_run_reports_without_connecting(self, runner, monkeypatch):
        source = _fake_source()
        monkeypatch.setattr(ingestion, "list_sources", lambda: [source])

        def _boom_connect():
            raise AssertionError("dry-run must not connect to Flight")

        monkeypatch.setattr(cli_mod, "connect", _boom_connect)
        result = ingestion.RunResult(
            source="s",
            tables=[ingestion.TableResult(table="s_t", verb="planned", rows=5)],
        )
        monkeypatch.setattr(ingestion, "dry_run_source", lambda *a, **kw: result)
        r = runner.invoke(cli_mod.cli, ["ingest", "run", "s", "--dry-run"])
        assert r.exit_code == 0, r.output
        assert "would write 's_t' - 5 rows" in r.output
        assert "nothing written" in r.output

    def test_full_refresh_asks_for_confirmation(self, runner, monkeypatch):
        monkeypatch.setattr(ingestion, "list_sources", lambda: [_fake_source()])
        r = runner.invoke(
            cli_mod.cli, ["ingest", "run", "s", "--full-refresh"], input="n\n"
        )
        assert r.exit_code == 0, r.output
        assert "Aborted" in r.output

    def test_ingest_dlt_alias_warns_and_still_works(self, runner, monkeypatch):
        # datahut_duckhouse.ingest_dlt does `from ingestion import
        # list_sources, run_source` -- its own bindings, independent of
        # `ingestion.list_sources`/`.run_source` once imported. Patch its
        # own module's names directly, not the ingestion package's.
        import datahut_duckhouse.ingest_dlt as ingest_dlt_mod

        monkeypatch.setattr(ingest_dlt_mod, "list_sources", lambda: [_fake_source()])
        monkeypatch.setattr(cli_mod, "connect", lambda: object())
        result = ingestion.RunResult(
            source="s",
            tables=[ingestion.TableResult(table="s_t", verb="created", rows=1)],
        )
        monkeypatch.setattr(ingest_dlt_mod, "run_source", lambda *a, **kw: result)
        r = runner.invoke(cli_mod.cli, ["ingest-dlt", "--source", "s"])
        assert r.exit_code == 0, r.output
        assert "deprecated" in r.output
        assert "Created 's_t' - 1 rows" in r.output  # ingest-dlt's own old format

    def test_sources_validate_alias_warns_and_still_works(self, runner):
        r = runner.invoke(cli_mod.cli, ["sources", "validate"])
        assert r.exit_code == 0, r.output
        assert "deprecated" in r.output
        assert "source(s) valid" in r.output


class TestRealFlightServer:
    """Real registry (`ingestion/config/sources.yml`), real connectors, a
    real in-process Flight server -- no Docker, no S3 credentials."""

    @pytest.fixture(autouse=True)
    def _sqlite_env(self, flight_server):
        os.environ.setdefault(
            "CHINOOK_SQLITE_URL", "sqlite:///tests/data/Chinook_Sqlite.sqlite"
        )

    def test_dry_run_writes_nothing(self, runner):
        r = runner.invoke(cli_mod.cli, ["list-tables"])
        assert "sqlite_chinook_Artist" not in r.output

        r = runner.invoke(
            cli_mod.cli,
            ["ingest", "run", "sqlite_chinook", "--table", "Artist", "--dry-run"],
        )
        assert r.exit_code == 0, r.output
        assert "would write 'sqlite_chinook_Artist' - 275 rows" in r.output

        r = runner.invoke(cli_mod.cli, ["list-tables"])
        assert "sqlite_chinook_Artist" not in r.output  # still nothing written

    def test_run_then_list_and_runs(self, runner):
        r = runner.invoke(
            cli_mod.cli, ["ingest", "run", "sqlite_chinook", "--table", "Artist"]
        )
        assert r.exit_code == 0, r.output
        assert "created 'sqlite_chinook_Artist' - 275 rows" in r.output

        r = runner.invoke(
            cli_mod.cli,
            [
                "query",
                "SELECT count(*) AS n FROM sqlite_chinook_Artist",
                "--format",
                "json",
            ],
        )
        assert r.exit_code == 0, r.output
        assert '"n": 275' in r.output

        r = runner.invoke(cli_mod.cli, ["ingest", "list"])
        assert r.exit_code == 0, r.output
        assert "sqlite_chinook" in r.output
        assert "ok" in r.output

        r = runner.invoke(cli_mod.cli, ["ingest", "runs", "--source", "sqlite_chinook"])
        assert r.exit_code == 0, r.output
        assert "sqlite_chinook_Artist" in r.output
        assert "ok" in r.output

    def test_rerun_does_not_duplicate(self, runner):
        for _ in range(2):
            r = runner.invoke(
                cli_mod.cli, ["ingest", "run", "sqlite_chinook", "--table", "Artist"]
            )
            assert r.exit_code == 0, r.output

        r = runner.invoke(
            cli_mod.cli,
            [
                "query",
                "SELECT count(*) AS n FROM sqlite_chinook_Artist",
                "--format",
                "json",
            ],
        )
        assert '"n": 275' in r.output  # replace -- never doubles to 550
