"""The ``dhd ingest`` command group -- a thin layer: options in,
``ingestion``'s public API out, nothing else. ``dlt`` never appears here,
directly or by name in any message.

How to reach the Flight server / pyiceberg catalog is injected by the
caller (``dhd``, via :func:`configure`) rather than imported here -- that
keeps this module independent of ``datahut_duckhouse``'s own connection
resolution (host/port, the Python-version guard, environment variables).
``tests/ingestion/test_dependency_rules.py`` enforces that this file never
imports anything under ``ingestion.*`` beyond the top-level package.
"""

from __future__ import annotations

import sys
import traceback
from collections.abc import Callable
from pathlib import Path

import click

import ingestion

_connect_flight: Callable[[], object] | None = None
_connect_pyiceberg: Callable[[], object] | None = None

_LOG_DIR = Path("ingestion") / "_state" / "cli_logs"


def configure(
    connect_flight: Callable[[], object], connect_pyiceberg: Callable[[], object]
) -> None:
    """Called once by ``dhd`` at startup with its own connection
    factories. ``connect_flight()`` returns an already-connected Flight
    backend; ``connect_pyiceberg`` is a zero-arg callable (called lazily,
    only for a ``merge`` source)."""
    global _connect_flight, _connect_pyiceberg
    _connect_flight = connect_flight
    _connect_pyiceberg = connect_pyiceberg


class _ConfigClickException(click.ClickException):
    """Exits 2: a configuration problem (bad registry, missing secret,
    mismatched catalog), not a run failure."""

    exit_code = 2


def _require_configured() -> tuple[Callable[[], object], Callable[[], object]]:
    if _connect_flight is None or _connect_pyiceberg is None:
        raise RuntimeError(
            "ingestion.cli.configure() was never called -- dhd's own "
            "startup is supposed to do this before dispatching any "
            "`dhd ingest` command."
        )
    return _connect_flight, _connect_pyiceberg


def _connect_flight_or_die() -> object:
    flight_connect, _ = _require_configured()
    return flight_connect()


def _log_traceback(exc: Exception) -> Path:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    import datetime

    path = _LOG_DIR / f"{datetime.datetime.now(datetime.UTC):%Y%m%dT%H%M%S%f}.log"
    path.write_text("".join(traceback.format_exception(exc)))
    return path


def _fail(exc: Exception, *, config_error: bool) -> None:
    log_path = _log_traceback(exc)
    message = f"{exc}\nFull traceback: {log_path}"
    if config_error:
        raise _ConfigClickException(message) from exc
    raise click.ClickException(message) from exc


@click.group("ingest")
def ingest_group() -> None:
    """Extract configured sources (sql/files/rest) and load them into
    Iceberg. See docs/INGESTION.md."""


@ingest_group.command("run")
@click.argument("source_name", required=False)
@click.option("--all", "run_all", is_flag=True, help="Run every configured source.")
@click.option(
    "--family",
    "family_filter",
    type=click.Choice(("sql", "files", "rest")),
    default=None,
    help="With --all, restrict to one family.",
)
@click.option(
    "--table",
    "tables",
    multiple=True,
    help="Restrict extraction to this resource/table. Repeatable.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Resolve, connect, extract and report row counts -- write nothing.",
)
@click.option(
    "--full-refresh",
    is_flag=True,
    help="Drop this source's incremental cursor first, re-extracting "
    "everything from scratch. Asks for confirmation unless --yes.",
)
@click.option("--yes", is_flag=True, help="Skip --full-refresh's confirmation.")
@click.option(
    "--fail-fast",
    is_flag=True,
    help="With --all, stop at the first failed source instead of "
    "running the rest and reporting a summary.",
)
@click.option(
    "--sink",
    "sink_override",
    type=click.Choice(("auto", "flight", "direct")),
    default=None,
    help="Override this source's sink (ADR-0006, decision 5). 'direct' "
    "exits 2 if a direct write isn't possible from this process -- "
    "never a silent fallback to flight.",
)
def ingest_run(
    source_name: str | None,
    run_all: bool,
    family_filter: str | None,
    tables: tuple[str, ...],
    dry_run: bool,
    full_refresh: bool,
    yes: bool,
    fail_fast: bool,
    sink_override: str | None,
) -> None:
    """Run SOURCE_NAME, or every source with --all."""
    if bool(source_name) == run_all:
        raise click.ClickException(
            "Pass either SOURCE_NAME or --all, not both/neither."
        )
    if tables and run_all:
        raise click.ClickException("--table needs a single SOURCE_NAME, not --all.")
    if full_refresh and not yes:
        label = source_name or "every selected source"
        if not click.confirm(
            f"--full-refresh on {label}: drops the incremental cursor and "
            f"re-extracts everything from scratch. Continue?"
        ):
            click.echo("Aborted.")
            return

    try:
        all_sources = ingestion.list_sources()
    except ingestion.SourcesConfigError as exc:
        _fail(exc, config_error=True)
        return

    if run_all:
        targets = [
            s.name
            for s in all_sources
            if not family_filter or s.family == family_filter
        ]
    else:
        names = {s.name for s in all_sources}
        if source_name not in names:
            _fail(
                ValueError(f"No source named '{source_name}' in sources.yml."),
                config_error=True,
            )
            return
        targets = [source_name]

    if dry_run:
        _run_dry(targets, tables)
        return

    _, pyiceberg_connect = _require_configured()
    flight_backend = _connect_flight_or_die()
    failures: dict[str, str] = {}
    for name in targets:
        try:
            result = ingestion.run_source(
                name,
                flight_backend=flight_backend,
                pyiceberg_connect=pyiceberg_connect,
                run_log_echo=lambda msg: click.echo(msg, err=True),
                tables=list(tables) or None,
                full_refresh=full_refresh,
                sink=sink_override,
            )
        except (
            ingestion.CatalogMismatchError,
            ingestion.MissingDriverError,
            ingestion.SourcesConfigError,
            ingestion.DirectSinkUnavailableError,
            ingestion.UnknownSinkError,
        ) as exc:
            failures[name] = str(exc)
            if not run_all or fail_fast:
                _fail(exc, config_error=True)
                return
            continue
        except (
            ingestion.ConnectorError,
            ingestion.SchemaContractViolationError,
            ingestion.SinkError,
        ) as exc:
            failures[name] = str(exc)
            if not run_all or fail_fast:
                _fail(exc, config_error=False)
                return
            continue
        for table in result.tables:
            click.echo(f"{name}: {table.verb} '{table.table}' - {table.rows} rows.")

    if run_all:
        ok = len(targets) - len(failures)
        click.echo(f"\n{ok}/{len(targets)} source(s) succeeded.")
        for name, message in failures.items():
            click.echo(f"  - {name}: FAILED ({message})", err=True)
    if failures:
        sys.exit(1)


def _run_dry(targets: list[str], tables: tuple[str, ...]) -> None:
    for name in targets:
        try:
            result = ingestion.dry_run_source(name, list(tables) or None)
        except (ingestion.MissingDriverError, ingestion.SourcesConfigError) as exc:
            _fail(exc, config_error=True)
            return
        except Exception as exc:  # noqa: BLE001 - CLI boundary: clean message
            _fail(exc, config_error=False)
            return
        for table in result.tables:
            click.echo(f"{name}: would write '{table.table}' - {table.rows} rows.")
    click.echo(f"\n({len(targets)} source(s), dry run -- nothing written)")


@ingest_group.command("list")
def ingest_list() -> None:
    """List configured sources: name, family, schedule, last run status."""
    try:
        sources = ingestion.list_sources()
    except ingestion.SourcesConfigError as exc:
        _fail(exc, config_error=True)
        return
    flight_backend = None
    try:
        flight_backend = _connect_flight_or_die()
    except Exception:  # noqa: BLE001 - status is best-effort, listing isn't
        pass
    runs = ingestion.read_runs(flight_backend) if flight_backend else []
    last_status: dict[str, str] = {}
    for row in runs:  # newest first (read_runs already sorts)
        last_status.setdefault(row["source"], row["status"])

    if not sources:
        click.echo("(no sources configured)")
        return
    headers = ("name", "family", "schedule", "last_status")
    rows = [
        (s.name, s.family, s.schedule or "-", last_status.get(s.name, "-"))
        for s in sources
    ]
    widths = [max(len(headers[i]), *(len(r[i]) for r in rows)) for i in range(4)]
    click.echo("  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)))
    click.echo("  ".join("-" * w for w in widths))
    for table_row in rows:
        click.echo(
            "  ".join(v.ljust(w) for v, w in zip(table_row, widths, strict=True))
        )


@ingest_group.command("validate")
@click.argument("source_name", required=False)
def ingest_validate(source_name: str | None) -> None:
    """Validate sources.yml, secrets (names only), and catalog coherence
    for any 'merge' source."""
    try:
        sources, warnings = ingestion.validate_registry()
    except ingestion.SourcesConfigError as exc:
        _fail(exc, config_error=True)
        return
    if source_name:
        sources = [s for s in sources if s.name == source_name]
        if not sources:
            _fail(ValueError(f"No source named '{source_name}'."), config_error=True)
            return

    click.echo(f"OK: {len(sources)} source(s) valid.")
    import os

    for source in sources:
        click.echo(f"  - {source.name} ({source.family})")
        if source.secret_ref:
            present = bool(os.getenv(source.secret_ref))
            click.echo(
                f"      secret '{source.secret_ref}': "
                f"{'present' if present else 'MISSING'}"
            )
        if source.write_disposition == "merge":
            _report_catalog_coherence(source)
    for warning in warnings:
        click.echo(f"WARN: {warning}", err=True)


def _report_catalog_coherence(source) -> None:
    try:
        flight_connect, pyiceberg_connect = _require_configured()
        ingestion.check_same_catalog(flight_connect(), pyiceberg_connect())
    except ingestion.CatalogMismatchError:
        click.echo(
            f"      catalog coherence (write_disposition=merge): MISMATCH "
            f"-- see `dhd ingest run {source.name}` for details",
            err=True,
        )
    except Exception as exc:  # noqa: BLE001 - best-effort check, never block validate
        click.echo(f"      catalog coherence: could not verify ({exc})", err=True)
    else:
        click.echo("      catalog coherence (write_disposition=merge): OK")


@ingest_group.command("runs")
@click.option("--source", "source_name", default=None)
@click.option("--last", type=int, default=None)
@click.option("--status", default=None)
def ingest_runs(source_name: str | None, last: int | None, status: str | None) -> None:
    """Show logged runs (source, table, status, rows, duration, error)."""
    flight_backend = None
    try:
        flight_backend = _connect_flight_or_die()
    except Exception:  # noqa: BLE001 - the log may be local-only (sqlite fallback)
        pass
    rows = ingestion.read_runs(
        flight_backend, source=source_name, status=status, last=last
    )
    if not rows:
        click.echo("(no runs logged)")
        return
    for row in rows:
        click.echo(
            f"{row['started_at']}  {row['source']:<20}  {row['table_name']:<24}  "
            f"{row['status']:<6}  rows={row['rows']}  "
            f"duration={row['duration_s']:.2f}s"
            + (f"  error={row['error']}" if row.get("error") else "")
        )


@ingest_group.command("new")
@click.argument("family", type=click.Choice(("sql", "files", "rest")))
@click.argument("name")
def ingest_new(family: str, name: str) -> None:
    """Generate a sources.yml entry + a skeleton test for NAME."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
    import new_source

    block, test_path = new_source.generate(family, name, new_source.DEFAULT_SOURCES_YML)
    click.echo(f"Appended to {new_source.DEFAULT_SOURCES_YML}:")
    click.echo(block)
    click.echo(f"Wrote skeleton test: {test_path}")
    click.echo(
        "\nNext: fill in the TODOs above, then:\n"
        "  uv run dhd ingest validate\n"
        f"  uv run pytest {test_path}"
    )
