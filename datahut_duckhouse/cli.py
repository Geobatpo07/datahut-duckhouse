"""``dhd`` - the DataHut-DuckHouse command-line client (roadmap Phase 2).

A thin wrapper over the Arrow Flight server: ingest and query real data without
writing Python. No tenant concept yet - that is Phase 6, behind the roadmap's
bascule trigger.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

import click
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.json as pajson
import pyarrow.parquet as papq

import ingestion.cli
from datahut_duckhouse import __version__
from datahut_duckhouse.connection import FlightUnavailableError, connect, flight_target
from datahut_duckhouse.flight_version_guard import FlightPythonVersionMismatchError

ENGINES = ("flight", "pyiceberg")

_READERS = {
    ".csv": pacsv.read_csv,
    ".parquet": papq.read_table,
    ".pq": papq.read_table,
    ".json": pajson.read_json,
    ".ndjson": pajson.read_json,
}


def _load_source(source: str) -> pa.Table:
    path = Path(source)
    if not path.is_file():
        raise click.ClickException(f"Source file not found: {source}")
    reader = _READERS.get(path.suffix.lower())
    if reader is None:
        raise click.ClickException(
            f"Unsupported source type '{path.suffix}'. "
            f"Supported: {', '.join(sorted(_READERS))}"
        )
    try:
        return reader(str(path))
    except Exception as exc:  # noqa: BLE001 - surface a clean message
        raise click.ClickException(f"Could not read {source}: {exc}") from exc


def _json_default(value: object):
    from datetime import date, datetime
    from decimal import Decimal

    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return str(value)


def _render(table: pa.Table, fmt: str) -> None:
    if fmt == "json":
        click.echo(json.dumps(table.to_pylist(), default=_json_default, indent=2))
        return
    if fmt == "csv":
        import io

        buf = io.BytesIO()
        pacsv.write_csv(table, buf)
        click.echo(buf.getvalue().decode())
        return

    # default: aligned text table
    cols = table.column_names
    rows = [[_fmt_cell(v) for v in row.values()] for row in table.to_pylist()]
    widths = [
        max(len(cols[i]), *(len(r[i]) for r in rows)) if rows else len(cols[i])
        for i in range(len(cols))
    ]
    sep = "  "
    click.echo(sep.join(c.ljust(w) for c, w in zip(cols, widths, strict=True)))
    click.echo(sep.join("-" * w for w in widths))
    for r in rows:
        click.echo(sep.join(c.ljust(w) for c, w in zip(r, widths, strict=True)))
    click.echo(f"\n({table.num_rows} row{'s' if table.num_rows != 1 else ''})")


def _fmt_cell(value: object) -> str:
    if value is None:
        return "NULL"
    return str(value)


def _resolve_engine(ctx: click.Context) -> str:
    engine = (ctx.obj or {}).get("engine") or os.getenv("DHD_ENGINE") or "flight"
    if engine not in ENGINES:
        raise click.ClickException(
            f"Unknown engine '{engine}'. Choose from: {', '.join(ENGINES)}."
        )
    return engine


class _ConfigClickException(click.ClickException):
    """A ``click.ClickException`` that exits 2, not 1 -- a configuration
    problem (bad registry, missing secret, mismatched catalog), not a run
    failure."""

    exit_code = 2


def _connect_or_die(ctx: click.Context):
    engine = _resolve_engine(ctx)
    try:
        if engine == "pyiceberg":
            from datahut_duckhouse.ingest import IngestUnavailableError
            from datahut_duckhouse.ingest import connect as ingest_connect

            try:
                return ingest_connect()
            except IngestUnavailableError as exc:
                raise click.ClickException(str(exc)) from exc
        return connect()
    except FlightUnavailableError as exc:
        raise click.ClickException(str(exc)) from exc
    except FlightPythonVersionMismatchError as exc:
        raise click.ClickException(str(exc)) from exc


def _pyiceberg_connect():
    from datahut_duckhouse.ingest import connect as ingest_connect

    return ingest_connect()


ingestion.cli.configure(connect, _pyiceberg_connect)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, prog_name="dhd")
@click.option(
    "--engine",
    type=click.Choice(ENGINES),
    default=None,
    help="Ingestion/query engine. 'flight' drives the Arrow Flight server (xorq); "
    "'pyiceberg' talks straight to Iceberg on MinIO, serverless and xorq-free. "
    "Env: DHD_ENGINE. Default: flight.",
)
@click.option(
    "--branch",
    "branch_override",
    default=None,
    help="Nessie reference for this invocation (ADR-0006, decision 3). Only "
    "affects the 'pyiceberg' engine and the branch/merge/tag/rollback "
    "commands -- the 'flight' engine reads the running Flight server's own "
    "ICEBERG_BRANCH, fixed at the server's startup, not per client call. "
    "Env: ICEBERG_BRANCH. Default: main.",
)
@click.pass_context
def cli(ctx: click.Context, engine: str | None, branch_override: str | None) -> None:
    """DataHut-DuckHouse CLI - drive the Flight server (Iceberg + DuckDB)."""
    ctx.obj = {"engine": engine}
    if branch_override:
        os.environ["ICEBERG_BRANCH"] = branch_override


cli.add_command(ingestion.cli.ingest_group)


@cli.command("list-tables")
@click.pass_context
def list_tables(ctx: click.Context) -> None:
    """List tables known to the server (Iceberg namespace, reflected in DuckDB)."""
    backend = _connect_or_die(ctx)
    names = sorted(backend.list_tables())
    if not names:
        if _resolve_engine(ctx) == "pyiceberg":
            click.echo(f"(no tables in namespace '{backend.namespace}')")
        else:
            host, port = flight_target()
            click.echo(f"(no tables on {host}:{port})")
        return
    for name in names:
        click.echo(name)


@cli.command("create-table")
@click.argument("name")
@click.argument("source", type=click.Path())
@click.pass_context
def create_table(ctx: click.Context, name: str, source: str) -> None:
    """Create table NAME in Iceberg from SOURCE (.csv/.parquet/.json)."""
    backend = _connect_or_die(ctx)
    if name in set(backend.list_tables()):
        raise click.ClickException(
            f"Table '{name}' already exists. Use `dhd insert {name} {source}` "
            f"to add rows."
        )
    data = _load_source(source)
    backend.create_table(name, data)
    click.echo(f"Created '{name}' - {data.num_rows} rows, {data.num_columns} columns.")


@cli.command()
@click.argument("table")
@click.argument("source", type=click.Path())
@click.option(
    "--mode",
    type=click.Choice(["append", "overwrite"]),
    default="append",
    show_default=True,
)
@click.pass_context
def insert(ctx: click.Context, table: str, source: str, mode: str) -> None:
    """Insert rows from SOURCE into an existing TABLE."""
    backend = _connect_or_die(ctx)
    if table not in set(backend.list_tables()):
        raise click.ClickException(
            f"Table '{table}' does not exist. Create it first with "
            f"`dhd create-table {table} {source}`."
        )
    data = _load_source(source)
    backend.con.upload_table(table, data, mode=mode)
    verb = "Appended" if mode == "append" else "Overwrote"
    click.echo(f"{verb} '{table}' with {data.num_rows} rows.")


@cli.command("drop-table")
@click.argument("name")
@click.option("--yes", is_flag=True, help="Skip the confirmation prompt.")
@click.pass_context
def drop_table(ctx: click.Context, name: str, yes: bool) -> None:
    """Drop table NAME. Irreversible -- asks for confirmation unless --yes.

    Works against both engines: the flight engine dispatches xorq's own
    (pre-existing, unmodified) `drop_table` Flight action; --engine
    pyiceberg drops straight from the Iceberg catalog.
    """
    backend = _connect_or_die(ctx)
    if name not in set(backend.list_tables()):
        raise click.ClickException(f"Table '{name}' does not exist.")
    if not yes and not click.confirm(f"Drop table '{name}'? This cannot be undone."):
        click.echo("Aborted.")
        return
    if _resolve_engine(ctx) == "pyiceberg":
        backend.drop_table(name)
    else:
        backend.con.do_action(
            "drop_table", {"name": name, "database": None, "force": False}
        )
    click.echo(f"Dropped '{name}'.")


@cli.command()
@click.argument("sql")
@click.option(
    "--limit", type=int, default=None, help="Cap the number of rows returned."
)
@click.option(
    "--format",
    "fmt",
    type=click.Choice(["table", "csv", "json"]),
    default="table",
    show_default=True,
)
@click.pass_context
def query(ctx: click.Context, sql: str, limit: int | None, fmt: str) -> None:
    """Run a read-only SQL query against the reflected DuckDB views."""
    backend = _connect_or_die(ctx)
    try:
        expr = backend.sql(sql)
        if limit is not None:
            expr = expr.limit(limit)
        result = expr.to_pyarrow()
    except Exception as exc:  # noqa: BLE001 - CLI: clean message, not a traceback
        raise click.ClickException(f"Query failed: {exc}") from exc
    _render(result, fmt)


@cli.command("ingest-dlt")
@click.option(
    "--source",
    "source_name",
    default=None,
    help="Source name from ingestion/dlt/sources.yml, or 'all'.",
)
@click.option(
    "--all",
    "run_all",
    is_flag=True,
    help="Run every source in ingestion/dlt/sources.yml. Same as --source all.",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Validate sources.yml and print what would run, without extracting, "
    "connecting to Iceberg, or writing anything.",
)
def ingest_dlt(source_name: str | None, run_all: bool, dry_run: bool) -> None:
    """Extract a dlt-configured source (sql/files/rest family) and load it into
    Iceberg.

    Requires a reachable Flight server independent of --engine/DHD_ENGINE:
    ``replace``/``append`` are written through it (the same path ``dhd
    create-table``/``dhd insert`` use), so the result is visible to ``dhd
    query``/``dhd list-tables`` right away. ``merge`` still writes directly
    via pyiceberg, but only once this client is proven to share the Flight
    server's own Iceberg catalog -- otherwise this exits with code 2. Each
    source's write_disposition (ingestion/config/sources.yml) controls
    whether a re-run replaces, appends, or merges -- see docs/INGESTION.md.
    """
    click.echo(
        "warning: `dhd ingest-dlt` is deprecated, use `dhd ingest run` "
        "(--source NAME -> SOURCE_NAME positional, --all stays --all).",
        err=True,
    )
    if source_name and run_all:
        raise click.ClickException("Pass either --source NAME or --all, not both.")
    if not source_name and not run_all:
        raise click.ClickException("Pass --source NAME or --all.")
    target = "all" if run_all else source_name
    assert target is not None  # guaranteed by the pass/not-both checks above

    from datahut_duckhouse.ingest_dlt import IngestConfigError, IngestDltError

    if dry_run:
        from datahut_duckhouse.ingest_dlt import plan_ingest_dlt

        try:
            sources = plan_ingest_dlt(target)
        except IngestConfigError as exc:
            raise _ConfigClickException(str(exc)) from exc
        except IngestDltError as exc:
            raise click.ClickException(str(exc)) from exc
        for source in sources:
            resources = (
                f"[{', '.join(source.resources)}]" if source.resources else "(all)"
            )
            click.echo(
                f"{source.name}: family={source.family} connector={source.connector} "
                f"resources={resources} write_disposition={source.write_disposition} "
                f"schema_contract={source.schema_contract} network={source.network}"
            )
        click.echo(
            f"\n({len(sources)} source{'s' if len(sources) != 1 else ''}, dry run)"
        )
        return

    from datahut_duckhouse.ingest_dlt import run_ingest_dlt

    try:
        results = run_ingest_dlt(target)
    except IngestConfigError as exc:
        raise _ConfigClickException(str(exc)) from exc
    except IngestDltError as exc:
        raise click.ClickException(str(exc)) from exc
    if not results:
        click.echo("No tables extracted.")
        return
    for table, (verb, n) in sorted(results.items()):
        click.echo(f"{verb.capitalize()} '{table}' - {n} rows.")


@cli.group()
def sources() -> None:
    """Deprecated: use `dhd ingest list`/`dhd ingest validate` instead."""


@sources.command("validate")
def sources_validate() -> None:
    """Deprecated, use `dhd ingest validate`. Validate sources.yml against
    the registry schema (readable line/field errors)."""
    click.echo(
        "warning: `dhd sources validate` is deprecated, use " "`dhd ingest validate`.",
        err=True,
    )
    from ingestion import SourcesConfigError, validate_registry

    try:
        configs, warnings = validate_registry()
    except SourcesConfigError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"OK: {len(configs)} source(s) valid.")
    for config in configs:
        click.echo(f"  - {config.name} ({config.family}/{config.connector})")
    for warning in warnings:
        click.echo(f"WARN: {warning}", err=True)


@sources.command("list")
def sources_list() -> None:
    """Deprecated, use `dhd ingest list`. List configured sources: name,
    family, connector, write_disposition, schedule, network."""
    click.echo(
        "warning: `dhd sources list` is deprecated, use `dhd ingest list`.",
        err=True,
    )
    from ingestion import SourcesConfigError, list_sources

    try:
        configs = list_sources()
    except SourcesConfigError as exc:
        raise click.ClickException(str(exc)) from exc
    if not configs:
        click.echo("(no sources configured)")
        return

    headers = (
        "name",
        "family",
        "connector",
        "write_disposition",
        "schedule",
        "network",
    )
    rows = [
        (
            c.name,
            c.family,
            c.connector,
            c.write_disposition,
            c.schedule or "-",
            "yes" if c.network else "no",
        )
        for c in configs
    ]
    widths = [
        max(len(headers[i]), *(len(r[i]) for r in rows)) for i in range(len(headers))
    ]
    click.echo("  ".join(h.ljust(w) for h, w in zip(headers, widths, strict=True)))
    click.echo("  ".join("-" * w for w in widths))
    for row in rows:
        click.echo("  ".join(v.ljust(w) for v, w in zip(row, widths, strict=True)))


def _versioning_or_die(fn, *args, **kwargs):
    """Runs a ``iceberg_catalog.versioning`` call, turning its errors into
    a clean exit-2 ``click.ClickException`` -- ``VersioningUnavailableError``
    in solo mode, any other ``NessieApiError`` otherwise. Callers that need
    special handling of one specific error (``dhd merge``'s
    ``MergeConflictError``) catch it themselves, before this fallback."""
    from iceberg_catalog.factory import VersioningUnavailableError
    from iceberg_catalog.versioning import NessieApiError

    try:
        return fn(*args, **kwargs)
    except VersioningUnavailableError as exc:
        raise _ConfigClickException(str(exc)) from exc
    except NessieApiError as exc:
        raise _ConfigClickException(str(exc)) from exc


@cli.group()
def branch() -> None:
    """Nessie branch management (ADR-0006, decision 4). Needs deployment
    mode (ICEBERG_CATALOG_TYPE=rest) -- solo mode has no reference model
    to branch."""


@branch.command("create")
@click.argument("name")
@click.option(
    "--from",
    "from_branch",
    default=None,
    help="Source branch. Default: --branch/ICEBERG_BRANCH, or 'main'.",
)
def branch_create(name: str, from_branch: str | None) -> None:
    """Create branch NAME."""
    from iceberg_catalog.versioning import create_branch

    source = from_branch or os.getenv("ICEBERG_BRANCH", "main")
    ref = _versioning_or_die(create_branch, name, from_branch=source)
    click.echo(f"created branch '{ref.name}' @ {ref.hash} (from '{source}')")


@branch.command("list")
def branch_list() -> None:
    """List every branch and tag."""
    from iceberg_catalog.versioning import list_references

    refs = _versioning_or_die(list_references)
    if not refs:
        click.echo("(no references)")
        return
    for ref in refs:
        click.echo(f"{ref.type:<6} {ref.name:<30} {ref.hash}")


@branch.command("delete")
@click.argument("name")
@click.option(
    "--force", is_flag=True, help="Delete even if it has work not yet merged."
)
def branch_delete(name: str, force: bool) -> None:
    """Delete branch NAME. Refuses if it has unmerged work unless --force."""
    from iceberg_catalog.versioning import delete_branch

    _versioning_or_die(delete_branch, name, force=force)
    suffix = " (forced; its files are left in storage)" if force else ""
    click.echo(f"deleted branch '{name}'{suffix}")


@cli.command("merge")
@click.argument("from_branch")
@click.option(
    "--into",
    default=None,
    help="Target branch. Default: --branch/ICEBERG_BRANCH, or 'main'.",
)
def merge_cmd(from_branch: str, into: str | None) -> None:
    """Merge FROM_BRANCH into another branch (default: main).

    Fails on any conflict -- lists the conflicting tables and never
    applies an automatic resolution (ADR-0006, decision 4; Nessie's own
    FORCE resolution overwrites instead of combining -- docs/spikes/
    NESSIE_SPIKES_2.md, T3 -- so it is never used here)."""
    from iceberg_catalog.factory import VersioningUnavailableError
    from iceberg_catalog.versioning import MergeConflictError
    from iceberg_catalog.versioning import merge as do_merge

    target: str = into if into else os.getenv("ICEBERG_BRANCH", "main")
    try:
        do_merge(from_branch, into=target)
    except VersioningUnavailableError as exc:
        raise _ConfigClickException(str(exc)) from exc
    except MergeConflictError as exc:
        tables = ", ".join(exc.conflicting_keys) or "(see message)"
        raise _ConfigClickException(
            f"merge conflict: '{from_branch}' into '{target}' conflicts on: "
            f"{tables}. No automatic resolution is applied -- resolve by "
            f"hand, then merge again."
        ) from exc
    click.echo(f"merged '{from_branch}' into '{target}'")


@cli.command("tag")
@click.argument("name")
@click.option(
    "--from",
    "from_branch",
    default=None,
    help="Source branch. Default: --branch/ICEBERG_BRANCH, or 'main'.",
)
def tag_cmd(name: str, from_branch: str | None) -> None:
    """Create tag NAME."""
    from iceberg_catalog.versioning import create_tag

    source = from_branch or os.getenv("ICEBERG_BRANCH", "main")
    ref = _versioning_or_die(create_tag, name, from_branch=source)
    click.echo(f"created tag '{ref.name}' @ {ref.hash} (from '{source}')")


@cli.command("rollback")
@click.argument("to_hash")
@click.option(
    "--branch",
    "branch_name",
    default=None,
    help="Branch to roll back. Default: --branch/ICEBERG_BRANCH, or 'main'.",
)
def rollback_cmd(to_hash: str, branch_name: str | None) -> None:
    """Reassign a branch's HEAD to TO_HASH (an earlier commit on that
    branch's own history)."""
    from iceberg_catalog.versioning import rollback as do_rollback

    target = branch_name or os.getenv("ICEBERG_BRANCH", "main")
    _versioning_or_die(do_rollback, target, to_hash=to_hash)
    click.echo(f"'{target}' rolled back to {to_hash}")


def main() -> None:
    try:
        cli()
    except KeyboardInterrupt:  # pragma: no cover
        click.echo("Interrupted.", err=True)
        sys.exit(130)


if __name__ == "__main__":
    main()
