# Ingestion: `dhd ingest`

How to add a source and run it with `dhd ingest`, and what each registry
field means. For the module layout behind this command (``ingestion/``'s
layers, dependency rules, and the extraction engine under the hood), see
[docs/INGESTION_ARCHITECTURE.md](INGESTION_ARCHITECTURE.md) -- the one
place this doc names the extraction library (dlt) at all. Results from
actually running this at scale (incremental, schema evolution,
crash-resume, volume) are in [docs/DLT_EVAL.md](DLT_EVAL.md).

## Adding a source in 5 steps

1. **Generate the skeleton.**

   ```bash
   uv run dhd ingest new sql my_source     # or: files | rest
   ```

   This appends a `TODO`-annotated entry to `ingestion/config/sources.yml`
   and writes `tests/test_source_my_source.py`.

2. **Fill in the `TODO`s** the generator printed -- the real `secret_ref`
   (an environment variable *name*, never the value), the real
   `dialect`/`path`/`base_url`, and `resources` (tables/resource names, or
   `[]` for "every table" -- `sql` only).

3. **Validate.**

   ```bash
   uv run dhd ingest validate
   ```

   Reports every problem with its line and field, not just the first one
   -- plus whether each source's secret is present (by name, never its
   value) and, for any `merge` source, whether this client and the Flight
   server agree on one catalog (see "Who actually writes to Iceberg"
   below).

4. **Pick `write_disposition` and `schema_contract`** (see below) --
   `replace` + `evolve` are safe defaults for a first pass.

5. **Run it.**

   ```bash
   uv run dhd ingest run my_source --dry-run   # sanity check, no I/O
   uv run dhd ingest run my_source
   uv run pytest tests/test_source_my_source.py
   ```

No code change is needed anywhere in `ingestion/` for any of this --
adding a source is purely a `sources.yml` edit (see
[docs/INGESTION_ARCHITECTURE.md](INGESTION_ARCHITECTURE.md) for the
module layout).

## `dhd ingest` commands

```
dhd ingest run <source> [--table T ...] [--dry-run] [--full-refresh] [--yes]
dhd ingest run --all [--family sql|files|rest] [--fail-fast]
dhd ingest list                        # name, family, schedule, last run status
dhd ingest validate [<source>]         # registry, secrets (names only), catalog coherence
dhd ingest runs [--source S] [--last N] [--status failed]
dhd ingest new <family> <name>         # registry entry + skeleton test
```

- **Exit codes**: `0` success; `1` at least one source failed to run;
  `2` a configuration problem (bad `sources.yml`, a missing secret, or a
  `merge` source whose client and Flight server don't share a catalog).
- `--table` narrows extraction to specific resources, same as editing
  `resources` for just this run -- needs a single source, not `--all`.
- `--dry-run` resolves the source, connects for real (querying a `sql`
  database, hitting a `rest` API), and reports exactly what would be
  written, without touching any sink or the run log at all -- verified by
  comparing a table's Iceberg snapshot count before and after: unchanged
  (`tests/ingestion/test_cli.py::TestRealFlightServer::test_dry_run_writes_nothing`).
- `--full-refresh` drops this source's incremental cursor first, so
  extraction starts from scratch exactly like a brand-new source's first
  run -- asks for confirmation unless `--yes` (it can mean a lot of
  re-extracted data for a `merge` source, or a full table rewrite for
  `replace`).
- `--all` runs every source isolated from the others: one failure doesn't
  stop the rest (prints a per-source summary at the end) unless
  `--fail-fast`, which stops immediately. `--family` narrows `--all` to
  one connector family.
- `dhd ingest validate`/`dhd ingest run`/`dhd ingest list`/`dhd ingest
  runs` all go through the same Python-version guard as every other `dhd`
  command against the Flight server (`flight_version_guard.py`) --
  a mismatched client/server exits with a clear message before anything
  else runs.
- **Deprecated aliases**: `dhd ingest-dlt` (`--source`/`--all`/`--dry-run`)
  and `dhd sources validate`/`dhd sources list` still work exactly as
  before, now printing a one-line deprecation warning (stderr) pointing
  at the `dhd ingest` equivalent. `dhd drop-table` is unrelated to
  ingestion and stays a top-level `dhd` command.

## The three families

| Family | Connector module | What it wraps |
|---|---|---|
| `sql` | `ingestion/connectors/sql.py` | A SQLAlchemy-backed extractor (sqlite3 stdlib, psycopg2, pymysql, pymssql) |
| `files` | `ingestion/connectors/files.py` | Local CSV/JSON/Parquet via pyarrow, optionally fetched over HTTP first (`http_url`) |
| `rest` | `ingestion/connectors/rest.py` | A declarative REST extractor (base URL + per-resource path/params/paginator) |

Every connector exposes the same contract: `build_resources(source,
work_dir) -> {prefixed_resource_name: parquet_path}`, one Parquet file per
table/resource, written in one call. `ingestion/connectors/registry.py`
dispatches to the right connector by `source.family` -- this is what lets
"no code change per source" hold across all three families, not just
`sql`.

### `sql`

Needs `dialect` (`sqlite`/`postgresql`/`mysql`/`sqlserver`) and
`secret_ref` (the connection-URL env var). `resources: []` means every
table in `schema`; an explicit list narrows it. `incremental: {column,
type}` pulls only rows past a locally-tracked cursor (see "Incremental"
below) instead of a full table scan every run.

### `files`

Needs `path` (local, repo-root-relative) and `format`
(`csv`/`json`/`parquet`), or `http_url` to download first. Always exactly
one resource (the file itself); `resources` defaults to `[name]` if
omitted. JSON records with nested objects are flattened one level
(`address: {city: ...}` -> `address_city`) so the resulting Parquet/Iceberg
schema stays flat.

### `rest`

Needs `base_url` and `resources` (one per REST endpoint/path).
`rest_options` is a free-form dict passed straight through to the
extractor's own `client`/`endpoint` config (`headers`, `params`,
`paginator`) -- see `sources.yml`'s `open_brewery_db` entry for a real
paginated example (`page_number` paginator, capped via `maximum_page` so a
routine run stays bounded). Mark `network: true` -- its connector test
then carries `@pytest.mark.network` and is excluded by default, same as
`integration`.

## write_disposition

| Value | Behavior | Needs |
|---|---|---|
| `replace` (default) | Full create-or-overwrite every run | -- |
| `append` | Always appends -- **not** idempotent, see below | -- |
| `merge` | Upserts by primary key (pyiceberg `Table.upsert`) | `primary_key` |

`append` duplicates rows on a re-run or a crash-then-resume once a table's
commit has already landed (verified:
`tests/ingestion/test_engine.py`'s `test_append_accumulates_on_rerun` and
[DLT_EVAL.md](DLT_EVAL.md)'s
crash-resume experiment). Pair it with `incremental` and still prefer
`merge` whenever a stable primary key exists -- `merge` is idempotent
regardless of how many times the same extracted rows get re-applied, which
is what makes `incremental`'s inclusive cursor boundary (see below) safe
in practice. **`incremental` + `append` is rejected at validation time**
(no dedup key exists for `append` to fall back on); **`incremental` +
`replace` only warns**, because it silently drops every unchanged row
instead of duplicating anything -- a different failure mode, not
inherently wrong if a source genuinely re-pulls its whole window at once.

## Who actually writes to Iceberg (single writer, ADR-0001)

| `write_disposition` | Who writes | How |
|---|---|---|
| `replace` | **the Flight server** | `FlightSink` -- `con.upload_table(mode="replace")`, the same action `dhd create-table`/`insert` use |
| `append` | **the Flight server** | `FlightSink` -- `con.upload_table(mode=None)` (create-or-append) |
| `merge` | the client, direct via pyiceberg | `DirectSink`, gated by a catalog-identity check |

**Verified 2026-11-08**: before this was fixed, a table loaded this way
was written pyiceberg-direct and was **invisible** to `dhd query`/`dhd
list-tables` -- two physically different SQLite catalog files, one in the
Flight server's Docker named volume, one on the host. Fix:
`replace`/`append` now write through the Flight server itself
(`ingestion/sinks/flight_sink.py`), so a table lands exactly where `dhd
query` already looks. One real consequence worth stating plainly: `dhd
ingest run` now *always* needs a reachable Flight server, for every
`write_disposition`, not a serverless batch path.

`merge` still has no Flight-level equivalent: `HybridBackend` inherits a
working `upsert()` *method* unmodified from xorq's `PyIcebergBackend`, but
no Flight action reaches it over the wire. So `merge` still writes
directly via pyiceberg from the client -- but **only once proven to be a
single writer**: `ingestion/sinks/catalog_guard.py` registers a tiny
action at runtime (xorq's own `add-action`, no `HybridBackend`/xorq
change) that reports the server's resolved catalog URI and warehouse
path, and compares them to the client's own. A mismatch exits **2** with
both catalogs named and a concrete fix (also checked proactively by `dhd
ingest validate` for every configured `merge` source), instead of
silently writing to a second, invisible catalog.

**Next** (not done here, estimate): a real `merge`-over-Flight needs a new
Flight action calling `server._conn.upsert(...)` -- small, same pattern as
the catalog-identity/schema-evolution actions already in
`ingestion/sinks/`, maybe half a day including tests.

## Incremental extraction

```yaml
incremental:
  column: updated_at
  type: timestamp   # or: bigint, etc. -- a cursor type hint
```

Tracks the highest value of `column` seen so far in a small local JSON
file per `(source, resource)` under `ingestion/_state/` (configurable via
`DHD_INGEST_STATE_DIR`; gitignored -- this is a cursor, not data).
`dhd ingest run <source> --full-refresh` drops it, so the next run starts
from scratch. **Verified caveat**: the lower bound is
inclusive (`>=`), so any rows that happen to share the exact cursor value
already recorded (e.g. a bulk-backfilled column where many rows got the
same timestamp) are re-extracted on every subsequent run, not skipped --
harmless under `merge` (idempotent), would duplicate under `append`. See
[DLT_EVAL.md](DLT_EVAL.md), experiment 1.

**Deletions are never detected.** A row removed from the source stays in
the Iceberg table forever under incremental+merge -- there is no cursor
value a deleted row could report. Verified live; see DLT_EVAL.md.

## schema_contract

Applied only when a table **already exists** (a brand-new table just
takes the first run's schema as-is). Verified against pyiceberg 0.11.1,
which raises `ValueError` on an unseen column and suggests
`union_by_name` -- this is exactly what `evolve` does, proactively.

| Value | Behavior |
|---|---|
| `evolve` (default) | Widens the Iceberg table's schema in place, old rows get `null` for the new column |
| `freeze` | Rejects the write with a clear message naming the new column(s) |
| `discard_value` | Silently drops the unrecognized column(s), keeps the table's schema unchanged |

A column *missing* from the incoming data (vs. the table's schema) is
never a contract concern -- pyiceberg already fills it with `null`.

## Secrets

`secret_ref` is always an environment variable **name**, never a value --
see `.devcontainer/.env.example` for the real variable names
(`CHINOOK_POSTGRES_URL`, etc.). `sources.yml` itself never holds a
connection string, API key, or password. `dhd ingest validate` reports
whether each source's secret is *present* (set, non-empty), never its
value; any error message that happens to echo a secret's resolved value
(a driver's own error text, for instance) is redacted (`SourceConfig.
redact`) before it reaches the CLI, the run log, or the full-traceback
log file -- verified in `tests/ingestion/test_cli.py::
test_secret_value_never_appears_in_output` with a fake secret value.

## Exploitation: the run log

Every `dhd ingest run` logs one row per table to a `_ingest_runs`
Iceberg table (source, table, start, end, duration, rows, commits,
status, error) -- appended through the Flight server (same single-writer
path as the data itself), so it costs no extra round trip and stays
visible to `dhd query`/`dhd ingest runs`. If that table can't be prepared
or appended to, it falls back to a local SQLite file
(`ingestion/_state/ingest_runs.db`, configurable via
`DHD_INGEST_RUN_LOG_DB`) and says so on stderr rather than silently
dropping the row. An error message in the log is redacted the same way as
everywhere else (see "Secrets" above) -- never a traceback; a full
traceback instead goes to a local log file whose path `dhd ingest run`
prints alongside the clean error message.

Every table write -- whatever its `write_disposition` -- is exactly **one**
Iceberg commit per run, regardless of how many Parquet chunks a connector
pulled it in. Verified by counting snapshots across 3 runs
(`tests/ingestion/test_engine.py::TestOneCommitPerTablePerRun`).
