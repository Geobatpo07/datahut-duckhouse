# Ingestion: the source registry (Vague 1b)

How to add a source to `dhd ingest-dlt`, and what each registry field
means. For the module layout behind this command (``ingestion/``'s
layers and their dependency rules), see
[docs/INGESTION_ARCHITECTURE.md](INGESTION_ARCHITECTURE.md). Results from
actually running this at scale (incremental, schema evolution,
crash-resume, volume) are in [docs/DLT_EVAL.md](DLT_EVAL.md).

## Adding a source in 5 steps

1. **Generate the skeleton.**

   ```bash
   uv run python scripts/new_source.py sql my_source     # or: files | rest
   ```

   This appends a `TODO`-annotated entry to `ingestion/config/sources.yml`
   and writes `tests/test_source_my_source.py`.

2. **Fill in the `TODO`s** the generator printed -- the real `secret_ref`
   (an environment variable *name*, never the value), the real
   `dialect`/`path`/`base_url`, and `resources` (tables/resource names, or
   `[]` for "every table" -- `sql` only).

3. **Validate.**

   ```bash
   uv run dhd sources validate
   ```

   Reports every problem with its line and field, not just the first one.

4. **Pick `write_disposition` and `schema_contract`** (see below) --
   `replace` + `evolve` are safe defaults for a first pass.

5. **Run it.**

   ```bash
   uv run dhd ingest-dlt --source my_source --dry-run   # sanity check, no I/O
   uv run dhd ingest-dlt --source my_source
   uv run pytest tests/test_source_my_source.py
   ```

No code change is needed anywhere in `ingestion/` for any of this --
adding a source is purely a `sources.yml` edit (see
[docs/INGESTION_ARCHITECTURE.md](INGESTION_ARCHITECTURE.md) for the
module layout). `--all` runs every
configured source; `--dry-run` validates and prints the resolved plan
(family, connector, resources, write_disposition, schema_contract, whether
it hits a real network) without touching Iceberg or the network at all.

## The three families

| Family | Connector module | What it wraps |
|---|---|---|
| `sql` | `ingestion/connectors/sql.py` | dlt's `sql_database` (SQLAlchemy: sqlite3 stdlib, psycopg2, pymysql, pymssql) |
| `files` | `ingestion/connectors/files.py` | Local CSV/JSON/Parquet via pyarrow, optionally fetched over HTTP first (`http_url`) |
| `rest` | `ingestion/connectors/rest.py` | dlt's declarative `rest_api_source` |

Every connector exposes the same contract: `build_resources(source,
work_dir) -> {prefixed_resource_name: parquet_path}`, one Parquet file per
table/resource, written in one call. `ingestion/connectors/registry.py`
dispatches to the right connector by `source.family` -- this is what lets
task 2's "no code change per source" hold across all three families, not
just `sql`.

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
`rest_options` is a free-form dict passed straight through to dlt's own
`client`/`endpoint` config (`headers`, `params`, `paginator`) -- see
`sources.yml`'s `open_brewery_db` entry for a real paginated example
(`page_number` paginator, capped via `maximum_page` so a routine run stays
bounded). Mark `network: true` -- its connector test then carries
`@pytest.mark.network` and is excluded by default, same as `integration`.

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

**Superseded 2026-11-08** (`feat/dhd-ingest`, step 0 of the second
follow-up review) -- the finding below ("the writer is always the client")
was verified and is kept for the record, but it is no longer how `dhd
ingest-dlt` behaves. The fix:

| `write_disposition` | Who writes | How |
|---|---|---|
| `replace` | **the Flight server** | `FlightSink` -- `con.upload_table(mode="replace")`, the same action `dhd create-table`/`insert` use |
| `append` | **the Flight server** | `FlightSink` -- `con.upload_table(mode=None)` (create-or-append) |
| `merge` | the client, direct via pyiceberg | `DirectSink`, gated by `catalog_guard.check_same_catalog` |

Reproduced live against the real Docker Flight server before fixing it:
`CHINOOK_SQLITE_URL=... dhd ingest-dlt --source sqlite_chinook` created
tables that were then **invisible** to `dhd query`/`dhd list-tables`
(`Catalog Error: Table ... does not exist`) -- two physically different
SQLite catalog files, one in the `flight_data` Docker named volume, one
on the host (`ingestion/data/pyiceberg_catalog.db`, confirmed to already
hold leftover tables from earlier, unrelated sessions). See
`ingestion/sinks/flight_sink.py`.

`merge` still has no Flight-level equivalent: `HybridBackend` inherits a
working `upsert()` *method* unmodified from xorq's `PyIcebergBackend`, but
no Flight action or `UploadMode` reaches it over the wire
(`xorq.flight.server.put_table` only dispatches create/append/replace --
adding one is real scope, see "Next" below, deliberately not done here).
So `merge` still writes directly via pyiceberg from the client -- but
**only once proven to be a single writer**:
`ingestion/sinks/catalog_guard.py` registers a tiny action at runtime
(xorq's own `add-action`, no `HybridBackend`/xorq change -- same technique
as `flight_version_guard.py`) that reports the server's resolved catalog
URI and warehouse path (read straight off the live `HybridBackend`
instance), and compares them to the client's own
(`PyIcebergEngine.uri`/`.warehouse_path`). A mismatch exits **2** with
both catalogs named and a concrete fix, instead of silently writing to a
second, invisible catalog.

Schema evolution (`schema_contract: evolve`) under `append`, which used to
mutate the table's Iceberg schema straight from the client, now needs the
same thing server-side when the write itself moved there:
`ingestion/sinks/flight_sink.py` adds one more dynamically registered
action, reaching the server's own `.catalog` object (the exact one
`HybridBackend` already exposes) to call the identical
`update_schema().union_by_name(...).commit()` the old direct path used --
again, no `HybridBackend` change.

One consequence worth stating plainly: `dhd ingest-dlt`/`dhd ingest` now
*always* needs a reachable Flight server, for every `write_disposition`,
not just `merge` -- this is a real behavior change from Vague 1b, where
the whole point of this command was to need no Flight server at all. That
trade-off (visibility by default vs. a serverless batch path) was an
explicit decision for this task, not a side effect.

**Next** (not done here, estimate): a real `merge`-over-Flight needs (a) a
new Flight action calling `server._conn.upsert(...)` -- small, same
pattern as the two actions above, maybe half a day including tests; (b)
deciding whether `UploadMode` grows a `MERGE` value (wire-protocol change,
touches `xorq.flight.enums`/`server.put_table`, upstream-adjacent) or stays
a bespoke action like schema-evolve (smaller, consistent with what's
already here) -- the latter is the lower-risk default.

## Incremental extraction

```yaml
incremental:
  column: updated_at
  type: timestamp   # or: bigint, etc. -- a dlt cursor type hint
```

Tracks the highest value of `column` seen so far in a small local JSON
file per `(source, resource)` under `ingestion/_state/` (configurable via
`DHD_INGEST_STATE_DIR`; gitignored -- this is a cursor, not data).
**Verified caveat**: the lower bound is
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
connection string, API key, or password.

## Exploitation: the run log

Every `dhd ingest-dlt` run logs one row per table to a `_ingest_runs`
Iceberg table (source, table, start, end, duration, rows, status, error) --
appended through the Flight server (same single-writer path as the data
itself), so it costs no extra round trip and stays visible to `dhd query`.
If that table can't be prepared or appended to, it falls back to a local
SQLite file (`ingestion/_state/ingest_runs.db`, configurable via
`DHD_INGEST_RUN_LOG_DB`) and says so on stderr rather than silently
dropping the row.

Every table write -- whatever its `write_disposition` -- is exactly **one**
Iceberg commit per run, regardless of how many Parquet chunks a connector
pulled it in. Verified by counting snapshots across 3 runs
(`tests/ingestion/test_engine.py::TestOneCommitPerTablePerRun`).
