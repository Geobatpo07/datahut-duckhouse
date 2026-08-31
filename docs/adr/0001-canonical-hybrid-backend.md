# ADR-0001: Iceberg-as-Source-of-Truth via a Single Canonical Hybrid Backend

**Status:** Accepted
**Date:** 2026-08-29
**Deciders:** Geovany Batista Polo Laguerre

## Context

The codebase currently contains three parallel, unconnected implementations of the
"decide where data lives / how a query is routed" problem:

1. `flight_server/app/app.py` (`DuckHouseFlightServer` + `QueryOrchestrator`) — heuristic
   query routing between DuckDB and Trino based on estimated row count and query
   complexity. **Not actually run**: the Dockerfile's `CMD` points at `app_xorq.py`, not
   `app.py`. It also contains a live bug (`datetime.utcnow()` called without importing
   `datetime`) and unimplemented `do_get` / `list_flights` / `get_flight_info` methods,
   so even if it were wired in, Flight could write data but never read it back.
2. `flight_server/app/app_xorq.py` + `xorq_config.py` — the server that **is** actually
   deployed. It registers two independent Xorq backends (`duckdb`, `iceberg`) with no
   routing logic; the caller has to know which backend to target.
3. `flight_server/app/backends/hybrid_backend.py` (`HybridBackend`) — extends Xorq's
   PyIceberg backend, writes to Iceberg, and reflects DuckDB views over Iceberg tables
   via `iceberg_scan`, with automatic snapshotting. Well-tested (`tests/test_hybrid_backend.py`)
   but only ever instantiated from `scripts/create_tenant.py`, never from the Flight
   server that actually serves traffic.

This split means the deployed path (2) is the least architecturally sound of the three,
and the most promising design (3) is disconnected from production traffic. There is no
single definition of "where does a table live" that the whole platform agrees on.

Separately, `HybridBackend.create_table` / `insert` currently accept a `target` parameter
of `"duckdb"` or `"iceberg"`. When `target="duckdb"`, data is written as a **native**
DuckDB table with no Iceberg counterpart — un-versioned, invisible to Trino, and lost if
the Flight server's local disk is not durable. This directly undermines the point of a
hybrid stack.

## Decision

Adopt **`HybridBackend` as the single canonical backend** for the platform, with one
structural rule: **Iceberg is the only durable write target. DuckDB is a query/cache
layer only, never a storage destination.**

Concretely:

- Remove the `target="duckdb"` branch from `HybridBackend.create_table` and `insert`.
  Every write goes to Iceberg; DuckDB views are refreshed via `_reflect_views()` as
  today.
- Retire `flight_server/app/app.py` and `flight_server/app/query_orchestrator.py` as
  the routing layer. If the query-complexity heuristics in `_analyze_query` are useful
  later (e.g. for a query-planner or cost estimator), they can be salvaged as a
  standalone diagnostic utility — but they will not sit on the write/read critical path.
- Rewire `app_xorq.py` / `xorq_config.py` so the Flight server registers `HybridBackend`
  as its one backend, instead of separate `duckdb` and `iceberg` backends in the Xorq
  registry.
- Implement `do_get`, `list_flights`, and `get_flight_info` against `HybridBackend` so
  Flight becomes a complete read/write path, not write-only.
- A genuinely ephemeral, session-scoped staging table (if ever needed) must go through a
  separate, explicitly-named connector — not through `HybridBackend` — so "ephemeral" is
  always visible in the code, never an accidental side effect of a routing decision.

## Options Considered

### Option A: `app.py` + `QueryOrchestrator` (heuristic routing)
| Dimension | Assessment |
|-----------|------------|
| Complexity | High — custom complexity scoring, hard-coded table registry |
| Cost | Low infra cost, high maintenance cost |
| Scalability | Poor — routing table is hard-coded, not derived from real metadata |
| Team familiarity | High (author-written), but currently broken and untested in situ |

**Pros:** Most "intelligent" routing logic on paper.
**Cons:** Dead code today (not run by Docker); contains a blocking bug; read path
(`do_get`) unimplemented; routing decisions are guesses about data size rather than
facts, duplicating work Iceberg/Trino already do better.

### Option B: `app_xorq.py` + `xorq_config.py` (two independent backends)
| Dimension | Assessment |
|-----------|------------|
| Complexity | Low |
| Cost | Low |
| Scalability | Poor — no unification, caller must pick the backend |
| Team familiarity | Medium — relies on Xorq's native primitives |

**Pros:** Currently deployed, minimal transport-layer changes needed.
**Cons:** No hybrid semantics at all — it's two separate stores with a shared network
port, not one platform.

### Option C: `HybridBackend` as sole backend, Iceberg-only writes
| Dimension | Assessment |
|-----------|------------|
| Complexity | Medium — one class to maintain, clear invariant |
| Cost | Low incremental cost over current state |
| Scalability | Good — versioning, multi-engine read access, and durability come from Iceberg itself |
| Team familiarity | Medium, but already has test coverage to build on |

**Pros:** Solves the underlying problem structurally (one logical table, two access
paths) instead of heuristically; already partially built and tested.
**Cons:** Requires implementing the missing read path and rewiring the Flight server
entrypoint; snapshot strategy needs revisiting (see ADR-0004... consequence noted below,
tracked separately).

## Trade-off Analysis

Option A's heuristic routing tries to solve a problem — "is this data big/complex enough
to need distributed processing" — that Iceberg + Trino already solve by design once data
lives in Iceberg. Keeping it would mean maintaining a second, weaker cost-based optimizer
by hand. Option B is architecturally the weakest: it doesn't merge the two systems at
all. Option C accepts a small amount of near-term work (finish the read path, rewire one
entrypoint) in exchange for removing an entire class of "which backend has this table?"
bugs going forward.

## Consequences

- **Easier:** Any tenant table has exactly one location and one durability guarantee;
  Trino, dbt, and DuckDB all read the same Iceberg data, so there's no drift between
  what different tools see.
- **Easier:** Onboarding — one backend class to understand instead of three.
- **Harder:** Every write pays the cost of an Iceberg commit, even for very small/throwaway
  data. This is an accepted trade-off in exchange for durability and consistency; if a
  genuine need for a throwaway staging path emerges, it should be a separate, explicit
  connector (see Decision above), not a silent branch in `HybridBackend`.
- **To revisit:** The per-insert snapshot strategy in `HybridBackend._create_snapshot`
  (full file copy on every write) does not scale with this change now driving all writes
  through Iceberg; it is addressed as a follow-up decision, not blocking this ADR.

## Action Items

1. [x] Remove `target="duckdb"` branch from `HybridBackend.create_table` / `insert`
2. [x] Delete or archive `flight_server/app/app.py` and `query_orchestrator.py`
       (optionally extract `_analyze_query` as a standalone diagnostic utility)
3. [x] Rewire `xorq_config.py` to register `HybridBackend` as the sole backend, via
       `xorq.flight.FlightServer(make_connection=...)` — this is the real installed
       API; earlier drafts of this ADR referenced a `registry`/`client=` API that does
       not exist in the published `xorq` package (see note below)
4. ~~Implement `do_get` / `list_flights` / `get_flight_info` on `HybridBackend`~~ —
       **not needed**: the real `xorq.flight.FlightServerDelegate` already implements
       `do_get`/`do_put`/`get_flight_info` generically, delegating to whatever backend
       `make_connection` returns via `to_pyarrow_batches`, `create_table`, `insert`, and
       `.tables` — all of which `HybridBackend` already provides. Verified against the
       pinned `xorq==0.2.4`.
5. [x] Update `docs/CORRECTED_ARCHITECTURE_SUMMARY.md` to reflect the retired routing
       layer once the above lands

### Post-implementation note (2026-08-29)

Implementing this ADR surfaced that the published `xorq` package's real API differs
substantially from what earlier code (and earlier drafts of this ADR) assumed:
no `xorq.registry` module, no `xorq.duckdb.connect`, `FlightServer` takes
`make_connection=`, not `client=`. These were invisible because the test suite mocked
`xo`/`xorq` entirely rather than exercising the real package. Fixed in
`hybrid_backend.py`, `xorq_config.py`, `app_xorq.py`, and `utils.py`; tests updated to
mock at the correct import boundary instead of the defining module.

### Update (2026-08-31): MinIO/S3 wiring resolved

The gap noted below — `HybridBackend` accepting an `s3://` `warehouse_path` but the real
`xorq.backends.pyiceberg.Backend.do_connect` always rewriting it to a local
`file://Path(...).absolute()` — is now resolved. `HybridBackend.do_connect` detects an
`s3://`/`s3a://` `warehouse_path` and routes to a new `_connect_s3_iceberg` method that
builds the pyiceberg catalog directly (bypassing the parent's local-only logic), with the
real `s3.endpoint`/`s3.access-key-id`/`s3.secret-access-key`/`s3.force-virtual-addressing`
catalog properties. Catalog *metadata* (which tables exist) still lives in a local SQLite
file — only table *data* goes to MinIO, via the `warehouse` catalog property. DuckDB is
configured with `httpfs` + the same S3 credentials + path-style addressing (required by
MinIO) so `_reflect_views`'s `iceberg_scan('s3://...')` calls can actually reach it.
`xorq_config.py` now uses the project's existing `ICEBERG_WAREHOUSE` convention (already
in `.env.example`, defaulting to `s3://duckhouse-warehouse/`) instead of the ad-hoc local
env var this ADR originally introduced as a stopgap.

Verified for real (catalog construction + namespace creation against the real pyiceberg
library, and the exact DuckDB `SET`/`INSTALL` commands produced) without a live MinIO
instance, since catalog metadata operations don't require one. **Not verified**: an
actual data write/read round trip against a running MinIO — needs validation in an
environment where MinIO can actually be reached (this sandbox cannot run the
`docker-compose` stack). See `docs/PROMPT_VALIDATION_PHASE1_2.md` for the validation
checklist this feeds into.

Still open, unrelated to this update: ~~`scripts/ingest_flight.py` and `scripts/pipeline.py`
still reference the `target=` parameter removed earlier by this same ADR — they predate
that change and are now broken.~~ **Fixed** (2026-08-31, same day): both rewritten to use
`datahut_duckhouse.connection.get_connection()` — the same verified path the CLI uses —
instead of hand-rolling the Flight protocol. `pipeline.py` had a deeper, independent bug:
it built a `FlightDescriptor.for_path(...)`, but the real server's `do_put`
(`FlightServerDelegate`) only parses a `for_command(...)`-style descriptor; it could
never have worked against the real server, `target=` aside. See `tests/test_scripts.py`.
