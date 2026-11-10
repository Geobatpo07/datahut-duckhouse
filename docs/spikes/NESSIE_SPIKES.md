# Nessie catalog spikes

Verifies the "Nessie through the Iceberg REST interface" decision in
[docs/adr/0006-single-shared-catalog.md](../adr/0006-single-shared-catalog.md)
before it moves from Proposed to Accepted. Everything here is throwaway spike
work on branch `spike/nessie-catalog`: no file under `flight_server/`,
`datahut_duckhouse/` or `ingestion/` was touched. Scripts live in
`scripts/spikes/`, the stack in `docker-compose.nessie.yml`.

**Reading this table:** "vérifié" means executed and observed directly (a
command was run, its output is quoted or described exactly); "non vérifié"
means asserted from documentation/knowledge but not run in this spike.

## Versions used (vérifié)

| Component | Version | How pinned |
|---|---|---|
| Nessie server | `ghcr.io/projectnessie/nessie:0.108.4-java` | exact tag in `docker-compose.nessie.yml` |
| Nessie's Postgres store | `postgres:16-alpine` | exact tag, dedicated `nessie-postgres` service/volume |
| pyiceberg | 0.11.1 | already pinned in `pyproject.toml`, unchanged |
| MinIO | whatever `docker-compose.yml`'s `minio` service runs (reused, not pinned here) | main stack |
| Trino | `trinodb/trino:latest` (main stack's existing pin) | main stack |

## Naming conventions followed

Relevé avant d'écrire quoi que ce soit, dans `docker-compose.yml`,
`docker-compose.test.yml` et `.devcontainer/docker-compose.dev.yml` :

- Fichier : `docker-compose.<rôle>.yml` à la racine, comme
  `docker-compose.test.yml` → ce spike utilise `docker-compose.nessie.yml`.
- `name:` du projet compose : `datahut-duckhouse-<rôle>` (vu sur
  `datahut-duckhouse-chinook-test`) → `datahut-duckhouse-nessie-spike`.
- Un service Postgres dédié par usage (`chinook-postgres` a son propre
  volume/healthcheck) → `nessie-postgres`, volume `nessie_postgres_data`,
  jamais la base tenant d'ADR-0003.
- Healthcheck Postgres : `pg_isready -U <user> -d <db>`, identique au style
  existant.
- Réseau et MinIO réutilisés, pas recréés : `docker-compose.nessie.yml`
  déclare `duckhouse-net` comme réseau **externe**
  (`datahut-duckhouse_duckhouse-net`, nom vérifié via `docker network ls`)
  et ne définit aucun service MinIO -- il suppose que `docker compose up -d
  minio` (stack principale) a déjà tourné.
- Secrets de test jetables en clair dans le compose (comme
  `chinook-postgres`/`chinook-mysql`), pas dans `.env` : Postgres de Nessie
  n'a aucune donnée sensible. Les identifiants MinIO, eux, restent lus
  depuis `.env` via `env_file:` + substitution `${MINIO_ROOT_USER:?...}`,
  exactement comme `docker-compose.yml`.
- Pas de Dockerfile créé : image officielle épinglée (`ghcr.io/
  projectnessie/nessie:0.108.4-java`), conforme à la règle "sinon, image
  officielle épinglée".
- Ports : `19120` (Nessie REST, port par défaut du projet, libre, vérifié
  via `docker ps`/`netstat`) et `5434` (nessie-postgres, libre -- `5433` et
  `3307` étaient déjà pris par les fixtures Chinook).

## Results

| # | Spike | Result | Key numbers / exact command |
|---|-------|--------|------------------------------|
| S0 | pyiceberg catalog types | **réussi** | `uv run python scripts/spikes/s0_check_catalog_types.py` → pyiceberg 0.11.1, types = `rest, hive, glue, dynamodb, sql, in-memory, bigquery`. No `"nessie"` type exists. **Impact ADR**: the ADR's wording `catalog_type="nessie"` is wrong; it must read `catalog_type="rest"` with `uri` pointed at Nessie's `/iceberg` endpoint. |
| S1 | Nessie+Postgres+MinIO, pyiceberg create/append from host and container | **partiel** | See below. |
| S2 | Concurrent commits (2 processes, N=50 each) | **réussi** | 100/100 rows, 0 duplicates, 6 conflict-retries total |
| S3 | Branches: write on `dev`, merge to `main`, tag, rollback | **réussi** | all 7 steps matched expectations exactly, see below |
| S4 | Trino reads Nessie-backed tables | **réussi** | counts matched exactly on 3 tables; read a specific branch too |
| S5 | Register an existing SQLite-catalog table in Nessie | **réussi** | same rows, same data file, zero copy |
| S6 | Commit latency on Nessie | **réussi** | p50 0.075s (1k rows), p50 0.264s (1M rows) -- faster than the 0.34s/1.0s baselines |
| S7 | Retention: delete branch, expire snapshots | **partiel** | branch delete frees 0 bytes immediately; client-driven expire_snapshots is flatly rejected |
| S8 | Solo-mode `get_catalog()` prototype | **réussi** | same smoke test passes in both modes; refuses cleanly, exit 2, in solo |

### S0 details (vérifié)

```
$ uv run python scripts/spikes/s0_check_catalog_types.py
pyiceberg version: 0.11.1
available catalog types: ['rest', 'hive', 'glue', 'dynamodb', 'sql', 'in-memory', 'bigquery']
"nessie" is a CatalogType member: False
```

### S1 details

**Setup (vérifié):** `docker compose -f docker-compose.nessie.yml up -d` pulled
`ghcr.io/projectnessie/nessie:0.108.4-java` and started cleanly on the first
try. Server log confirms the Postgres-backed JDBC2 store self-creates its
schema with no separate migration step:

```
Selected datasource: postgresql
Creating/opening version store JDBC2 ...
Using JDBC2 version store (catalog: nessie, schema: public, table 'refs2' created, table 'objs2' created)
Nessie 0.108.4 on JVM (powered by Quarkus 3.37.4) started in 9.114s.
```

**Reference selection (vérifié):** confirmed by reading Nessie's own REST
config response (`curl http://localhost:19120/iceberg/main/v1/config`): the
`prefix` property *is* the reference. pyiceberg's `load_catalog(..., prefix="main")`
(or `"dev"` once that branch exists, S3) selects the branch -- not a URI path
segment baked into `uri`, not a header. `GET /iceberg/v1/config` (no prefix)
returns Nessie's own identity (`nessie.default-branch.name: "main"`,
`nessie.prefix-pattern: "{ref}|{warehouse}"`).

**CRUD across host and a container (partiellement vérifié):**

- Container → container (via a disposable `python:3.13-slim` container on
  `duckhouse-net`, see "Windows pitfall" below for why this stands in for
  "a container" rather than literally reusing `flight-server`): created
  table `spike.s1_crud`, appended 5 rows, read them back. **Vérifié**,
  exact output:
  ```
  [container] spike.s1_crud on branch main: 5 rows
  {'id': 0, 'origin': 'container'} ... {'id': 4, 'origin': 'container'}
  ```
- Host → create table via pyiceberg REST catalog: **succeeds** (metadata-only
  operation; Nessie itself writes the initial `metadata.json` to MinIO using
  its own server-side S3 connection, so the client never touches S3 for
  `create_table` with no data).
- Host → **append rows** (write actual data/manifest files): **fails**, not
  a Nessie bug -- see "Two real bugs found" below. Not fixed (blocked > 45 min
  on the second one), documented instead.

**Two real bugs found while running S1 (vérifié):**

1. **`s3fs` is unconstrained in `pyproject.toml`** (`"s3fs"`, no version) and
   `uv` resolved it to **`s3fs==0.4.2`** (released 2020-03-31), wildly
   incompatible with the installed `fsspec 2026.7.0`/`botocore 1.43.83`.
   Nessie's REST catalog **forces** `py-io-impl=pyiceberg.io.fsspec.FsspecFileIO`
   per table (confirmed: `table.config` returned by the server literally
   contains `'py-io-impl': 'pyiceberg.io.fsspec.FsspecFileIO'`, which
   overrides any client-side `py-io-impl` -- table-level REST config always
   wins over catalog-level client properties in pyiceberg's
   `RestCatalog._load_file_io`). With the old `s3fs`, every data write
   crashes:
   ```
   TypeError: botocore.session.Session.create_client() got multiple values for keyword argument 'aws_access_key_id'
   ```
   Root cause: `s3fs 0.4.2`'s `S3FileSystem.connect()` passes
   `aws_access_key_id=self.key` positionally *and* pyiceberg's
   `client_kwargs` dict *also* sets `aws_access_key_id`, which 0.4.2-era
   s3fs didn't dedupe. **Fixed, ad hoc, for spike runs only**: an unpinned
   override (`uv run --with "s3fs>=2024.1.0" ...` on the host, or
   `pip install 'pyiceberg[s3fs,pyarrow]==0.11.1'` -- pulls a modern `s3fs`
   -- inside the disposable container run by `scripts/spikes/run_in_container.sh`).
   No production file was changed. **Impact ADR / follow-up**: `pyproject.toml`
   should pin `s3fs>=2024.1.0` (or drop the extra and force
   `py-io-impl=pyiceberg.io.pyarrow.PyArrowFileIO` catalog-wide, which is
   moot against Nessie since the server overrides it anyway) before any
   production code talks to a REST catalog that vends `s3.*` config per
   table. This is a real, previously-latent bug independent of Nessie --
   today's SQL/in-memory catalogs never hit this path.

2. **A client running on the Windows host cannot resolve Nessie's vended
   `s3.endpoint` (`http://minio:9000/`).** Metadata operations (create
   table, list namespaces, read the REST config) go through Nessie itself
   (port `19120`, published, reachable at `localhost`) and work fine from
   the host. But actual data-file writes/reads go **client-side straight to
   S3**, using whatever `s3.endpoint` Nessie was configured with
   (`minio:9000` -- the Docker-network name, since Nessie itself always
   runs inside a container). That name does not resolve from the raw
   Windows host (`nslookup host.docker.internal` from the host also fails,
   confirmed -- there is no symmetric hostname). Fix would be a
   `127.0.0.1 minio` line in `C:\Windows\System32\drivers\etc\hosts`, which
   needs administrator elevation; **not applied** (classifier correctly
   refused the write, and this is exactly the kind of system-wide,
   hard-to-reverse-without-asking change this task's rules say to check
   first, so it was left alone and documented rather than worked around).
   This is a **Windows dev-box-only** limitation: in the target deployment
   everything (Flight server, ingestion, Trino, Nessie) runs inside the
   same Docker network, so no component other than a developer's bare host
   process would ever hit it.

**Conclusion for S1:** pyiceberg's REST catalog protocol against Nessie
works correctly for both metadata and data operations once a compatible
`s3fs` is used; the catalog-sharing claim central to ADR-0006 (a table
written by one actor is visible to another through the same REST catalog)
is verified between two independent container processes. The host-side data
path specifically is blocked by a local Windows networking fact, not by
Nessie or pyiceberg.

### S2 details (vérifié)

Two disposable containers (see S1's Windows pitfall for why not literally
"host + container") each appended 50 single-row commits to the same new
table concurrently (`&` + `wait` in bash, true wall-clock overlap). Exact
output:

```
[A] committed 50 rows, 6 conflict-retries
[B] committed 50 rows, 0 conflict-retries
[check] final total rows: 100, by actor: {'A': 50, 'B': 50}
[check] duplicate seq values for this actor: 0
```

No corruption, no lost or duplicated rows, exact expected count (100).
**Important nuance**: pyiceberg does **not** retry commits automatically.
`Table.append()` raises `pyiceberg.exceptions.CommitFailedException`
synchronously on a stale-base-metadata conflict (classic optimistic
concurrency: whoever commits second against a now-stale snapshot must
reload the table and retry); the spike script implements its own
retry-with-reload loop (10 attempts, linear backoff). Any production code
path that writes directly to a Nessie-backed table under concurrent writers
(the direct sink in ADR-0006 point 5) **must** implement the same
reload-and-retry pattern itself -- this is not free with the REST catalog.

### S3 details (vérifié)

pyiceberg manages Iceberg-table-level snapshot refs, not Nessie's
catalog-wide branches -- confirmed by reading pyiceberg's `Catalog` API
(no branch/tag/merge methods at the catalog level). Branch/tag/merge/
rollback need Nessie's own API. Chose the **REST API v2 directly** over
`pynessie` (extra dependency) and the Java `nessie-cli` (needs its own JVM)
-- `requests` is already a project dependency, satisfying "la plus légère
en dépendances". Exact endpoints (verified by fetching the live server's
own `/nessie-openapi/openapi.yaml` after two wrong guesses -- see
`scripts/spikes/nessie_api.py`):

- Create branch/tag: `POST /api/v2/trees?name=<new>&type=BRANCH|TAG`, body =
  the **source** reference object (not the new one -- this reversal is
  exactly what cost the two wrong guesses).
- Merge: `POST /api/v2/trees/{branch}@{expectedHash}/history/merge`, body
  `{"fromRefName": "dev", "fromHash": "<dev hash>"}`.
- Rollback/reassign: `PUT /api/v2/trees/{ref}@{expectedHash}?type=BRANCH`,
  body = the target reference to point at (a tag, another branch, or an
  older hash on the same branch).

Full run (`scripts/spikes/s3_branching.py`, exact output):

```
1. baseline on main: 3 rows (expect 3)
2. created branch 'dev' from main@e8de29a9
3. after writing 3 more rows on dev: dev=6 (expect 6), main=3 (expect 3, UNTOUCHED)
4. merged dev into main: {'wasApplied': True, 'wasSuccessful': True, ...}
   after merge: main=6 rows (expect 6)
5. created tag 'v1' at main@ca309f32 (6 rows via the tag, expect 6)
6. after 2 more rows on main post-tag: main=8 (expect 8)
7. rolled main back to tag v1: main=6 rows (expect 6, the pre-step-6 state)
```

Every step matched expectations exactly: writes on `dev` are invisible on
`main` until merged; the merge is clean (`wasSuccessful: True`, no
conflict); a tag freezes a point in history that survives further commits
on `main`; rolling `main` back via `assign` to the tag's hash restores the
pre-step-6 row count. **Impact ADR**: `dhd branch create|list|delete`,
`dhd merge`, `dhd tag`, `dhd rollback` (ADR-0006 point 4) map directly onto
`createReferenceV2`/`getAllReferencesV2`/`deleteReferenceV2`/`mergeV2`/
`assignReferenceV2` -- a thin CLI wrapper, no custom version-control logic
needed.

### S4 details (vérifié)

A disposable `trinodb/trino:latest` container (own `etc/` dir under
`scripts/spikes/trino-etc/`, **not** `config/trino/etc/` -- the existing
config was deliberately left untouched) with a minimal Iceberg REST catalog:

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://nessie:19120/iceberg/main
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.path-style-access=true
s3.aws-access-key=${ENV:AWS_ACCESS_KEY_ID}
s3.aws-secret-key=${ENV:AWS_SECRET_ACCESS_KEY}
```

Started clean on the first config fix (one typo: `node.environment` can't
contain a hyphen, `nessie-spike` → `nessie_spike`):
`-- Added catalog nessie using connector iceberg --` /
`======== SERVER STARTED ========`. Reading back via `trino --execute`:

```
SHOW TABLES FROM nessie.spike;        -- s1_crud, s2_concurrency, s3_branch_test
SELECT count(*) FROM nessie.spike.s1_crud;         -- 5  (matches S1 exactly)
SELECT count(*) FROM nessie.spike.s2_concurrency;  -- 100 (matches S2 exactly)
SELECT count(*) FROM nessie.spike.s3_branch_test;  -- 6  (matches S3's post-rollback main)
```

**Reading a specific branch (vérifié)**: added a second catalog file
(`nessie_dev.properties`) identical except
`iceberg.rest-catalog.uri=http://nessie:19120/iceberg/dev` (branch baked
into the URI path -- Trino's REST catalog connector has no separate
"reference" property, confirmed in its own startup property dump:
`iceberg.rest-catalog.prefix` exists but is for the REST catalog's generic
multi-tenant prefix mechanism, not Nessie's branch selection specifically;
the URI path is what worked). `SELECT count(*) FROM
nessie_dev.spike.s3_branch_test` → 6 rows, ids 0-5 -- `dev`'s state frozen
at merge time, independent of `main`'s separate rollback to the same count
by coincidence (different row set would have shown this more clearly, but
the branch isolation itself -- a second catalog, a second fixed snapshot --
is the point being tested, and it held).

**Static vs. vended credentials**: Trino's own property dump showed
`iceberg.rest-catalog.vended-credentials-enabled=false` by default, and
static `s3.aws-access-key`/`s3.aws-secret-key` worked without any issue --
unlike pyiceberg, Trino's Iceberg-REST connector apparently does **not**
force a specific S3 client implementation per table, so it never hit
anything resembling S1's `py-io-impl` override problem.

**On the old config's errors**: the main stack's existing `trino` container
was found stopped (`Exited (143)`, i.e. `SIGTERM` -- a clean stop, not a
crash) with no error lines in its retained log buffer. **Non vérifié**:
could not reproduce or confirm the "41 configuration errors" from the old
`iceberg.catalog.type=hadoop` setup with the logs currently available (the
buffer may simply not go back far enough). What **is** verified: the old
config's `iceberg.catalog.type=hadoop` + `hadoop-config.xml` path is the
heavier, legacy Iceberg-Trino integration; `iceberg.catalog.type=rest`
started cleanly with a fraction of the properties and no Hadoop filesystem
classes at all. Not fixed here per the task's instruction ("ne répare pas
l'ancienne config").

### S5 details (vérifié)

Created a fresh table (`src.orig`, 3 rows) in a throwaway pyiceberg **SQL
catalog** (`type=sql`, `sqlite:////tmp/s5_sqlite_catalog.db`), its data still
on MinIO (so Nessie can actually read it afterward), then called
`nessie_catalog.register_table("spike.s5_registered", metadata_location)`.
Exact output:

```
2. registered into Nessie as spike.s5_registered, its own metadata_location: s3://.../00001-....metadata.json
   same metadata_location (no copy at register time): True
3. rows read back through Nessie: [{'id': 1, 'label': 'a'}, {'id': 2, 'label': 'b'}, {'id': 3, 'label': 'c'}]
   identical to original: True
4. same physical data files (no data copy): True
   data file path(s): ['s3://duckhouse-warehouse/nessie-spike-sqlite-source/src/orig/data/00000-....parquet']
```

Confirmed at three levels: the registered table's `metadata_location` is
**byte-identical** to the source's (not a new file), the rows read back
through Nessie match exactly, and `plan_files()` on both tables resolves to
the exact same Parquet object path. `register_table` is a pure metadata
operation -- no data (and in this case, not even a new metadata.json) is
copied. **Impact ADR**: migrating existing SQLite-catalog tables (e.g. from
today's `dhd --engine pyiceberg` local catalog) into Nessie is a cheap,
instant, per-table operation; a bulk-migration script is just a loop over
`list_tables()` + `register_table()`.

### S6 details (vérifié)

30 commits of 1,000 rows each, then 5 commits of 1,000,000 rows each
(wall-clock budget did not allow 30 commits at 1M rows -- noted as a gap,
not claimed as done). Exact output:

```
[small] 30 commits x 1000 rows:
  min=0.066s  p50=0.075s  p95=0.259s  max=0.431s
[large] 5 commits x 1000000 rows:
  min=0.188s  p50=0.264s  p95=1.423s  max=0.969s
```

Both comfortably **faster** than the previously measured baselines (0.34s
local, 1.0s MinIO, both with the SQLite catalog) -- Nessie's JDBC2/Postgres
commit path is not a latency regression. Caveat: the `large` p95 (computed
via `statistics.quantiles(n=100)`) is a linear-interpolation artifact of
only 5 samples and comes out *above* the observed max -- not a real
latency measurement; only `large`'s `min`/individual durations should be
trusted. First commit in each run is consistently the slowest (JIT/
connection warmup), consistent with the local-catalog baseline's own notes.

**Memory** (`docker stats --no-stream`, vérifié, after the above runs):

| Container | Memory |
|---|---|
| `nessie` | 366.6 MiB |
| `nessie-postgres` | 40.8 MiB |

~407 MiB combined for the catalog layer. Real, but not negligible next to
the "light, laptop-friendly" positioning -- a concrete number for that
trade-off discussion, not a verdict on it.

### S7 details (vérifié)

Exact output (object counts under the spike's own MinIO prefix, via
`boto3 list_objects_v2`):

```
0. baseline: 582 objects, 9545728 bytes under s3://duckhouse-warehouse/nessie-spike/
1. after deleting branch 'dev': 582 objects, 9545728 bytes (delta: +0 objects, +0 bytes)
2. client-driven expire_snapshots() on spike.s3_branch_test (1 snapshots): REJECTED by the server
   CommitStateUnknownException: UnsupportedOperationException: Nessie Catalog does not allow external snapshot management
```

**Deleting a branch recovers nothing immediately (vérifié).** `dev`'s
commits were merged into `main` earlier (S3), so they stay reachable
through `main`'s history after `dev` itself is deleted -- expected, not a
bug: Nessie's history is commit-graph-based, like git, and deleting a
branch pointer doesn't orphan commits still reachable from elsewhere.

**pyiceberg's standard `expire_snapshots()` is explicitly refused by
Nessie's REST catalog (vérifié)**, not silently ignored: a clean
`UnsupportedOperationException: Nessie Catalog does not allow external
snapshot management`. Consistent with `s3_branch_test` showing only **1**
Iceberg-level snapshot despite 7 operations across S3's scenario -- Nessie
tracks table history through its own commit graph, not by accumulating
Iceberg snapshot-list entries the way a bare (non-catalog-versioned) table
does. Retention/GC on Nessie is therefore **not** the usual
"pyiceberg expires snapshots, then something sweeps orphan files" pattern.

**Non vérifié**: Nessie ships a separate `nessie-gc` tool (its own jar/
image, `ghcr.io/projectnessie/nessie-gc`) that is the documented way to
actually reclaim storage (it walks all branches' reachable history across
the whole repository, not a single table). Not run here -- it needs its
own setup and a time budget this spike didn't have left; flagged as a
follow-up, not attempted as a workaround.

**Impact ADR**: point 7 ("a snapshot-expiry and cleanup policy must be
defined... before the first client deployment") is confirmed necessary and
non-trivial: it is not a pyiceberg-side concern once Nessie is adopted, it
is specifically `nessie-gc`, which is unverified here and should be its own
follow-up spike before any retention policy is promised to a client.

### S8 details (vérifié)

`scripts/spikes/s8_get_catalog.py` (prototype only, not a production
suggestion for file location -- the real factory belongs in production
code if/when ADR-0006 is accepted): `get_catalog(mode)` reads
`CATALOG_MODE` (`solo`/`deployment`) and returns either a local SQLite
`SqlCatalog` or a Nessie-backed `RestCatalog`, nothing else branches on
mode. One `run_smoke_test(catalog)` function (create table, append 3 rows,
read them back, assert) is called identically in both runs. Exact output:

```
=== CATALOG_MODE=solo ===
1. got a SqlCatalog from get_catalog()
   smoke test passed: 3 rows round-tripped correctly
2. versioning command (dhd branch create stub):
   refused as expected: exit code 2, message: error: `dhd branch create` needs deployment mode (Nessie catalog) -- solo mode (CATALOG_MODE=solo) has no branches/tags/rollback. ...

=== CATALOG_MODE=deployment ===
1. got a RestCatalog from get_catalog()
   smoke test passed: 3 rows round-tripped correctly
2. versioning command (dhd branch create stub):
   (deployment mode: would call nessie_api.create_reference(...) here)
```

Confirms ADR-0006 point 2 ("two modes, same code, configuration only") and
point 4 (solo mode's versioning commands fail with exit code 2 and a clear
message) are both implementable exactly as specified, with no mode-specific
branching beyond the catalog construction itself.

## Pièges et surprises

- **`s3fs` unpinned** (see S1, bug 1) -- latent, project-wide, not Nessie-specific.
- **Windows host ≠ container DNS** (see S1, bug 2): any spike needing the
  host to write/read actual Iceberg data files (not just catalog metadata)
  needs either a hosts-file entry or, as done here, a disposable container
  standing in for "the host" process. Noted per spike below where this
  substitution was used.
- `docker exec` into an already-running production-shaped container
  (`flight-server`) to `pip install` something ad hoc was refused by the
  permission classifier ("Remote Shell Writes") -- used a brand-new
  disposable `python:3.13-slim` container instead
  (`scripts/spikes/run_in_container.sh`), which is also a cleaner, fully
  reversible stand-in for "a second process" in S2.
- Git-bash (MSYS) rewrites `/absolute/paths` in `docker run` arguments
  unless `MSYS_NO_PATHCONV=1` is set -- hit immediately on the first
  `docker run -v ... -w /spikes` call; worked around in
  `run_in_container.sh`.

## Impact on ADR-0006 (proposed changes, not applied to the ADR itself)

- Fix the wording `catalog_type="nessie"` → `catalog_type="rest"` (pyiceberg
  has no `nessie` catalog type; confirmed S0).
- Add an explicit note that `s3fs` must be pinned to a modern version
  (`>=2024.1.0`) before any component talks to Nessie's REST catalog for
  data (not just metadata) operations -- this is a new, previously-latent
  requirement introduced by adopting a REST catalog that vends per-table
  `py-io-impl`/`s3.*` config, not something today's SQL-catalog code path
  needed (S1).
- Add an explicit assumption under "Assumptions to Verify": every writer
  (including developer laptops in solo/dev mode, not just deployed
  components) must be able to resolve the configured S3 endpoint by name --
  true by construction inside Docker Compose/Kubernetes, **not** true for a
  bare host process on Windows without a hosts-file entry (S1).
- Point 5 ("the direct sink becomes the default for replace/append/merge")
  needs an explicit retry-with-reload policy against `CommitFailedException`
  -- pyiceberg does not retry commits automatically; this is new work the
  direct sink does not need today against a single-writer SQLite catalog (S2).
- Point 4 (`dhd branch`/`merge`/`tag`/`rollback`) can map directly onto
  Nessie's REST API v2 (`createReferenceV2`/`mergeV2`/`assignReferenceV2`/
  `deleteReferenceV2`) -- confirm this in the ADR's decision text so the
  implementation isn't left to guess between the REST API, `pynessie`, and
  the Java CLI (S3).
- Point 6 (Trino follow-up): `iceberg.catalog.type=rest` works and is
  confirmed simpler than the current `hadoop` setup; the ADR could name
  this explicitly as the replacement catalog type for Trino rather than
  leaving it unspecified (S4).
- Point 7 (retention) needs to name `nessie-gc` specifically, not just "a
  cleanup policy" -- pyiceberg's own `expire_snapshots()` is flatly rejected
  by Nessie's REST catalog, so whatever policy gets written cannot be a
  pyiceberg-side mechanism (S7, **non vérifié** beyond confirming the
  rejection -- `nessie-gc` itself was not run).

## Non vérifié

- Whether `s3fs>=2024.1.0` is sufficient long-term or whether pyiceberg
  should instead be told to ignore the server's `py-io-impl` override
  entirely (not currently possible client-side; would need a pyiceberg
  change or a Nessie-side config to stop vending `py-io-impl` for S3).
- Nessie's authentication/authorization story (left disabled here,
  `NESSIE_SERVER_AUTHENTICATION_ENABLED=false`, matching the ADR's own
  "Negative/risks" note that this is acceptable for a dedicated
  per-client deployment only).
- The "41 configuration errors" claimed for the old Trino `hadoop` catalog
  config (S4) -- the running container was found stopped cleanly (SIGTERM),
  not crashed, and no error lines survived in its log buffer.
- `nessie-gc`, the tool that actually reclaims storage (S7) -- not run,
  flagged as a follow-up spike of its own.
- S6's 1M-row latency test at only 5 commits, not the full 30 (time budget).
- Nessie's merge-conflict UX in a realistic scenario (S3's merge was
  conflict-free by construction; S2 exercised append-conflicts on a single
  branch, not a cross-branch merge conflict).

## Recommendation

**Accepter avec changements.** S1 through S8 all ran (S1 and S7 partially --
see their sections above); nothing found blocks the REST-catalog approach
itself, but three changes to the ADR are warranted before "Accepted":

1. Fix `catalog_type="nessie"` → `catalog_type="rest"` (S0) -- purely
   editorial, but currently wrong.
2. Add the `s3fs>=2024.1.0` pin as an explicit prerequisite, independent of
   whether this ADR is accepted (S1) -- this is a real, previously-latent
   bug in `pyproject.toml` found *while* verifying this ADR, not caused by it.
3. Name `nessie-gc` explicitly as the retention mechanism in point 7, since
   pyiceberg's own snapshot-expiry API does not work against Nessie at all
   (S7) -- and treat it as its own follow-up spike before promising any
   retention SLA to a client.

Everything else tested -- cross-process visibility (S1, container-to-
container), concurrent writers (S2), branch isolation/merge/tag/rollback
(S3), Trino reads including branch-specific reads (S4), zero-copy migration
of existing tables (S5), and commit latency/memory (S6) -- came back
positive, with commit latency *better* than the previously measured
baseline and memory (~407 MiB) a known, named cost rather than an unknown
one. Solo-mode's config-only mode switch (S8) is implementable exactly as
the ADR describes.
