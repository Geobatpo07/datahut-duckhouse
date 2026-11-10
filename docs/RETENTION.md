# Retention (ADR-0006, decision 7 / condition C2)

Branches and old snapshots keep files alive in storage. Two tools, run
periodically, together cover cleanup; neither alone is sufficient.

## 1. `nessie-gc` — routine cleanup of reachable tables' old snapshots

Cleans obsolete files of tables still reachable from a **current**
branch or tag (superseded snapshots past the cutoff you choose). Does
**not** touch Nessie's own commit history, and does **not** see
anything that belonged to a branch that has since been deleted (see
"Known gap" below).

Prerequisites: its own Postgres database for the "live contents set" it
builds while running — separate from both Nessie's own version-store
Postgres and ADR-0003's tenant metadata database. `docker-compose.nessie.yml`
already defines this as `nessie-gc-postgres`, gated behind the `gc`
profile so a normal `up -d` does not start it:

```bash
docker compose -f docker-compose.nessie.yml --profile gc up -d nessie-gc-postgres
```

Exact command (pinned image, matching the version this project runs —
check `docker-compose.nessie.yml`'s `nessie` service for the current pin):

```bash
docker run --rm --network datahut-duckhouse_duckhouse-net \
  -e AWS_REGION=us-east-1 \
  ghcr.io/projectnessie/nessie-gc:0.108.4 gc \
  -u http://nessie:19120/api/v2 \
  --jdbc-url jdbc:postgresql://nessie-gc-postgres:5432/nessiegc \
  --jdbc-user nessiegc --jdbc-password nessiegc \
  -c NONE \
  -I s3.access-key-id=<MINIO_ROOT_USER> -I s3.secret-access-key=<MINIO_ROOT_PASSWORD> \
  -I s3.endpoint=http://minio:9000 -I s3.path-style-access=true \
  -I client.region=us-east-1
```

Notes, verified live against this project's stack:

- `-u` is Nessie's **core** API base (`.../api/v2`), not the Iceberg
  REST endpoint `ICEBERG_CATALOG_URI` names (`.../iceberg`) — passing the
  Iceberg path fails with a 404 before `nessie-gc` even starts walking
  references.
- `-c NONE` keeps every snapshot in every *currently reachable* table's
  history (no cutoff); use a duration (e.g. `-c P30D`) or a commit count
  once there is a real retention policy to enforce. Needs `-e
  AWS_REGION=...` **and** `-I client.region=...` — `nessie-gc` is a Java
  client, so it does not read pyiceberg's `s3.region` property name the
  way the rest of this project's Python code does.
- Its own log, per table, says exactly how many files it found, how
  many it considered expired vs. live, and how many it deleted — read
  that output; it is the actual audit trail for a given run, not just
  this doc's description of what it does.

## 2. `orphan_sweep.py` — the gap `nessie-gc` cannot close

`nessie-gc` builds its live/dead bookkeeping by walking commit history
across **currently named** references. Once a branch is deleted, its
commits — and the files they point to — fall out of that bookkeeping
entirely: not marked live, not marked for deletion, just never
considered. Verified live (this project's spike and implementation
work): a branch created, written to, and deleted without merging left
files in storage that a full `nessie-gc` run's log never once mentioned.

`scripts/orphan_sweep.py` instead lists what is **physically in
storage** (`boto3`, independent of Nessie's own bookkeeping) and
compares that against every file reachable from the full snapshot
history of every table on every **current** branch/tag. A deleted
branch's files still show up in storage and are not in that reachable
set, so they show up as orphans here.

```bash
# report only (default) -- inside the Docker network, like any
# deployment-mode script that touches real data (see the workstation rule):
docker compose run --rm \
  -e ICEBERG_CATALOG_TYPE=rest -e ICEBERG_CATALOG_URI=http://nessie:19120/iceberg \
  -e ICEBERG_WAREHOUSE=s3://duckhouse-warehouse/<path>/ -e ICEBERG_BRANCH=main \
  --entrypoint /app/.venv/bin/python flight-server scripts/orphan_sweep.py

# delete what it found, with a confirmation prompt (or --yes to skip it):
... scripts/orphan_sweep.py --delete
```

Verified live (see the implementation report, step 7): a fresh branch
with an isolated table, deleted unmerged, produced 5 files `nessie-gc`'s
run never mentioned at all; `orphan_sweep.py`'s report found exactly
those 5. The report-only path and the detection logic are verified this
way; the `--delete` path's actual deletion call was **not** exercised in
this session (blocked by this environment's own safety guard against
bulk storage deletion) — it is the same `boto3.delete_objects` call
pattern used nowhere else in this project yet, so treat it as
code-reviewed but not live-tested until someone runs it once by hand.

## Recommended periodic procedure

1. Run `nessie-gc` (section 1) on a schedule appropriate to how fast
   your branches turn over commits.
2. Run `orphan_sweep.py` (report only) on a slower schedule (e.g.
   weekly) — it walks every table's full history on every reference, so
   it costs more per run than `nessie-gc`'s normal cleanup.
3. Review the orphan report before passing `--delete`: most of it should
   be explainable by branches you know were deleted unmerged. Anything
   unexplained is worth investigating before deleting it.

Neither tool deletes a Nessie *reference* itself — that is `dhd branch
delete` (ADR-0006, decision 4; see `docs/CATALOG.md`), which already
warns that a forced delete of unmerged work leaves exactly these files
behind for this procedure to find later.
