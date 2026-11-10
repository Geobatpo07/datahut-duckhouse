# Draft upstream issue: `xorq`

**Status: draft, not posted.** Prepared on `fix/segfault-diagnosis` (2026-11-08)
while diagnosing a 100%-reproducible `dhd create-table` / `dhd query` segfault
against a real Dockerized Flight server. Root cause is isolated and verified
(see `docs/adr/0001-canonical-hybrid-backend.md`'s "Segfault root cause"
note) to a cross-Python-minor-version `cloudpickle` failure inside `xorq`'s
vendored `ibis`, not to anything in this repo's own code. Posting this is
left to whoever owns the relationship with the `xorq` project — this file is
ready to paste as-is.

---

## Title

`cloudpickle.loads()` of a vendored `ibis` `Schema`/`Annotable` across a
Python minor-version boundary: `SystemError` or a hard segfault in
`Annotable.__recreate__`

## Versions

- `xorq` 0.4.11 (latest published on PyPI as of 2026-11-08; no 0.5.x exists)
- `pyarrow` 20.0.0 (server) / 20.0.0 and 21.0.0 (two clients tested, see below)
- `cloudpickle` (whatever pin `xorq==0.4.11` resolves — not independently varied)
- `duckdb` 1.1.3, `pyiceberg` 0.11.1 — present in the process but not implicated
  by the isolation below (the bug reproduces with a bare
  `pyarrow.flight.FlightClient` + `cloudpickle`, no `duckdb`/`pyiceberg` import
  at all)
- Server: Python 3.11.18, Linux (Debian bookworm slim container)
- Client A (crashes): Python 3.12.7, Windows 11
- Client B (does not crash): Python 3.11.15, Windows 11 — same Windows machine
  as Client A, different Python minor version only
- Client C (does not crash): Python 3.11.18, Linux — run from inside the
  server's own container

## Summary

A Flight `do_action` round trip that returns a cloudpickled
`xorq.vendor.ibis.expr.schema.Schema` (a subclass of
`xorq.vendor.ibis.common.grounds.Annotable`/`Concrete`) works when client and
server run the **same Python minor version**, and segfaults the client (or
raises a `SystemError` from a C-API contract violation, depending on process
state) when they don't. We saw both failure modes from the *identical* saved
payload bytes:

- Live, through the real client code path: `Windows fatal exception: access
  violation` (SIGSEGV, exit 139), 100% reproducible.
- The same saved bytes replayed standalone (same process generation, no
  network) deterministically instead raise:

  ```
  SystemError: <function Annotable.__recreate__ at 0x...> returned NULL
  without setting an exception
  ```

A `SystemError` with that message is a signal that some C-level code path
(CPython's own call machinery, or a Cython-compiled dependency) returned
`NULL` without setting an exception — undefined-behavior-adjacent. That the
same trigger shows up as either a catchable `SystemError` or a hard crash
depending on surrounding process state is consistent with memory corruption
rather than an ordinary, deterministic bug.

## Where

- `xorq/flight/action.py::GetSchemaQueryAction.do_action` — server side,
  returns `make_flight_result(schema)` where
  `make_flight_result = toolz.compose(paf.Result, pa.py_buffer, dumps)`
  (`dumps` = `cloudpickle.dumps`).
- `xorq/flight/client.py::FlightClient._do_action` — client side, does
  `map(loads, (result.body.to_pybytes() for result in
  self._client.do_action(...)))` (`loads` = `cloudpickle.loads`).
- `xorq/vendor/ibis/common/grounds.py::Concrete.__reduce__` — returns
  `(self.__recreate__, (state,))`: a *bound classmethod reference* as the
  pickle reconstructor, which cloudpickle must resolve against the
  unpickling process's own import of the class.
- `xorq/vendor/ibis/common/grounds.py::Annotable.__recreate__` — where the
  crash/`SystemError` is attributed.

Any Flight action that round-trips a `Schema`/`Annotable` this way is
affected. In `xorq`'s own `GetSchemaQueryAction`, that's every caller of
`_get_schema_using_query` (`xorq/flight/backend.py`) — in this project, that
is `dhd create-table`'s post-write confirmation and `dhd query`'s schema
resolution. `dhd list-tables` / `dhd insert` don't hit this path and never
crash, which matches what we observed.

## Reproduction

Minimal reproducer (no `duckdb`, no `pyiceberg`, no `HybridBackend` — just
`pyarrow.flight` + `cloudpickle` against a real running `xorq` Flight
server), attached in full at `scripts/repro_segfault.py` in this repo:

```python
import pyarrow.flight as paf
from cloudpickle import dumps, loads

client = paf.FlightClient("grpc://<flight-server-host>:8815")

# 1. Sanity: a plain-string action survives fine regardless of Python version
# (e.g. xorq's own `version` action) -- only Annotable-class payloads are hit.

# 2. The actual trigger:
action = paf.Action("get_schema_using_query", dumps("SELECT * FROM sales"))
results = list(client.do_action(action))      # clean: this part never crashes
raw = results[0].body.to_pybytes()             # clean: this part never crashes
schema = loads(raw)                            # <-- segfault or SystemError here,
                                                #     IFF client Python minor
                                                #     version != server's
```

Run the *same* code from a Python process matching the server's minor
version (same machine, different venv; or inside the server's own
container) and it returns a normal `Schema` object with no error at all.

## What was ruled out

- **Not MinIO/S3-specific.** Reproduces identically against a local
  filesystem warehouse once the Python-version variable is controlled for
  (and does *not* reproduce at all, warehouse notwithstanding, once client
  and server Python versions match).
- **Not Windows-specific.** A Windows client on the *matching* Python minor
  version (3.11) against the real Linux/Docker server does not crash. Only
  the *mismatched* combination (Windows, Python 3.12) crashes. (We didn't
  test a mismatched Linux client, so we can't rule out that Windows makes
  the specific failure *mode* more likely to be a hard segfault rather than
  a `SystemError` — only that Windows alone, version-matched, is not
  sufficient to trigger it.)
- **Not `xorq`-version-specific in the sense of "fixed in a later
  release"** — 0.4.11 is the latest published version; there is no newer
  release to test against.
- **Not in `HybridBackend`** (this project's own code) — the bare
  `pyarrow.flight.FlightClient` reproducer never imports it.
- **Not in the gRPC transport itself** — `client.do_action(...)` and
  `result.body.to_pybytes()` both complete cleanly every time; only the
  subsequent `cloudpickle.loads()` is implicated.

## Not yet tried / open questions for upstream

- Whether this is a `cloudpickle` bug (mishandling a bound classmethod
  reconstructor, or a class registered as "not importable by reference" in
  a way that behaves differently across versions) or a bug in exactly how
  `Annotable`/`Concrete.__reduce__` structures its pickle payload (e.g.
  relying on CPython call internals that changed between 3.11 and 3.12).
- Whether pickling/unpickling plain Arrow (`pa.Schema`) instead of the
  vendored `ibis` `Schema` object for this specific action would sidestep
  the issue entirely (Arrow's own IPC schema serialization is explicitly
  designed to be cross-version-safe; a hand-rolled `cloudpickle` of a
  custom Python class is not).
- Whether this also affects other `Annotable`/`Concrete` payloads that cross
  the Flight boundary elsewhere in `xorq` (we only exercised
  `GetSchemaQueryAction`).

## Suggested fix direction (not ours to implement)

Serialize the schema for this action as Arrow's own `Schema.serialize()` /
`pa.ipc` bytes (version-stable across Python and Arrow versions by design)
instead of `cloudpickle`-pickling the vendored `ibis` `Annotable` object, and
reconstruct an `ibis.Schema` client-side with `Schema.from_pyarrow` — mirrors
exactly the client-independent fix already applied in this project's own
`HybridBackend._get_schema_using_query` (see ADR-0001), just one layer
further out, in `xorq`'s own Flight client/server pair.

## Workaround available today (documented, not imposed)

Keep the Flight client and server's Python **minor** version identical.
Verified to fully avoid the crash in both directions (matching-version
Windows client against the real Linux/Docker server: clean; mismatched
Windows client against the same server: 100% reproducible crash).

## Downstream mitigation shipped in this repo (`chore/python-matrix`, 2026-11-08)

Rather than just document the workaround, `datahut-duckhouse` now ships a
small client-side guard
(`datahut_duckhouse/flight_version_guard.py`) that makes the mismatch
impossible to hit silently, without waiting on an upstream fix:

- Registers a tiny custom Flight action at runtime via xorq's own existing
  `add-action` (no `xorq`/`HybridBackend` changes) that reports the
  server's `sys.version_info[:2]` as a plain `(major, minor)` tuple of
  ints. Plain values survive the cross-version `cloudpickle` round trip
  fine -- confirmed live, host Python 3.13 against a still-3.11 server, no
  crash -- since only `Annotable` instances (this issue's actual trigger)
  are affected.
- One thing ruled out along the way: the guard's action class can't
  subclass xorq's own `AbstractAction` -- its `name`/`description` are
  `classproperty` objects, which `cloudpickle` refuses to pickle at the
  class level (`TypeError: cannot pickle 'classproperty' object`, raised
  client-side, before any network I/O). A plain class with a `name` string
  attribute and a `do_action` classmethod is enough; the server's dispatch
  (`xorq/flight/server.py`) never actually needed the ABC itself.
- Every `dhd` command compares the server's answer to its own version
  before issuing anything that could round-trip a Schema/`Annotable`
  object, and refuses with a clear message on a mismatch instead of
  segfaulting. Verified live, both directions, against real Docker/MinIO
  containers.

This is a workaround for *this* downstream project's CLI, not a fix for the
underlying `cloudpickle`/`Annotable` fragility described above -- any other
caller of `xorq.flight` that round-trips a `Schema`/`Annotable` across a
Python-version boundary is still exposed. See
`.devcontainer/flight-server.Dockerfile` (now built for either Python 3.11
or 3.13 via `--build-arg PYTHON_VERSION=`) and the README's Python-version
matrix section for the full story and verification.
