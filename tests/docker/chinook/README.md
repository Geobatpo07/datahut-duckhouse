# Chinook test fixtures

Vendored from the official [lerocha/chinook-database](https://github.com/lerocha/chinook-database)
repository, pinned at commit
[`7f67772503d71ba90f19283c38e93923addb43fa`](https://github.com/lerocha/chinook-database/commit/7f67772503d71ba90f19283c38e93923addb43fa)
(2026-11-08), so a `docker compose` run always loads the exact same dataset
regardless of what changes upstream later.

- `postgres/Chinook_PostgreSql.sql` — from `ChinookDatabase/DataSources/Chinook_PostgreSql.sql`
  at that commit. Mounted as a Postgres `docker-entrypoint-initdb.d` script; it
  creates its own `chinook` database and `\c`s into it, which is why
  `docker-compose.test.yml` points `POSTGRES_DB` at the default `postgres` db.
- `mysql/Chinook_MySql.sql` — from `ChinookDatabase/DataSources/Chinook_MySql.sql`
  at that commit. Mounted as a MySQL `docker-entrypoint-initdb.d` script; it
  creates and `USE`s its own `` `Chinook` `` database.
- `mssql/` — see its own note; **not run or verified in this pass** (optional
  compose profile, disabled by default).

License: [MIT](https://github.com/lerocha/chinook-database/blob/7f67772503d71ba90f19283c38e93923addb43fa/LICENSE.md)
— see [docs/THIRD_PARTY_CHINOOK.md](../../../docs/THIRD_PARTY_CHINOOK.md) for
the full text, vendored here under its terms.

The SQLite version is **not vendored as a binary file** — `scripts/fetch_chinook.py`
downloads `Chinook_Sqlite.sqlite` (~1 MB) from the same pinned commit into
`tests/data/` on demand (used by `dhd demo` and the SQLite integration test).
