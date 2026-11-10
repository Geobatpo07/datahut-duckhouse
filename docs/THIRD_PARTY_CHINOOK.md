# Third-party data: Chinook

The multi-engine ingestion benchmark (Vague 1, see
[docs/ROADMAP.md](ROADMAP.md)) uses the **Chinook** sample database — a
fictional digital music store (artists, albums, tracks, customers, invoices).
It is not DataHut-DuckHouse's own data; it is a widely-used, small, relational
test dataset with ready-made load scripts for SQLite, PostgreSQL, MySQL and
SQL Server, which is exactly what's needed to prove multi-engine ingestion
with dlt before AYITISTATS data is wired in (a later session).

- Source: [lerocha/chinook-database](https://github.com/lerocha/chinook-database)
- Pinned at commit [`7f67772503d71ba90f19283c38e93923addb43fa`](https://github.com/lerocha/chinook-database/commit/7f67772503d71ba90f19283c38e93923addb43fa)
  (2026-11-08) for every file this repo uses or downloads from it.
- License: MIT (Copyright (c) 2008-2024 Luis Rocha). Full text below, copied
  verbatim from that commit's `LICENSE.md`.

Files vendored under `tests/docker/chinook/` (`Chinook_PostgreSql.sql`,
`Chinook_MySql.sql`) and downloaded on demand by `scripts/fetch_chinook.py`
(`Chinook_Sqlite.sqlite`) are redistributed under the terms below.

---

```
Chinook Database
--------------------------------------
Copyright (c) 2008-2024 Luis Rocha

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
```
