# 07 — PostgreSQL → SQLite → DuckDB

An analytical job over a synced replica. The bridge keeps a SQLite file current
through `libzb`; DuckDB attaches that file and runs the query. PostgreSQL is never
touched by the job, and the replica keeps syncing while DuckDB reads it.

```sh
set -a && . ./.env.bridge && set +a
pip install duckdb
ZB_PRINCIPAL=bob ZB_TABLES=test_types python examples/07-duckdb/analyze.py
```

What it does, in order: opens the replica with the C card (twenty lines of ctypes,
every string in and out is JSON), syncs — a fresh replica seeds the tables from the
chain, one object plus the deltas after it — then `ATTACH` in DuckDB with
`TYPE sqlite, READ_ONLY`, and two analytical queries over the table.

Two things to know. The replica's columns are the wire's shapes: timestamps are ISO
text and numerics are text, so money wants one `CAST(price AS DECIMAL(20, 8))`,
best kept in a view. And the file is a live replica, not a frozen export: a fresh
machine seeds it in the time the chain takes and DuckDB reads whatever the last poll
applied.

The same shape fits a warm micro-VM: the replica file synced continuously, the job
fired on demand, PostgreSQL untouched.

## The native route: libzb's DuckDB engine

Since §10fl libzb can write the replica straight into a `.duckdb` file — no SQLite
in the middle — when built with the engine (`cd libzb && zig build
-Doptimize=ReleaseFast -Dduckdb=true`; `duckdb.h` and `libduckdb` from the host, the
Homebrew keg or the release zip). The worker is then one command:

```sh
libzb/zig-out/bin/zb sync --creds scripts/native/creds/bob.creds --principal bob \
  --tables test_types --db /tmp/replica.duckdb --engine duckdb --once
duckdb /tmp/replica.duckdb "SELECT tenant_id, count(*), avg(age) FROM test_types GROUP BY 1"
```

Measured on the 3 M-row table: the seed in 32 s through DuckDB's appender, the
group-by in 41 ms against 856 ms on the attached SQLite file, and `COPY … TO
'x.parquet'` in under a second. The columns are DuckDB's own types (the numeric is a
DECIMAL, the timestamps TIMESTAMP WITH TIME ZONE, arrays lists), so no view of casts
is needed. DuckDB allows one open of a file per process: run the job after `zb` has
exited, or keep `zb` following and open the file read-only from another process
between its runs.
