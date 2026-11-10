# Direct Postgres versus libzb: one fresh SQLite database

This measures one instance's **connection-to-queryable** time. It compares
`CREATE TABLE` + streamed Postgres `SELECT` + batched SQLite `INSERT` against
libzb's connection, schema provisioning and generation-chain seeding over NATS.
It runs one client at a time. Repetitions are sequential, with alternating order
and a fresh process and SQLite file for every sample.

The runner creates a disposable database, roles, authenticated NATS server and
bridge using the existing private-stack scenario helper. It does not modify an
existing ZeBridge deployment. Postgres must already be running locally with
`wal_level=logical`, a `postgres` administrator able to create databases and roles,
and local TCP authentication configured for that administrator (the scenario
helper uses `psql`). The cluster itself is not created or stopped by this runner.
No Docker or microVM runtime is required.

## Run

Build the current bridge and native client, and install the benchmark dependencies
in a virtual environment (Python 3.10+):

```sh
zig build -Doptimize=ReleaseFast
(cd libzb && zig build lib -Doptimize=ReleaseFast)
python3 -m venv /tmp/zb-seed-venv
/tmp/zb-seed-venv/bin/pip install -r scripts/compare/postgres/requirements.txt
/tmp/zb-seed-venv/bin/python scripts/compare/postgres/seed.py \
  --rows 50000 --rounds 5 --output /tmp/seed-comparison.json
```

`nats-server` must be on PATH. Set `ZB_PGBIN` if PostgreSQL's binaries are not on
PATH; on macOS the helper also discovers Homebrew PostgreSQL 18. `--pg-port`
selects a local cluster on a different port. `ZB_BRIDGE_BIN` and `ZB_LIB` can select
other builds, but keep them compatible and record the builds when sharing results.

Options:

| Option | Default | Meaning |
| --- | ---: | --- |
| `--rows` | 50000 | Rows in the private static fixture |
| `--payload-bytes` | 192 | Non-null note size; every 17th note is NULL |
| `--rounds` | 5 | Sequential pairs of direct/libzb measurements |
| `--batch-rows` | 50000 | Direct fetch/insert/commit batch and libzb seed chunk size |
| `--pg-port` | 5432 | Local Postgres administrator and source port |
| `--timeout` | 180 | Hard wall-clock limit per child, including preparation/checksum |
| `--output` | — | Save the JSON report, also printed to stdout |
| `--enrollment` | off | Also measure fresh invitation enrollment plus seeding, before stopping the bridge |

Progress goes to stderr. The JSON includes individual samples, direct-path phase
times, median seconds and `libzb_over_direct`: below 1 means libzb was faster;
above 1 means direct loading was faster. Failure or checksum mismatch exits nonzero
and does not produce a successful comparison report. Temporary replicas and the
private database, roles and services are cleaned up; scenario logs stay under
`/tmp/zb_rrseed-*` for diagnosis.

## What is timed

**Direct:** open a new Postgres connection as the bridge's reader role, open a new
SQLite file, set storage pragmas, create the application table and indexes, execute
a tenant-filtered `SELECT … ORDER BY id` using a server cursor, import every batch
with prepared `executemany` inserts and commits, then run the readiness count query.
Timestamp conversion to SQLite text is included. Selection follows the same RLS
and tenant as the NATS snapshot. Fetch and insertion overlap at batch boundaries;
their combined time is reported as `select_and_insert`.

**libzb:** call the C ABI's connect and sync, then poll until the local table holds
the expected rows **and has a generation watermark**. The watermark prevents CDC
delivery alone from passing as a generation seed. The timer includes schema
retrieval, local table creation, object download, decoding, commits, and the
readiness query. No Python background polling thread or fixed sleep is involved.

All fixture columns are transferred on both paths. The direct path uses the same
application table/index DDL as an untimed prepared replica, supplied before timing
as an app would supply its known local schema. It does not discover schema from
Postgres during the timed interval. Both paths use 16 KiB SQLite pages, WAL,
`synchronous=NORMAL`, a 10,000-page automatic checkpoint interval and a 128 MiB seed
page cache, matching libzb's current settings. The report records the actual DDL.
libzb's internal replication bookkeeping remains part of its measured cost.

The timer stops with committed, queryable data, before closing connections. Close
and checksum work are excluded equally. After close, a new SQLite reader checks
every fixture column in primary-key order against a SHA-256 computed from
Postgres. Counts alone are insufficient: changed values, NULLs and types affect
the checksum. The file must remain readable after its writer closes. NORMAL/WAL
does not imply that the last commit survives power loss.

## What this result means

The generation is prepared and validated before timing, and credentials are minted
before timing. The bridge is then stopped for **both** paths so generation building
and replication do not compete with a timed client. NATS serves existing objects
and retained streams. Postgres remains available for direct loading. This is a
static, prepared-service experiment: it excludes generation construction, enrollment,
schema changes, live writes and continuous CDC catch-up. Cache contents on the
servers and OS are not flushed; "fresh" refers to the client database, not a cold
Postgres or filesystem cache. Preparing and checksumming the fixture warms caches.

The payload repeats an MD5 string, matching the earlier comparison fixtures. It is
compressible; these numbers cannot predict performance for incompressible blobs.
The direct path uses psycopg/Python insertion while libzb imports in native code,
and the two SQLite runtimes can differ. This measures the two complete loading
implementations, not NATS protocol overhead in isolation or the fastest possible
Postgres importer. `COPY` and a native direct importer are separate future baselines.

Container/microVM launch time is excluded. Establish this initialization baseline
first; measuring the same work inside constrained instances is a separate run.

## Invitation enrollment

Add `--enrollment` to exercise the first-run client path:

```sh
/tmp/zb-seed-venv/bin/python scripts/compare/postgres/seed.py \
  --rows 50000 --rounds 5 --enrollment --output /tmp/enrollment-comparison.json
```

After validating the prepared generation, the harness keeps the bridge running
and performs one enrollment/seed per round, sequentially. Each gets a new one-use
invitation code, identity path, SQLite file and child process. The principal and
tenant are the same, representing additional devices for the same user. Invitation
creation is untimed. The client receives only the bridge URL, invite, identity/DB
paths, table list and seed options: no pre-minted credentials, NATS URL override,
principal override or domain override. The endpoint supplies those connection facts.

The JSON `enrollment` object reports three boundaries and the combined duration:

| Metric | Boundary |
| --- | --- |
| `enrollment` | libzb's existing internal timer: device key generation, HTTP invite redemption, response processing and identity persistence; reported at 1 ms resolution |
| `connect_with_enrollment` | Immediately before `zb_client_connect` until it returns; includes enrollment, NATS authentication and local client initialization |
| `sync_to_ready` | From successful connect until all expected rows and the generation watermark are present; some initial work can already occur inside connect |
| `total` | Immediately before connect until the local database is ready; measured directly, not by adding medians |

The `enrollment` phase is **inside** `connect_with_enrollment`, so these must not be
added together. It is extracted from libzb's own `enrolled in … ms` log, not estimated
by subtracting separate runs. A missing or ambiguous enrollment timing fails the
run. After timing, each identity must contain credentials for the expected
principal, its invitation must be marked used in Postgres, and its complete local
database must pass the same checksum verification as the other paths.

These measurements use HTTP and TCP over loopback with the bridge's real enrollment
endpoint and NATS JWT authentication. They exclude user sign-in, code delivery,
TLS, remote DNS/network latency and generation construction. The report records
that the bridge is running during enrollment measurements. Afterwards the bridge
is stopped and the original direct/pre-minted-credential comparison runs as before;
its `enrollment_timed: false` describes those baseline samples only.

Measured on 2026-11-10 with the same 50,000-row local fixture and runtime versions
as below, five fresh devices produced these results ([raw report](results/local-enrollment-50000.json)):

| Metric | Median | Minimum–maximum |
| --- | ---: | ---: |
| Enrollment: key generation, redemption and identity save | 9 ms | 8–10 ms |
| Connect including enrollment | 12.0 ms | 11.3–12.2 ms |
| Full first-run initialization, including enrollment and 50,000 rows | 167.4 ms | 153.8–169.3 ms |

All five invitations were redeemed and all five new identities saved. All fifteen
timed databases (five enrolled, five direct, five with pre-minted credentials)
passed the complete contents check. The repeated baseline medians were 493.4 ms
direct and 158.7 ms with pre-minted credentials. Enrollment runs keep the bridge
active, whereas baseline runs stop it; treat them as separate operating conditions.
No TLS or remote network was involved, so 9 ms is a local service measurement.

## One million rows (2026-11-10)

The same runner, local runtime versions and 192-byte non-null note fixture, scaled
to 1,000,000 rows. Five sequential rounds used 50,000-row batches, new local files
and alternating baseline order. All fifteen timed databases matched Postgres
across all six columns, and all five enrollment invitations were redeemed with
new identities saved. [Raw report](results/local-enrollment-1000000.json).

```sh
/tmp/zb-seed-venv/bin/python scripts/compare/postgres/seed.py \
  --rows 1000000 --rounds 5 --batch-rows 50000 --enrollment --timeout 300 \
  --output /tmp/enrollment-comparison-1000000.json
```

| Path | Median | Minimum–maximum |
| --- | ---: | ---: |
| Direct Postgres SELECT + Python/SQLite import | 14.467 s | 12.411–17.359 s |
| Prepared NATS generation + libzb/SQLite | 3.197 s | 3.111–3.594 s |
| Fresh enrollment + libzb connection and seed | 3.217 s | 3.081–3.379 s |

The prepared-generation baseline was **4.52× faster** than this direct importer
(about 22% of its time). Enrollment itself had a 9 ms median, ranging from 9 to
23 ms. Its internal timer is included in the enrolled path above. The enrolled
path runs with the bridge active; the two baselines run after the bridge stops.

The producer log reported the initial full as 1,000,000 rows in a 29,333,907-byte
object, built/uploaded in 1,589 ms. That preparation was untimed for client
initialization. The fixture remains compressible, both paths run over local
loopback, and Python conversion/insertion versus native import remains part of the
comparison. It is not a measurement of transport overhead alone. Temporary
replicas were retained until the end of the run; the host initially had about
7.6 GiB free disk space. No attempt was made to flush OS/server caches between
samples, and the observed ranges are reported rather than a single best time.

## First local result (2026-11-10)

[Raw report](results/local-50000.json): 50,000 rows, 192-byte non-null notes,
50,000-row batches, five sequential rounds with alternating order. All ten local
databases matched the Postgres checksum across all six columns. The measurement
ran natively on macOS ARM64 over loopback, outside containers or microVMs, against
a temporary PostgreSQL 18.6 cluster with 128 MiB shared buffers. Both import paths
used SQLite 3.54.1; Python was 3.13.18, psycopg 3.3.8, and NATS Server 2.15.2.
Binary and runner hashes are in the report; `git_commit` names the source checkout
before this harness was committed.

| Path | Median | Minimum–maximum |
| --- | ---: | ---: |
| Direct Postgres SELECT + Python/SQLite import | 0.504 s | 0.488–0.530 s |
| Prepared NATS generation + libzb/SQLite | 0.160 s | 0.151–0.164 s |

libzb took about 32% of the direct path's time (3.15× faster) for this fixture and
these implementations. Most direct-path time was in the combined fetch, timestamp
conversion and insertion phase. This does not establish which part caused the
difference; the native importer and prepared compressed representation differ as
well as the transport. It establishes that this prepared NATS path did not add a
startup penalty here. A separate 1,000-row, single-round smoke run with 333-row
batches also verified all contents; it exercised a final partial batch, rather
than serving as a stable performance estimate.

Verification helper checks can run without Postgres, NATS or libzb:

```sh
python3 -m unittest discover -s scripts/compare/postgres -p 'test_*.py' -v
```
