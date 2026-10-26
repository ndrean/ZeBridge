# ZeBridge — a guide for an LLM working in this repository

ZeBridge replicates PostgreSQL to local replicas (SQLite, PGlite, DuckDB) through NATS JetStream, with
tenant isolation, offline-first writes and snapshot "generations". This file is the shortest path to being
useful; the long-form design lives in the documents named below. `README.md` is the entry point but is not
fully current — **Zig is 0.17** everywhere, and any mention of 0.16 is stale.

## What is in the tree

| Path | What it is |
|---|---|
| `src/` | The bridge (Zig): logical replication → CDC streams, generation producer (`generation_producer.zig`), RLS guard (`rls_guard.zig`), mutation admission, HTTP/metrics, `--init-sql`, `--init-nats`, `--diagnose`. `grammar.json` names buckets, streams, subjects. |
| `libzb/` | The native client (Zig, C ABI, shared library `libzbcore`): `client.zig` (sync, seed, tail, fence, frontiers), `core.zig` (pure decisions, shared with TypeScript), `transport.zig`, `capi.zig`. Wrappers: `zb-python/`, `zb-dart/`, `zb-android/`, `zb-react-native/`, Swift in `examples/`. |
| `zb-client-ts/` | The TypeScript client (`src/libzb.ts` is the whole shell; `src/fence.ts` the pure fence decision; `src/core.ts` wraps `wasm/zb_core.wasm`, built from libzb's `core.zig`). |
| `scripts/scenarios/` | The functional tests that matter. Python scenarios that start real Postgres databases, nats-servers and bridges. `README.md` there, `run.py` sequences groups. |
| `nats.zig/` | Git submodule: the fork `ndrean/nats.zig`, branch `zebridge` (upstream plus our patches, one commit each; ledger `NATS_ZIG_NOTES.md` in the fork). Nothing is applied at build time; `nats.zig-patch/` is a retired README. |
| `HANDOFF.md` | A dated snapshot of open work and blockers; read it first if it exists, delete it when its items close. |
| `NOTES.md` | The design log, numbered (`§10xx`); code comments cite it. `PROTOCOL.md`, `GENERATION.md`, `SCOPE.md`, `OPERATIONS.md`, `LEAF_NODES.md`, `SECURITY.md`, `AUTHENTICATION.md` carry the contracts. |

The two clients are twins: **a behaviour change belongs in both** (libzb `client.zig` and `zb-client-ts/src/libzb.ts`),
with the same words in the log lines, because the scenarios grep them.

## Build and test

```
zig build                                   # the bridge (add -Doptimize=ReleaseFast for the scenarios)
zig build test --summary all                # bridge unit tests (a few need a live PostgreSQL / are skipped)
cd libzb && zig build test --summary all    # libzb unit tests
cd libzb && zig build -Doptimize=ReleaseFast                       # libzb/zig-out/lib/libzbcore.so
cd libzb && zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults   # fault-hook build (see below)
cd libzb && zig build wasm-core             # core.zig → zig-out/wasm (pnpm wasm copies it into zb-client-ts)
cd zb-client-ts && pnpm typecheck && pnpm test    # node:test with node:sqlite; one test skips without a libzb wasm build
```

- The Zig test runner **fails a test that logs at `err`**: use `log.warn` for conditions a test provokes.
- A sandbox may block Zig's HTTP/archive fetch. Fetch dependencies with `git clone`, `git archive` to a tarball and
  `zig fetch ./x.tar.gz`; the hashes match. `libzb/build.zig.zon` may be locally trimmed in such a sandbox — check
  `git ls-files -v libzb/build.zig.zon` (an `S` means skip-worktree) before committing anything under `libzb/`.
- TypeScript tests build clients with `Object.create(ZeBridge.prototype)` and hand-listed fields. **A new private field
  used by a code path must be added to those fixtures** (`fence-apply.test.ts`, `reconcile.test.ts`, `measure.test.ts`),
  and the fixture's SQLite needs any new table.

## Scenarios (the real tests)

Most scenarios need `psql`/a PostgreSQL superuser at `127.0.0.1:5432` (user `postgres`, CREATEDB), `nats-server`, a built
bridge (`zig-out/bin/bridge`), libzb, and a Python venv with `nats-py`, `nkeys`, `msgpack`. `ZB_PGBIN` points at the
PostgreSQL binaries. Each scenario builds a **private stack** (own database and replication slot, own nats-server, own
bridge) and drops it.

- `recreate_recovery.py --check N` — the recovery model, one stack per check. 1–3 recreate/gap, 4 the fence's crash and
  retention boundaries (libzb fault build), 12 the same with a zb-client-ts client, 13/14 two tables on one route
  (applied frontiers; libzb / TS), 15/16 a plain full that crashes after its first chunk (libzb / TS), 5–11 incarnation,
  epoch and cleanup barriers. Takes minutes; run one check at a time.
- `rls_visibility.py` — the RLS guard (a policy that hides rows blocks publication, visible in metrics and `--diagnose`).
- `leaf_fence.py` — a fence taken from a seeding leaf's chain (hub + leaf nats-servers, no auth).
- `gc_isolation.py` — the GC floor: competing sweepers, lock-owner failure, rollback and retention increase, a delayed write racing with reaping (private stack).
- `scripts/scenarios/run.py {offline|live|owns|all}` sequences the groups; `--list` shows them.

**Fault-hook builds.** Crash and timing points are compiled in only with `-Dtest-faults=true` (libzb) and exist as env
variables there (`ZB_TEST_FENCE_ABORT`, `ZB_TEST_FENCE_SLEEP_MS`, `ZB_TEST_SHARED_UNREADABLE`,
`ZB_TEST_SCHEMA_PAUSE_FILE`). zb-client-ts reads **no** environment for them: they are the `faultHook` config option, mapped
from the same variables by `scripts/scenarios/fence_client.mts`. A scenario fails if an ordinary build carries a hook.

## How recovery works (the part most changes touch)

- A client follows per-tenant CDC streams (`CDC_<tenant>`) and the shared `CDC_PUBLIC`. A **position** per stream is the
  highest sequence applied, gated or held; it certifies everything below it. Snapshots are **generations** per
  (table, tenant): a manifest (KV `generations`) names a full and deltas (object store `gen-<tenant>`), a **cut**
  (`cutoff_seq`) on the CDC stream, the stream's `created` identity, and a seed epoch. An **anchor** (`_zbz_generations` /
  `_zebridge_generations`) records the cut a table was seeded at and gates older events.
- **D2** (`fullPredatesReplica`): a full cut behind the position would destroy rows CDC never re-delivers. Instead of
  refusing, the **fence** takes it: PREPARE (one transaction: anchor cleared, cursors moved *back* to the cut — never
  forward —, an owed record), replace the rows, VERIFY (fresh stream facts: same incarnation, event after the cut still
  retained), COMMIT (anchor + owed removal). An interrupted fence stays owed; its retry is a fence too.
- **Every full is destructive**, fence or plain seed: once the object is in hand and before its first chunk, the client
  writes an owed record and clears the anchor (`oweReplacement`), so a crash mid-replace is retried as a fence, never
  trusted as a seeded table.
- **Applied frontiers** (`_zbz_frontier` / `_zebridge_frontier`): other seeded tables on a rewound route keep their rows and
  skip replay up to the position they held; spent when the position passes, dropped on stream restart/recreate, replay from
  0, table drop, or when a chain is applied to that table. Loading them **fails closed**.
- **Route protection (TS):** `fenced` (route → owner tables) parks a route's tails; `replacing` records every chain
  replacement; a fence that commits hands the parking to replacements still running. Fence decisions are taken again inside
  `prepareExclusive` — never trust positions read before it. libzb is single-threaded in `poll`, so this cannot overlap there.
- **Leaves:** a seeding leaf mirrors manifests and objects; it never mints cuts, incarnations or epochs. Clients prove
  everything against the **hub's** stream and may fence from the leaf's chain; a leaf with a missing object sends the client
  to the hub before anything changes. There is no age bound on a leaf chain by design; its age and CDC backlog are
  reported (`cut_age_ms`, `cdc_behind`).
- **GC floor:** `zebridge_gc_watermark` is one row per database and never goes down (a trigger refuses it). One sweeper
  per database, enforced by an advisory lock; each pass advances the floor, then reaps before the cutoff it returned; the
  bridge reads the floor under a shared lock inside each write's transaction. Multi-bridge shares one GC policy
  (NOTES "One floor, one sweeper"; `gc_isolation.py`). Scenarios stage a floor by disabling the trigger by name.
- **RLS guard:** the producer reads as the reader role, so a policy that hides rows would publish an empty full. The guard
  classifies the reader's policies from the catalog; `normalize` keeps quoted identifiers distinct from keywords.

## Conventions

- Branch per increment from the latest `main`; commit and push to the branch; **do not open pull requests** unless asked —
  the owner opens them from the UI. Never push to `main`. Do not put a model identifier in commits, PR text or code comments.
- Comments here explain *why* and cite `NOTES.md` sections; match the density and idiom of the surrounding file. Each
  behaviour fix gets a regression test that **fails without the fix** (prove it: restore the old code, or mutate the fix).
- A claim about recovery is only done when a scenario passes live, not only the unit tests.
- Documentation follows one-fact-one-file: put a fact in the document that owns it and link from elsewhere.
- Use `git ls-remote origin <branch>` to confirm a push; a clone that fetches only `main` shows no upstream for other
  branches — add `remote.origin.fetch` for the branch and `git branch -u origin/<branch>`.

## Pitfalls

- `pkill -f <pattern>` matches the shell running it; use `pgrep -x` or the `[p]attern` trick.
- A tool hook may block shell commands whose text mentions secrets (environment, credentials, `.env`). Put such text in files
  written with the editor tools, and never print secret values. Reading variable *names* is fine.
- nats-server must be recent (the generated configs use `default_max_consumers`; v2.15 works, 2.11 does not).
- PostgreSQL 16: `ALTER POLICY` waits for a running snapshot that holds the table; do not run it from the same session as a
  parked build.
- `CDC_PUBLIC` that a client has never read a message of counts as gapped for ever in libzb (`streamHasGap` with stored 0):
  every pass re-seeds each table with a shared route. Scenarios that need a quiet route write one open-tenant row first.

## Troubleshooting: when a fixed limit is the cause

Start with `bridge --diagnose` (load the same environment files as the bridge; `--pub` and `--slot` have no default). It
changes nothing, exits 0 (`all clear`) or 1 (a count of findings), and covers configuration, catalogue, slots, NATS and what a
fresh client would find ([OPERATIONS](OPERATIONS.md#diagnose)). The symptom table in
[OPERATIONS](OPERATIONS.md#troubleshooting) and the suspension reasons in [OPERATIONS](OPERATIONS.md#suspended-tables) own the
problems the bridge names itself. This section is for the other kind: a **compiled-in limit** that the bridge does not name,
so the symptom points somewhere else.

Where to look, in order: the bridge's log (stdout and stderr; `error(` and `🔴` lines), `/metrics` on `--port`
(`bridge_refused_tables`, `bridge_refused_events_dropped_total`, `bridge_cdc_window_short`, `bridge_connected`),
`SELECT * FROM zebridge_suspensions`, `SELECT * FROM zebridge_check_all()`, `nats stream ls` / `nats kv ls`. In a scenario's
private stack the bridge log is `/tmp/zb_rr<tag>_<label>.log`.

### A bridge that stops without saying why

Zig prints `error: <Name>` and a return trace **only when `main` returns**, and `main` cannot return while a `defer` join waits
for a thread that is still running. Measured on the scenarios' private stack (2026-10-26), an error that escapes `main` leaves
the process **alive with replication dead**: the walsender still `streaming`, the slot still `active` (its `confirmed_flush`
behind the walsender's `sent` position), the last log line an ordinary `info`. Two shapes:

- the error escapes during boot, before the mutation listener starts: the process waits in the HTTP accept loop. **One request
  (`curl -s localhost:<port>/health`) releases it**; it exits 1 and the real `error:` line is in the log;
- the error escapes in the replication loop, after the listener started: `main` waits in `MutationListener.join`, and neither an
  HTTP request nor SIGTERM releases it (80 s and a SIGTERM measured). Only SIGKILL ends it, and the error is never printed. The
  `errdefer` that sets `should_stop` (`bridge.zig`, after `wal_mon.start`) runs *after* the later `defer` joins in unwinding
  order, so the join it should have released has already begun. This is the source's reading of the frame list from `gdb -p`
  (`bridge.main` → `MutationListener.join`); the error value itself was not captured.

So: **a bridge that is up but silent** (no new `info` line, `bridge_wal_confirmed_lag_bytes` rising or the CDC stream not
growing) is dead until proven otherwise. Check the walsender position against `pg_current_wal_lsn()`, take
`gdb -p <pid> -batch -ex 'thread apply all bt 12'`, send one HTTP request, then kill it and read the log. Two causes found this
way are in the table below (table name, tenant id). They are defects of the walls and of the unwind order, not of the
operator; `HANDOFF.md` lists the fix.

### Walls

"Measured" means reproduced on a private stack in this tree; "source" means read from the code, not provoked.

| Where it hurts | You see | The wall | Confirm | Way out |
|---|---|---|---|---|
| A table name of about 40+ characters (PostgreSQL allows 63) | Bridge boots, publishes the first boot schemas, stops; the log ends on `Boot schema published to KV for …` (the first table that fits); `error: MsgIdTooLong` appears only once the process is released (see above). Measured: 20, 30 and 35 characters boot, 40 and 50 do not | The slot-resident message id is `[64]u8` (`batch_publisher.zig`, `msg_id_buf`); the boot schema id is `schema-boot-<table>-<lsn in decimal>-e<epoch>`, so the limit is `64 - 25 - (digits of the LSN - 9)`: **it falls by one for each extra digit as the WAL position grows**, so a table that booted last year can stop the bridge at a later restart (derived from the source, consistent with the measure). Event ids `<lsn in hex>-<table>-<op>` allow up to 40 | `error: MsgIdTooLong` at `bridge.zig` `publishBootSchemas`; length of the longest published table | Rename the table. Not recoverable by configuration |
| A tenant id of 93+ characters (with an 8-character table) | Rows are published (CDC stream has them), but **no generation is ever built** for that tenant: `generation build failed for '<tenant>'/'<table>': error.InvalidSubject — next cadence retries`, every cadence; no manifest, so clients never seed. Measured with an 8-character table: 92 builds, 93 does not | A subject over NATS's 255-character limit (`nats.zig/src/validation.zig`); the tenant is part of it more than once (not traced to the exact subject). The limit therefore also shrinks with the table name | the `InvalidSubject` line; `nats kv get generations <key>` has no entry | Shorter tenant ids. Keep them under ~64 |
| `len(tenant) + len(table) > 115` (a 108-character tenant id with an 8-character table; less with an `.<suffix>`) | The row is **lost**, the bridge is dead and does not say so: `CDC_<tenant>` stays empty, no error line, process alive, SIGTERM ignored (previous section). Measured: tenant 107 publishes, 108 does not, with table `rr_items`. The generation failure above is also present | The CDC subject `cdc.<tenant>.<table>.<op>[.<suffix>]` is built in a `[128]u8` (`Config.Buffers.subject_buffer_size`) with a sentinel byte, so `4 + T + 1 + L + 1 + 6 + 1 ≤ 128`; `std.fmt.bufPrintSentinel` returns `NoSpaceLeft` and the insert prong does `else => return err` (from the source; the error value was not captured live) | Walsender `sent` ahead of the slot's `confirmed_flush`, CDC stream not growing, `gdb` shows `MutationListener.join` under `bridge.main`. Compute the subject length yourself | Shorter tenant ids. `isSubjectToken` (`utils.zig`, `max_subject_token_len = 128`) admits ids this buffer cannot hold |
| A tenant id with `.`, `*`, `>`, whitespace, empty, or over 128 characters | `🔴 Quarantining insert on '<table>' to '.unrouted': the catalogue names tenant column '…' but this event carries no value for it. Check that the replica identity still covers it.` The row is **dropped** (counted, never published) | `utils.isSubjectToken`. The log text blames the replica identity; the value is present and merely unusable as a subject token | The quarantine line for each such row; the tenant value in PostgreSQL | Fix the data. The message misleads: check the value before the replica identity |
| A row wider than the event buffer | One table is suspended (`row_too_large`); its events are dropped and counted; others flow | `BASE_BUF` (default 12 → 4 KB per event slot, ceiling `BASE_BUF_MAX`); also bounded by the server's `max_payload` minus `payload_envelope_margin_bytes` (16 KiB) | `zebridge_suspensions`; the boot sizing lines; `--diagnose` | [Sizing the ring](OPERATIONS.md#sizing-the-ring); `BASE_BUF` up |
| A migration adds columns | `too_many_columns` suspension on that table | `MAX_COLUMNS`: auto-detected at boot as the widest table doubled and rounded to a multiple of 8, floor/ceiling in `Config.Batch` (`default_max_columns` 128, `absolute_max_columns` 1600) | the boot line `MAX_COLUMNS=… (auto-detected …)` | Restart re-detects; or set `MAX_COLUMNS` |
| More than 256 writable tables, or more than 64 refused tables in one bridge | `WritableRegistryFull` / `RefusalRegistryFull`; later tables are not registered | `writable_tables.max_tables = 256`, `refused_tables.max_refused = 64`; fixed arrays, a concurrent reader scans them | source | Split the publication across bridges, or fix the refusals (64 refused tables is already a misconfiguration) |
| A CDC batch cannot be published | The bridge **stops** (`Exhausted retries`), by design, rather than acknowledge an LSN it did not deliver | `Config.Retry.publish_max_retries = 5` | the log; NATS up? JetStream storage full? (`StorageResourcesExceeded`) | Fix NATS and restart: the slot replays. Do not raise the budget to hide an outage |
| A new tenant or stream is refused | `MaximumConsumersLimit`, `StorageResourcesExceeded`, or no stream for a tenant | nats-server `default_max_consumers` (needs a recent server) and per-stream byte reservations (`CDC_MAX_BYTES`, readings streams) against the account's JetStream limits | `nats account info`, the `--init-nats` output | Raise the account limits or lower the per-stream valves |
| Enrollment is refused or slow | `503 busy — retry shortly` from `/enroll`, or a connection closed after the 3 s receive timeout | HTTP server: 16 connections (`max_connections`), 3 s receive timeout, 4 concurrent enrolls (each dials a PostgreSQL writer connection), invite code 16–128 characters, JWT lifetime 1 day | source (`Config.Http`) | Retry; mint codes of valid length; renew before the day ends |
| Writes stop with `too many connections` or the writer role is exhausted | Mutation lanes and enrollments fail to connect | Writer role `CONNECTION LIMIT` 20, needs ingress lanes (≤ 8, `max_ingress_lanes`) + sweeper + enrollments; the preflight line `connection budget: … needs N` says it | the preflight line; `--diagnose` | Fewer lanes, or raise the role's limit |
| A write that was accepted never gets a verdict after a long PostgreSQL outage | Dead-lettered on `mutation_error.>` | `mutation_max_deliver = 15` redeliveries | `nats consumer info MUTATIONS <principal>` | The client resends; this is the designed end of a retry |
| A write is refused after a long offline period | `PredatesGcWatermark` | The database's GC floor, which never goes down ([PROTOCOL](PROTOCOL.md#the-floor-one-per-database-and-it-never-goes-down)) | `SELECT * FROM zebridge_gc_watermark` | Intended; the client must resync. Do not lower the floor to make a write pass |
| `GcFloorUnavailable` on every write | Mutation lanes retry forever | The write path takes the floor under a shared lock; the init SQL was not re-applied after an upgrade (`zebridge_gc_floor_acquire()` missing) | `--diagnose`; `\df zebridge_gc_*` | Re-apply `--init-sql` as the administrator |
| A client back from a pause re-seeds, or says `chain predates the stream` | | The CDC window (default 3 × cadence) against the chain depth and the GC retention (7 d); `CDC_MAX_AGE_SECONDS`, `CDC_MAX_BYTES`, `CDC_MAX_MSGS` | `bridge_cdc_window_short`, the boot line `CDC window: streams keep …` | [OPERATIONS Troubleshooting](OPERATIONS.md#troubleshooting) |
| The TypeScript client drops events and logs `events held while waiting for a chain that never came` | | `UNSEEDED_HOLD_MAX = 50_000` (`libzb.ts`); after it, further events for the unseeded table are dropped | the client log | Fix why no chain exists (see the 93-character tenant row), then reconnect |
| A client waits 90 s at connect for a table | `no generation chain yet — waiting up to 90s` | `GENERATION_WAIT_MS` (`libzb.ts`), same in libzb | the client log | A followed table without a chain: enable it, or remove the stale `schemas` key. A tenant id of 93+ characters never gets one |

### Rules for adding to this list

- Reproduce it first (private stack, `scripts/scenarios/recreate_recovery.Stack`; `bridge_up` then a row), and write "measured"
  or "source" honestly. An unconfirmed cause stays out of the table.
- A wall whose symptom names something else (the quarantine text that blames the replica identity, a silent stop, a
  generation failure for a tenant whose CDC works) is the reason this section exists: record the **symptom first**.
- If the fix is a validation at the door (enable time, enrollment, preflight), say so in `HANDOFF.md` rather than documenting the
  limit as permanent.
