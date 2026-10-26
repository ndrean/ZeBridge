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
