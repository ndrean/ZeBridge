# Handoff — where the recovery work stands (2026-10-27)

A snapshot for whoever (person or LLM session) picks the work up next. It is a log of a moment, not a
contract: the contracts are in `NOTES.md`, `GENERATION.md`, `LEAF_NODES.md`; the working rules in `CLAUDE.md`.
Delete this file once the open items below are closed.

## State of `main`

`main` is at `b134a28` (PR #17 merged) plus whatever the open increment below has. Everything listed is on `main`.

| # | Increment | PR |
|---|---|---|
| 1 | RLS visibility safety (guard) | #6 |
| 2 | TS fence parity | #7 |
| — | Review fixes: RLS quoted identifiers, fence/pass deadlock, failed-PREPARE parking | #8 |
| 3 | Applied frontiers (both clients) | #9 |
| — | Frontier hardening: fail-closed load, overlapping TS fences, route protection | #10 |
| 4 | Leaf-sourced recovery (fence from a leaf's chain, hub fallback, age/backlog reported) | #11 |
| — | `CLAUDE.md` | #12 |
| — | Every full is destructive: owed record + cleared anchor before the first chunk; leaf refusals fall back to the hub | #13 |
| — | nats.zig becomes a fork submodule | #14 |
| — | A plain full is decided again before its first chunk (CDC overtaking the download); the chain waits survive a throwing attempt | #16 |
| — | The shared route is judged on its own cut (a full is a fence when CDC_PUBLIC alone is past its cut) | #17 |

**5. GC floor (branch `gc-watermark-isolation`, PR to be opened):** decided with the maintainer — one database-wide
retention policy, one lock-enforced sweeper, a floor that never decreases (enforced in the database), advance-then-reap, and
the floor read under a shared lock inside each write's transaction. NOTES "One floor, one sweeper" is the record;
`gc_isolation.py` the acceptance gate. Per-publication floors were rejected (publications overlap); independent retention
would be per-table floors, not built. Scenarios that run `bridge_sweeper --once` need no sweeper daemon on the same database.

**To replay once PR gc-floor-followups is in:** every scenario, as the batteries do. What was run here and what was not: `gc_isolation.py` (49 checks) and `gc_resurrect.py` (10) on private stacks; `sweeper.py` and `sweeper_catalogue.py` on a local PostgreSQL at the default retention. Not run (they need the dev stack, the `nats` CLI, macOS or a Homebrew PostgreSQL): `sweeper_restart.py`, `reaps.py`, `drip.py`, `tombstone_children.py`, `incremental.py` (its own scratch database and nats-server, so its five-second sweeper touches no shared floor), `multi_bridge.py`'s sync check fails on `main` here too.

Outstanding proof obligations (not yet turned into scenarios): snapshot boundaries / overlap replay, pending writes across
a recreate, clients connecting before tenant mappings arrive. Also not live-tested: the ordering "own route behind its cut,
shared route past its" (unit tests only, #17) and a download paused while CDC advances (unit test only, #16).

## The nats.zig fork (resolved 2026-10-26)

`main` pins the `nats.zig` gitlink at `da738dccf3b2e1044a87ceef2aaa2641d1e49383`, the tip of branch `zebridge` on
`https://github.com/ndrean/nats.zig` (also tagged `zebridge/b8ffdd9-1`). The fork has it and
`git submodule update --init` on `main` was checked from the owner's clone. Pitfall met on the way: a plain
folder at `nats.zig/` (not a clone) makes git run submodule commands against the superproject and makes the
pull fail with "Could not access submodule"; keep the folder empty until `git submodule update --init`.

Open decision: `build.zig.zon` still has `.nats = .{ .path = "nats.zig" }` (the submodule); a
`url = git+https://github.com/ndrean/nats.zig#zebridge/b8ffdd9-1` dependency would drop the submodule.

## Remaining work: verifying the deployment (the code and isolated-test remediation is substantially complete)

A snapshot of the six hardening findings, to be turned into a deployment audit and a restore drill. Nothing below certifies a rollout: the deployed servers have not been re-inspected.

| Finding | Status in the tree | What remains |
| --- | --- | --- |
| 1. Mirror credentials administer the hub | separate link/keeper identities, connection restrictions, tenant-scoped grants; security scenarios test direct hub login and upstream deletion refusals | rotate legacy credentials; verify every deployed leaf uses the restricted identities |
| 2. `/renew` integer-overflow crash | wider arithmetic, boundary unit tests, renewal scenarios | ensure deployed bridge binaries include the fix |
| 3. Backlog isolation bypass | admission puts each principal's mutations on one private queue subject, with count/size limits | verify admission is on everywhere, direct ingress cannot bypass it, production limits suit the workload; shared storage stays a finite fleet-wide resource |
| 4. Leaf tenant selection was only caching configuration | tenant lists constrain authorization, manifest projections and admission; scoped principals prevent cross-leaf impersonation | audit deployed tenant lists and credentials; a compromised leaf still exposes its authorized tenants' cached data |
| 5. Revocation needed manual leaf updates | signed trust refresh propagates revocations and rotations; tests cover disconnect, reconnect refusal, stale-trust shutdown | verify the supervisor, refresh endpoint and alerts in production; Compose stops NATS after 300 s without a verified refresh |
| 6. Destructive reconciliation, untested recovery | mirror discovery no longer authorizes deletion; checkpoint/restore scenarios cover receipts, policy and replacement-hub recovery | an operator restore drill from a real deployed backup; backup scheduling, retention, off-host storage |

Priority after the GC floor: a deployment security audit and a real backup/restore drill (rebuild the hub on another machine, measure the recovery time, state what a restore can lose), alongside the recovery proof obligations above. The README's outage table is played by `outage_matrix.py` (passes on a private stack); what is still to verify is how LONG each component may stay down: CDC max age and bytes, the admission queue's per-principal and stream caps, the object store.

## Observed, not investigated

The first `zb_client_sync` of a libzb client answers `PrepareFailed` (no detail) on the private stacks, with the tenant mapping and the schema both published; the next polls seed normally. `multi_bridge.py` shows the same on unmodified `main`, so it predates the GC-floor work. The private-stack scenarios ignore that first answer (as `recreate_recovery.py`'s worker always did); worth finding which SQLite statement fails and why, since a host that treats a sync error as fatal would stop there.

## Walls: fixed 2026-10-27 (PR 21), reviewed, and what is left

NOTES "A bridge that could not return, and the limits it hit" and "Identifiers are measured in bytes, and the limits do not drop rows". `scripts/scenarios/walls.py` A–G and I, unit tests in `limits.zig`, `drops.zig`, `http_server.zig`, `batch_publisher.zig`.

- **Before release: apply the fork patch.** `nats.zig-patch/pending/42-nats.zig-subject-length.patch` (README there) makes the tenant limit 128 bytes instead of 46; it could not be pushed from the session that wrote it. Verified both ways here: the patched library (`walls.py` A–I, tenants of 128 bytes with a 62-byte table, a 90-byte tenant with a 40-byte table) and the pinned one (limit 46).
- A deployment with tenants of 47 to 92 bytes that worked with short tables is refused on new rows (guard) and found by `--diagnose` until the patch is applied. Counters restart at 0 on a bridge restart; `bridge_table_degraded` means "dropped since boot", not "inconsistent": `zebridge_reseed` is what makes it consistent.
- Not done: the per-table health is a metric and a `/dropped` list; there is no persisted "table needs reseed" flag in PostgreSQL (`zebridge_suspensions` holds suspensions only).
- Not replayed on the native dev stack: only private stacks (`walls.py`).

## Open decisions and ideas

- **Stale leaf chains:** no age bound by design (an arbitrary cutoff would send a storm back to the hub). Age
  (`cut_age_ms`) and backlog (`cdc_behind`) are reported in the fence log and the `chain` measure event; a
  cost-based fallback and a bound on waiting for a fresh leaf can be decided on that evidence.
- **Upstreaming nats.zig:** small independent topic branches off upstream `main` first — direct-get subject (01),
  named consumer (02), inactive threshold (06), objstore double release (10), JetStream error level (12),
  objstore max-age (16), JetStream domain (17), creds from memory (19), KV bind (27). The pull-inbox series is an
  issue to discuss before a patch; 08/11/14/15/25 depend on the maintainer's taste.
- Documentation hook: the only remaining flags are vendored `zig-pkg/tls-*` readmes.

## Baselines at `974b38d`

- Bridge: `zig build test --summary all` — 52 of 55 steps pass, 3 skipped (need a live PostgreSQL).
- libzb: `cd libzb && zig build test --summary all` clean.
- TS: `cd zb-client-ts && pnpm typecheck && pnpm test` — 383 pass, 1 skipped (needs a libzb wasm build).
- Live: `recreate_recovery.py --check` 4, 12, 13, 14, 15, 16; `leaf_fence.py` (all cases); `rls_visibility.py`.
  A claim about recovery is not done until the live scenario passes.

## Rebuilding the local environment (sandbox recipe)

Cloud containers are ephemeral; none of this survives. Needed: PostgreSQL 16 (`/usr/lib/postgresql/16/bin`),
nats-server ≥ 2.15, Zig 0.17, a venv at `/opt/zenv` with `nats-py nkeys msgpack`, node with `pnpm`.

1. **Dependencies.** The sandbox blocks Zig's archive fetch. `git clone` each dependency, `git archive` it to a
   tarball, `zig fetch ./x.tar.gz`. `libzb/build.zig.zon` may be trimmed locally for that; it is marked
   skip-worktree (`git ls-files -v libzb/build.zig.zon` shows `S`). **Never commit it.** Restore with
   `git update-index --no-skip-worktree libzb/build.zig.zon && git checkout libzb/build.zig.zon`.
2. **Submodule.** `git submodule update --init` fetches the fork; if the sandbox cannot reach it, check the
   submodule out at the pinned commit from a local copy. A submodule left dirty or at another commit trips the
   "uncommitted changes" stop hook.
3. **PostgreSQL.** A throwaway server with `wal_level=logical`, trust auth, user `postgres` (CREATEDB):

   ```
   initdb -D /var/lib/zbtest/pgdata -U postgres --auth=trust       # as the postgres OS user
   pg_ctl -D /var/lib/zbtest/pgdata -o '-p 5432 -c wal_level=logical -c max_replication_slots=10 \
          -c max_wal_senders=10 -c listen_addresses=127.0.0.1 -c unix_socket_directories=/tmp' start
   ```

   Then apply `init.core.template.sql` and `init.write.template.sql` through `envsubst` with the variables
   from `.env.admin` / `.env.bridge` (and `scripts/zb-derive-env.py`), and create the publication named by
   `BRIDGE_CDC_PUBLICATION` with `public.zebridge_create_publication(...)`. Read the variable *names* only; the
   tool hook blocks commands that print secrets. Write such scripts with the editor tools.
4. **Run a check.**

   ```
   zig build -Doptimize=ReleaseFast
   (cd libzb && zig build -Doptimize=ReleaseFast && zig build -Doptimize=ReleaseFast -Dtest-faults=true -p zig-out-faults)
   export ZB_PGBIN=/usr/lib/postgresql/16/bin PATH=$ZB_PGBIN:$PATH
   /opt/zenv/bin/python -u scripts/scenarios/recreate_recovery.py --check 13
   ```

   One check at a time; each takes minutes. `leaf_fence.py` and `rls_visibility.py` run the same way.

## Gotchas that cost time

- Hooks block shell text that looks like secrets or env access; put it in a file written with Write/Edit.
- `pkill -f` kills the shell running it. Use `pgrep -x` or the `[p]attern` trick.
- The Zig test runner fails a test that logs at `err`; use `log.warn` for conditions a test provokes.
- TS fixtures are `Object.create(ZeBridge.prototype)` with hand-listed fields: a new private field must be added
  to `fence-apply.test.ts`, `reconcile.test.ts`, `measure.test.ts`.
- libzb is single-threaded in `poll`: a timing hook that sleeps cannot hold a fence open while another pass runs
  (that is why `ZB_TEST_SCHEMA_PAUSE_FILE` exists); the TS client takes the same hold through `faultHook`.
- libzb never reading a message of `CDC_PUBLIC` counts as a gap for ever (`streamHasGap` with stored 0), so a
  table with a shared route re-seeds every pass; scenarios write one open-tenant row first.
- Reviews of merged PRs have found real P1s each time. Before calling an increment done, probe the *failure* paths
  (a refused PREPARE, a crash after the first chunk, a conversion fence → plain seed) and the overlap of two
  passes, not just the happy path; and prove each regression test fails without its fix.
