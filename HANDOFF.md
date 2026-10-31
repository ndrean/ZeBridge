# Handoff — where the work stands (2026-10-30)

A snapshot for whoever (person or LLM session) picks the work up next. It is a log of a moment, not a contract: the contracts are in `NOTES.md`, `GENERATION.md`, `LEAF_NODES.md`; the working rules in `CLAUDE.md`. Delete this file once the open items below are closed.

## State of `main`

`main` is at `bed95b7` (PR #43 merged, after #42 and #41; #41 carried #39 and #40, which had been merged into their stacked base branches, after #38). Everything in the table is on `main`.

The production hub runs 0.2.8 since 2026-10-31 about 08:30: the init SQL applied first, every chain rebuilt at generation 1 (so clients re-seed), the doctor all clear, no table suspended. Its public NATS address is `tls://nats.zebridge.eu:4222` and its bridge credentials are `/etc/zebridge/creds/bridge.creds` on the hub; `/health` is not exposed through HAProxy, `/status` is. The lost `my_slot` and the dropped `storm_items` rules were removed from the London database by the owner. What is deployed there, recorded separately because a version number does not say it: **package** `zebridge 0.2.8`, built by `release.yml` from tag `v0.2.8` = commit `23b54e7`; **database** init SQL rendered by that same bridge (from the package, on the hub) and applied 2026-10-31, so `v0.2.8`'s templates. Not deployed, merged after the tag: the init SQL of #38 (the internal-table predicate) and #40 (`zebridge_enable` and `zebridge_grant_edge_writes` refuse the `zebridge_` prefix), and the binary changes of #38, #39 (the namespace rule in `isForbiddenTable`) and #45 (the doctor's NATS message). A later upgrade needs both halves again: the package, and the SQL rendered by it. A 0.2.9 is the owner's call.

| #   | Increment                                                                                                                      | PR  |
| --- | ------------------------------------------------------------------------------------------------------------------------------ | --- |
| 1   | RLS visibility safety (guard)                                                                                                  | #6  |
| 2   | TS fence parity                                                                                                                | #7  |
| —   | Review fixes: RLS quoted identifiers, fence/pass deadlock, failed-PREPARE parking                                              | #8  |
| 3   | Applied frontiers (both clients)                                                                                               | #9  |
| —   | Frontier hardening: fail-closed load, overlapping TS fences, route protection                                                  | #10 |
| 4   | Leaf-sourced recovery (fence from a leaf's chain, hub fallback, age/backlog reported)                                          | #11 |
| —   | `CLAUDE.md`                                                                                                                    | #12 |
| —   | Every full is destructive: owed record + cleared anchor before the first chunk; leaf refusals fall back to the hub             | #13 |
| —   | nats.zig becomes a fork submodule                                                                                              | #14 |
| —   | A plain full is decided again before its first chunk (CDC overtaking the download); the chain waits survive a throwing attempt | #16 |
| —   | The shared route is judged on its own cut (a full is a fence when CDC_PUBLIC alone is past its cut)                            | #17 |
| 5   | GC floor: one database-wide policy, one lock-enforced sweeper, a floor that never decreases, advance-then-reap                 | #18–#20 |
| —   | Walls: identifiers in bytes, no supported pair loses CDC, drops counted, the silent-stop fixes                                 | #21, #23, #24 |
| —   | The archiver's native reader (`bridge_readings_fetch`)                                                                         | #22 |
| —   | Scenario rerun fixes (the first-sync `PrepareFailed`)                                                                          | #25 |
| —   | The scenario stack in containers (`scripts/dev`: PostgreSQL, NATS in operator mode, SeaweedFS)                                 | #26 |
| —   | A stream that has never held a message is not a gap (`born_empty`, both clients)                                               | #27 |
| —   | `--diagnose`: a dropped tenant-scoped table is not a warning                                                                   | #28 |
| —   | Admission-mode dev stacks, repeatable fixtures; `scripts/linux` (the stack without Docker); READMEs rewritten with install checklists | #29 |
| —   | The joint fence (both clients, durable across a restart) and the admission refusal counters                                    | #30 |
| —   | Scenario harness: never two client processes on one replica (the third-table mismatch was this, not the recovery code)          | #32 |
| —   | One synchronization owner per file-backed replica (libzb and zb-client-ts, `ReplicaAlreadyInUse`)                              | #34 |
| —   | The generation producer forgets the cuts of tables that are no longer derived (a dropped table no longer fails a build a minute) | #35 |
| —   | `shared_cut` picks a tenant with a live row                                                                                    | #36 |
| —   | Release 0.2.8 (every package at one version; published, smoke green)                                                          | #37 |
| —   | The internal tables an upgrade creates (`zebridge_leaf_scopes` and two more) are internal; a test that a client cannot move the GC floor | #38 |
| —   | `isForbiddenTable()` refuses the whole `zebridge_` namespace, not six names (case ignored)                                     | #39, via #41 |
| —   | `zebridge_enable()` and `zebridge_grant_edge_writes()` refuse the `zebridge_` prefix, like `lf-` for principals               | #40, via #41 |

The decisions behind the GC floor: one database-wide retention policy, one lock-enforced sweeper, a floor that never decreases (enforced in the database), advance-then-reap, the floor read under a shared lock inside each write's transaction. NOTES "One floor, one sweeper" is the record and `gc_isolation.py` the acceptance gate. Per-publication floors were rejected (publications overlap); independent retention would be per-table floors, not built. Scenarios that run `bridge_sweeper --once` need no sweeper daemon on the same database.

## Where the scenario batteries stand

The owner runs `run.py offline|live|owns` on two stacks, each also in mutation-admission mode (`bridge --mutation-ingress` plus `MUTATION_ADMISSION=1`): the Docker Compose stack (`scripts/dev`, macOS) and the plain-process stack (`scripts/linux`, Ubuntu with PostgreSQL 16). Compose, 2026-10-30, every binary rebuilt, on the replica-lock branch (#34, since merged): `live` 62 of 66 on the first run and 66 of 66 after the explained reruns (`check`: an orphan slot left by an interrupted earlier run; `standby` and `multi_bridge`: `MUTATION_ADMISSION=1` was exported, and they run private NATS stacks with no admission process; `shared_cut`: its tenant picker, fixed in #36), and `owns` 47 of 47. Linux: the last numbers reported to me, 53 of 66 before the fixes of #29 and #30 were pulled there, are stale.

Known to need more than the default build or install, by design, and documented in the two READMEs: `receipt_faults`, `recreate_recovery` 4/13/15/19/20 and `walls` E (fault-hook builds), `hub_fallback` (`zig build endpoint-probe`), `leaf_security` (the leaf image, Docker), `archive`, `readings_roles` and the S3 case of `readings_fetch` (SeaweedFS, Compose only), `serve` (Python 3.14 or newer).

Reported green on the Docker Compose stack on 2026-10-30, with every binary rebuilt: `archive.py`, the sweeper scenarios and `shared_record.py`. `drip.py` ran its whole 70 minutes clean (20,246 ticks; the stores agreed every minute, outboxes empty, no refused verdict, the bridge's allocation flat; the hourly sweeper reaped 0 because the GC floor is a week behind). On the Canada test hub, upgraded to 0.2.8 and driven from a Mac across the Atlantic (about 100 ms), the storm: 100 of 100 clients synced cold (median 13.2 s, slowest 14.7 s), warm in a median 1.7 s, and after a NATS restart in the marker window every client came back, longest gap 2.9 s, none lost. `multi_bridge.py`'s sync check failed on `main` in the sandbox that built the GC floor and has not been reported since.

## Outstanding proof obligations

Not yet turned into scenarios: snapshot boundaries and overlap replay, pending writes across a recreate, clients connecting before tenant mappings arrive. Not live-tested: the ordering "own route behind its cut, shared route past its" (unit tests only, #17), a download paused while CDC advances (unit test only, #16), and the libzb lapse path of the joint fence (a participant owed again by the last one's commit: its restore and parking are unit-tested, but a live run needs two chains with different cuts and a purge between them; in the live restart case of checks 19 and 20 both chains shared a cut).

## The nats.zig fork (resolved 2026-10-26)

`main` pins the `nats.zig` gitlink at the tip of branch `zebridge` of `https://github.com/ndrean/nats.zig` (tag `zebridge/b8ffdd9-2`, patch `zebridge 42`), so the tenant limit is 128 bytes (`bridge --limits`). `git submodule update --init` on `main` was checked from the owner's clone. A plain folder at `nats.zig/` (not a clone) makes git run submodule commands against the superproject and the pull fail with "Could not access submodule"; keep the folder empty until `git submodule update --init`. An empty submodule is also what makes the build stop with "unable to find nats".

Open decision: `build.zig.zon` still has `.nats = .{ .path = "nats.zig" }` (the submodule); a `url = git+https://github.com/ndrean/nats.zig#zebridge/b8ffdd9-2` dependency would drop the submodule.

## Remaining work: verifying the deployment

A snapshot of the six hardening findings, to be turned into a deployment audit and a restore drill. Nothing below certifies a rollout: the deployed servers have not been re-inspected.

| Finding                                                 | Status in the tree                                                                                                                                      | What remains                                                                                                                                               |
| ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1. Mirror credentials administer the hub                | separate link/keeper identities, connection restrictions, tenant-scoped grants; security scenarios test direct hub login and upstream deletion refusals | rotate legacy credentials on deployed leaves and verify the old ones are refused                                                                           |
| 2. `/renew` integer-overflow crash                      | wider arithmetic, boundary unit tests, renewal scenarios                                                                                                | ensure deployed bridge binaries include the fix                                                                                                            |
| 3. Backlog isolation bypass                             | admission puts each principal's mutations on one private queue subject, with count/size limits                                                          | verify admission is deployed and the legacy queue drained (`deploy/migrate-mutations.py` migrates legacy to admission only)                                |
| 4. Leaf tenant selection was only caching configuration | tenant lists constrain authorization, manifest projections and admission; scoped principals prevent cross-leaf impersonation                            | audit deployed tenant lists and credentials; a compromised leaf still exposes its authorized tenants' cached data                                          |
| 5. Revocation needed manual leaf updates                | signed trust refresh propagates revocations and rotations; tests cover disconnect, reconnect refusal, stale-trust shutdown                              | verify the supervisor, refresh endpoint and alerts in production; Compose stops NATS after 300 s without a verified refresh                                |
| 6. Destructive reconciliation, untested recovery        | mirror discovery no longer authorizes deletion; checkpoint/restore scenarios cover receipts, policy and replacement-hub recovery                        | an operator restore drill from a real deployed backup; backup scheduling, retention, off-host storage                                                      |

Priority: a deployment security audit and a real backup/restore drill (rebuild the hub on another machine, measure the recovery time, state what a restore can lose), alongside the proof obligations above. The README's outage table is played by `outage_matrix.py` (passes on a private stack); what is still to verify is how LONG each component may stay down: CDC max age and bytes, the admission queue's per-principal and stream caps, the object store.

## The archiver's native reader (merged, #22)

`bridge_readings_fetch` is the archiver's only reader: there is no `--reader` flag and no Python reader mode (`examples/16-archive/README.md`; `test_archive_readers.py` asserts the flag is absent), so there is no default to switch. `archiver.py` finds the executable with `--fetch-bin`, `ZB_READINGS_FETCH` or the `PATH` (NOTES "The archiver's reader is a binary of the bridge's build"). The fetcher is built with the bridge (`zig build`), and `deploy/build-linux.sh` and `deploy/package-linux.sh` carry it into the release package. Verified in the sandbox: unit tests, `readings_fetch.py` (A-I, with SeaweedFS), `readings_roles.py` P3 (real operator-mode grants), 500,000 readings end to end 26.8 s to 3.0 s; green on the Docker Compose stack on 2026-10-30 (`archive.py`). The container image (`deploy/archiver.Dockerfile`) extracts the fetcher from the release `.deb`. Not verified by me: that image built from a real `.deb` and run against a hub, and a run of `build-linux.sh` and `package-linux.sh` end to end (both were edited, not run, in the sandbox). If you have run them, strike this line.

## Walls (merged, #21, #23, #24)

NOTES "A bridge that could not return, and the limits it hit" and "Identifiers are measured in bytes, and the limits do not drop rows". `scripts/scenarios/walls.py` A–G and I, unit tests in `limits.zig`, `drops.zig`, `http_server.zig`, `batch_publisher.zig`.

- A deployment built against upstream nats.zig (no `max_subject_len`) gets the 46-byte limit: tenants of 47 to 92 bytes that worked with short tables are refused on new rows and found by `--diagnose`. Counters restart at 0 on a bridge restart; `bridge_table_degraded` means "dropped since boot", not "inconsistent": `zebridge_reseed` is what makes it consistent.
- Not done: the per-table health is a metric and a `/dropped` list; there is no persisted "table needs reseed" flag in PostgreSQL (`zebridge_suspensions` holds suspensions only).
- Not replayed on a dev stack: only private stacks (`walls.py`).

## Observed, not investigated

- **A slow admission verdict** (judged an outlier by the owner on 2026-10-30; no action for now, which the owner is not happy with, so keep the log timestamps idea below ready). One verdict took 3.7 s: the first write after a fresh start of the Linux stack, after `reprovision.py`. It did not repeat in the ten or so further starts I tried (first writes 25 to 300 ms, later ones 20 to 50 ms after any quiet spell, a DDL change right before the write included). The admission loop sleeps at most 20 ms and the bridge's mutation lane at most about 0.6 s, so neither explains it. `scripts/scenarios/invalidate.py` waits for the verdict instead of sleeping a fixed time. If it shows up again, log timestamps in the lane (receipt, apply, verdict publish) before guessing.
- **`shared_record.py`.** Failed once on 2026-10-28 (steps 4 to 10 on the Compose stack) and was never explained; green on 2026-10-30 after every binary was rebuilt, and again twice that day (alone and in `owns`). A stale binary or the empty-`CDC_PUBLIC` gap (fixed by #27) are the suspects, neither shown. If it fails again, keep the log before rebuilding anything.
- **The drip emitter's growth was the harness, not libzb** (2026-10-31). drip's "emitter" RSS is the Python process (`os.getpid()`), and `clients.Lib.poll` kept every write's outcome in a list rebuilt on each poll. A/B with the drip write loop at 20 ms on the dev stack: kept, 40 → 182 MB in 10 minutes (65 MB of Python heap for 67,291 outcomes, the rest allocator churn from the copies); cleared each tick, flat at 44 MB for 23,740 ticks. Fixed: `Lib` keeps outcomes in a dict by msg id, updated in place, and `drip.py` empties it each tick. Seen on the way, not yet checked: the dev nats-server grew to 270 MB with 139,025 VERDICTS messages, one subject each (kept 2 h, file storage, so the RAM is the per-subject index and block cache); it should fall back as they age out. `collist.py` fails three checks (descriptor, audit: `no manifest for globex.col_t`) on the current dev stack with the old and the new `clients.py` alike; it passed in the 2026-10-30 battery.
- **Connect time over a long link.** On the Canada hub (about 100 ms round trip) a libzb client enrolled in about 1.0 s and connected in about 1.3 to 1.9 s, roughly 13 sequential requests. The enrollment is mostly the bridge's fresh database connection per call (757 ms from the VPS to its Supabase database across the Atlantic, measured); the connect scales with the round trip. Fewer sequential requests in the connect path is the lever; not looked at.
- **A refused client write may get no verdict.** In the floor probe (`zebridge_gc_watermark`, refused as a forbidden table) the libzb client saw no outcome for the write within 12 s. Whether it is told at all, or its outbox keeps it pending, was not checked.
- **A table an older bridge already refused.** A hub upgraded with a bridge that predates #38 (0.2.8 is one) keeps `zebridge_leaf_scopes` in `zebridge_suspensions` and logs `REFUSED … (no_cdc_subject)` again and again (about 40 warnings in 10 minutes on the Canada hub). After moving to a bridge with the fix, check `zebridge_suspensions` for it; the expectation that it clears is not verified.
- **DuckDB.** The DuckDB header is vendored (1.5.x); DuckDB 2.0 LTS is out and its API compatibility with the vendored header and the `libzb` DuckDB path has not been examined. Building with and without `-Dvendor=true` compiles; nothing was run against a 2.0 library.

## Open decisions and ideas

- **Releases.** 0.2.8 is published (smoke green, run by the owner). `main` has since gained #38 to #41 (and #42 once merged): a 0.2.9 is the owner's call. The Canada test hub runs 0.2.8; **the production hub has not been upgraded**: follow OPERATIONS "Upgrading a hub and a client library to a release" (the init SQL first, then the packages), and rehearse on the scratch hub first. The London database's smoke artifacts (`smoke_items`, its chain rows, four unused invites) were removed on 2026-10-30.
- **The replica lock covers file-backed replicas only.** PostgreSQL storage, the browser and PGlite have no owner check (NOTES "One synchronization owner per replica").
- **The database's own refusal of client writes to bridge tables is not built.** Today the only thing standing between a client and `zebridge_gc_watermark` is `isForbiddenTable()` in the bridge (with it disabled a client moved the floor, `writable.py` step 4). A trigger on each bridge-owned table, attached in the init SQL next to its `CREATE TABLE`, that refuses a write when `zb.principal` is set to anything but `zb_sweeper` would make PostgreSQL refuse it too; it needs an audit of every path that writes those tables (enrollment, the sweeper, `--revoke`, receipts) for a leftover `zb.principal`.
- **The internal-table lists can collapse to the prefix.** `zebridge_is_internal_table()` and its two Zig mirrors are still name lists; the rule "`zebridge_` except the deliberately public `zebridge_gc_watermark`" would replace them, but "internal" (never replicate) and "never client-writable" are two questions, and `zebridge_user_tenants` is exempt in `event_processor.zig` for its own reason: check every use first.
- **Stale leaf chains:** no age bound by design (an arbitrary cutoff would send a storm back to the hub). Age (`cut_age_ms`) and backlog (`cdc_behind`) are reported in the fence log and the `chain` measure event; a cost-based fallback and a bound on waiting for a fresh leaf can be decided on that evidence. The joint fence proves its participants from the hub only, never from a leaf's chain.
- **Parking for a single owed fence in libzb.** zb-client-ts keeps a failed fence's routes parked in memory; libzb does not for a single fence (the tail replays from the cursor meanwhile, by design of #9–#13), only for a joint one. Whether a restart should park the routes of any owed fence is open.
- **A re-check right before the commit in libzb.** The joint re-proof runs between the VERIFY and the commit there, a few requests apart. zb-client-ts refreshes its own proof inside the completion section; the gap rule stays necessary in both.
- **Upstreaming nats.zig:** small independent topic branches off upstream `main` first — direct-get subject (01), named consumer (02), inactive threshold (06), objstore double release (10), JetStream error level (12), objstore max-age (16), JetStream domain (17), creds from memory (19), KV bind (27). The pull-inbox series is an issue to discuss before a patch; 08/11/14/15/25 depend on the maintainer's taste.
- **Mutation throughput on a multi-core VPS.** `ZB_INGRESS_LANES` (1 to 8) is the knob; one lane applies about 8,500 writes/s, two about 14,000, four about 19,500 on one Mac (OPERATIONS). The admission process in front of the lanes was not measured; run `examples/09-event/ramp.sh` through it before sizing a host.
- Documentation hook: the only remaining flags are vendored `zig-pkg/tls-*` readmes.

## Baselines at `2a8bd7c` (the head of #41)

- Bridge: `zig build test --summary all` — 486 of 488 tests pass, 2 skipped (need a live PostgreSQL). A cached run prints no counts: pass `--seed` or change something to see them.
- libzb: `cd libzb && zig build test --summary all` clean (59 of 61, 2 skipped).
- TS: `cd zb-client-ts && pnpm typecheck && pnpm test` — 421 pass.
- Live, in the sandbox: `recreate_recovery.py --check` 1, 2, 3, 4, 12, 13, 14, 15, 16, 19, 20 clean; `reconcile.py` 26 of 26; `leaf_fence.py` was last run on #17; `rls_visibility.py` passes on the owner's Compose stack. A claim about recovery is not done until the live scenario passes.

## Rebuilding the local environment (sandbox recipe)

Cloud containers are ephemeral and their processes die between turns: nothing survives. Needed: PostgreSQL 16 (`/usr/lib/postgresql/16/bin`), `nats-server` 2.15 or newer, the `nats` CLI and `nsc`, Zig 0.17, a Python with `nats-py nkeys msgpack`, node with `pnpm`.

1. **Dependencies.** The sandbox blocks Zig's archive fetch. `git clone` each dependency, `git archive` it to a tarball, `zig fetch ./x.tar.gz`. `libzb/build.zig.zon` may be trimmed locally for that; it is marked skip-worktree (`git ls-files -v libzb/build.zig.zon` shows `S`). **Never commit it.** Restore with `git update-index --no-skip-worktree libzb/build.zig.zon && git checkout libzb/build.zig.zon`.
2. **Submodule.** `git submodule update --init` fetches the fork; if the sandbox cannot reach it, check the submodule out at the pinned commit from a local copy. A submodule left dirty or at another commit trips the "uncommitted changes" stop hook.
3. **The stack.** `BRIDGE_CDC_PUBLICATION=zb_pub scripts/linux/up.sh` starts PostgreSQL and NATS, mints the credentials and applies the init SQL; then `python3 scripts/reprovision.py`. As root, PostgreSQL runs as the `postgres` user with its data in `/var/lib/zebridge-dev/pgdata`, and `pg_ctl` refuses to run as root (so `standby.py` cannot run in a root sandbox). `scripts/linux/README.md` has the exports and the install checklist.
4. **Builds.** The bridge and libzb in `ReleaseFast` take several minutes each; the fault-hook builds (`-Dtest-faults=true -p zig-out-faults`, bridge and libzb) are separate and needed by `recreate_recovery` 4/13/15/19/20. After a change of host CPU, native binaries can die with SIGILL: rebuild.
5. **Run a check.**

   ```sh
   export ZB_PGBIN=/usr/lib/postgresql/16/bin PATH=$ZB_PGBIN:$PATH
   python3 -u scripts/scenarios/recreate_recovery.py --check 19
   ```

   One check at a time; each takes minutes (the live checks 19 and 20 about twelve). `leaf_fence.py` and `rls_visibility.py` run the same way.

## Gotchas that cost time

- Learned on 2026-10-30 and kept where they belong (CLAUDE.md Conventions and Pitfalls, OPERATIONS "Upgrading a hub…"): stacked PRs merge into their base branch, not `main` (PR #41); which database an env file reaches (`system_identifier`); a bare `bridge` as root is iproute2's; the smoke client's 300 s live-row window wants a script.
- `MUTATION_ADMISSION=1` exported in the test terminal (the dev stack's admission mode) breaks the scenarios that run a private NATS stack and no admission process, `standby` and `multi_bridge`: writes get `NoStreamResponse`. Run them with it unset.
- An interrupted scenario run (a power cut, a killed terminal) leaves its replication slot behind (`zb_hf2_…` from `hub_fallback`), and `run.py live`'s `check` then reports the orphan. Drop it as a superuser (`docker exec zb-dev-postgres psql -U postgres`); the bridge's own role may not.
- `psql` with `DATABASE_WRITER_URL` runs under row-level security and shows no `test_types` rows even when there are some. Inspect fixtures through the scenarios' admin connection (`zb.psql`); a diagnosis of `shared_cut` nearly went wrong on this.
- Hooks block shell text that looks like secrets or env access; put it in a file written with Write/Edit.
- `pkill -f` kills the shell running it. Use `pgrep -x` or the `[p]attern` trick, in a call of its own.
- The Zig test runner fails a test that logs at `err`; use `log.warn` for conditions a test provokes. Zig 0.17 rejects `"x" ** 200` in a test body ("binary operator has whitespace on one side"): build such strings with `@memset`.
- TS fixtures are `Object.create(ZeBridge.prototype)` with hand-listed fields: a new private field must be added to `fence-apply.test.ts`, `reconcile.test.ts`, `measure.test.ts`, and the fixture's SQLite needs any new table or column.
- libzb is single-threaded in `poll`: a timing hook that sleeps cannot hold a fence open while another pass runs (that is why `ZB_TEST_SCHEMA_PAUSE_FILE` exists); the TS client takes the same hold through `faultHook`. The same single thread is why libzb cannot have the completion race that zb-client-ts had in the joint fence.
- A position of 0 is a gap (`streamHasGap`), except on a stream the client recorded as `born_empty` that is still empty (NOTES "A stream that has never held a message is not a gap"). Every reset of a position clears the flag, the rewind of a reconciliation included; a client's FIRST sight of an empty `CDC_PUBLIC` is still a gap once; a scenario that needs a message on it writes one open-tenant row first.
- The mutation-admission mode is a switch for the whole stack: the queue becomes `$ZB.mutation.>` and a publisher's reply inbox must be `_INBOX.<principal>.…`, or admission refuses the request and the publisher only times out (the admission log counts refusals by reason; OPERATIONS "Troubleshooting").
- A clone that fetches only `main` shows no upstream for a pushed branch, and the stop hook then reports unpushed commits: add `remote.origin.fetch` for the branch and `git branch -u origin/<branch>`.
- Reviews of merged PRs have found real P1s each time, the joint fence's included (a restart that forgot the group, a completion race, a proof older than the queue it waited in). Before calling an increment done, probe the _failure_ paths (a refused PREPARE, a crash after the first chunk, a restart in the middle, a conversion fence to plain seed) and the overlap of two passes, not just the happy path; and prove each regression test fails without its fix.
