# Scenario scripts

Runnable counterparts to `TEST_SCENARIOS.md`. Each prints the evidence the scenario
claims, so a group in that document is a command rather than a paragraph someone has to
re-derive at 1am.

Deliberately **not** in `python-consumer/`: that is a reference *client*, meant to show
an SDK author what a correct consumer looks like. These are a test harness. They rot for
different reasons and should read as different things.

Python here because these grew out of live debugging and the decode side needs
MessagePack. The natural alternative is `emitter/` — it already has `gnat` and a
Postgres pool, so it would not need the `psql` shell-out below. Worth moving if this
harness grows.

## Setup

```bash
python3 -m venv scripts/scenarios/.venv
scripts/scenarios/.venv/bin/pip install -r scripts/scenarios/requirements.txt

set -a && . ./.env.bridge && set +a   # DATABASE_URL, NATS_URL, BRIDGE_PORT
export NATS_NKEY_SEED="SU..."         # kept out of the file on purpose

scripts/scenarios/.venv/bin/python scripts/scenarios/stampede.py
```

`.env.bridge` and not `.env.admin`: the probes drive the bridge, and the bridge has no
business seeing admin credentials — that separation is one of the things `endpoint.py`
exists to keep true.

`nats-py` needs `nkeys` alongside it for seed authentication — it imports the package
lazily, so a missing dependency surfaces at connect time, not at install time.

| variable | default |
| --- | --- |
| `NATS_URL` | `nats://127.0.0.1:4222` |
| `NATS_CREDS` | *the JWT world's credential*: path to a .creds file (bridge.creds for admin scenarios, a principal's for client ones). Wins over the seed in `connect()` and `nats_cli` |
| `NATS_NKEY_SEED` | legacy (pre-operator server), from `.env` |
| `ZB_PSQL` | `docker exec -i postgres-primary psql -U postgres` |
| `DATABASE_URL` | *required by the bridge-spawning probes* — they start a real bridge, and it no longer has a PG_HOST/PG_USER fallback |
| `ZB_BRIDGE_ARGS` | `--slot zb_probe --pub my_pub --port 9096` — the probes get their own slot and port so they never disturb a running bridge |

Subject and stream names are read from `grammar.json` (via `zb.py`), never hardcoded:
one rename must move the bridge, `nats-init` and this harness together. `zb.py` also
exposes `tenants()`, which reads the live tenant list from `zebridge_user_tenants` —
tenants are data, not config, so no file carries them.

## ⚠️ Special / heavy scenarios — run in ISOLATION, never batched

Most scenarios spin their own probe bridge on a private slot+port (`zb_probe` / :9096)
and leave a running production bridge untouched — those are safe to run back-to-back.
A subset is **special** and must be run one at a time, alone, on a quiet system,
NOT folded into a "run everything" pass:

| scenario | why it is special |
| --- | --- |
| `speed.py` | measures THROUGHPUT — any other load on the box skews the number |
| `leaksoak.py` | long memory soak (`SOAK_SECONDS`) — concurrent churn from another scenario invalidates the RSS-drift reading; also the one that wants a ReleaseFast build for the production-shape leak proof |
| `chaos.py` | **restarts the shared nats-server** and kills PG backends — every other consumer (browsers, a running bridge) disconnects during it |
| `race.py` | concurrent-contention probe, owns the bridge (WIP — see its banner) |
| `adversarial.py` | fires hostile input at a probe bridge; owns the bridge, runs as a client principal |
| `downtime.py` | stops and restarts a bridge on a SHARED slot to prove changes committed during the outage still replay; owns the only bridge |
| `faults.py` | deletes and recreates the `REQUESTS` stream out from under the bridge |

Rule of thumb: if a scenario's `⚠️` line says it *restarts nats-server*, *owns the only
bridge*, or *measures timing/memory*, give it the whole machine for its run. Stop any
production bridge and close browser tabs first; each restores what it touched on the way
out, but they will disrupt anything live meanwhile. The rest of the table below is the
non-invasive set.

## Scripts

| script | scenario | asserts |
| --- | --- | --- |
| `snapshot.py [table] [seed_rows] [tenant]` | B, C | chunks replay, no duplicate keys, every chunk fits `max_payload`, and the key sequence is **identical to PostgreSQL's own `ORDER BY`** — which is what proves composite keyset pagination. Pass `25000` to force multiple chunks and exercise a real boundary. Tenant-scoped (§1.12): `tenant` defaults to the table's own published `tenant_column` (`$KV.schemas.<table>`) when set, else `OPEN_TENANT` — a seeded row is stamped with that tenant so it survives RLS into the snapshot it was seeded to force |
| `stampede.py [table] [n]` | C1 | N concurrent requests → exactly **one** enters the window; the rest are refused by the broker |
| `burst.py [table] [stmts] [rows]` | I | generates a burst large enough to build a backlog, then names the LOOP numbers that mean healthy versus "the reader cannot drain its socket" |
| `speed.py [stmts] [rows]` | I | automates the README's "measured throughput" method end to end: seeds `users`, starts its own bridge at the documented `BASE_BUF`/`RING_BUFFER_COUNT`, drives the same burst, polls `/metrics` for the end-to-end rate, and samples the bridge process's own RSS against its boot-computed "Event ring: N MB" line — so a config or allocator change reports a real number next to the theoretical one instead of asking someone to eyeball the log |
| `wide.py` | B, C | **owns the only bridge** (the snapshot consumer is a fixed durable, so a second bridge competes for the same requests). Three acts: residency stays flat as the table grows 10x — measured inside one warm process, because across two fresh bridges the ring-buffer slab dominates and the numbers come out *inverted*; a row over the message budget is refused by the **pre-scan alone**, isolated with `pg_replication_slot_advance` so CDC cannot refuse it first; and an aborted snapshot purges its orphan chunks and publishes no descriptor |
| `decode_integrity.py [rows]` | B, C | **owns the only bridge.** Every decoded value equals PostgreSQL's own, on BOTH read paths: seeds a fixture mixing uuid/int/text/jsonb/numeric/enum, then compares each CDC event and each replayed snapshot chunk field-by-field against `psql`'s answer (NUMERIC by decimal value — the snapshot path pads to declared scale, CDC does not, and that difference is not a bug). Declares its fixture with one catalogue INSERT before the probe boots and asserts boot reconciliation bound `cdc.decode_fixture.>` itself — the by-hand `nats stream edit` half of the old flow is now a bridge boot step under test |
| `replies.py [table]` | ingress | every write gets a definitive reply: `accepted`, `stale` (row present, newer version won) and `row_deleted` (tombstoned **or** absent) — two of which are zero rows affected and so cannot be told apart from the row count |
| `rowsize.py [table]` | ingress | a row the change feed cannot carry is refused at ingress (`RowTooLargeToReplicate`) with **no table suspended** — before this, one legal 50 KB write earned its sender an `accepted` and cost every other client the table. Also asserts the limit is published as `max_row_bytes`, since a client cannot size a write against a number it cannot read |
| `keys.py [table]` | ingress | a database-allocated (`serial`/`identity`) primary key on an edge-writable table: preflight names it at boot, and the write path **refuses** every mutation with `DbAllocatedKey` rather than letting it succeed and collide with the application's own inserts later. ⚠️ Grants INSERT to make the finding reachable and revokes it afterwards |
| `guards.py [table]` | ingress | the `BEFORE UPDATE`/`BEFORE DELETE` write guards: a writer that forgets the version has it stamped, a writer that sets it keeps its value, a direct `DELETE` becomes a tombstone, and the **sweeper alone** can still reap — the two halves that must both hold, or the soft-delete design deadlocks. ⚠️ Installs guards on a live table and removes them again, since leaving them on turns every other scenario's cleanup `DELETE` into a tombstone |
| `clamp.py [table]` | ingress | a version a year ahead is capped at the database's clock, the row unfreezes once the tolerance passes, the verdict reports the stored value **in §7.2 wire format** rather than PostgreSQL's own printing, and a version inside the tolerance is untouched |
| `invalidate.py` | schema | migrations run against a **live** bridge, and the four caches that must notice: the KV schema (DDL trigger → WAL → KV), the replication thread's relation cache (⚠️ decoded positionally — staleness shifts values rather than erroring), the refusal registry (a keyless table refused, then **fixed without a restart**, then dropped), and the write path's catalog cache — both halves, since a *dropped* column heals through PostgreSQL's `42703` while an *added* one never reaches PostgreSQL at all. Its `SCRATCH` fixture is what found NOTES.md §2.19 (a table with no CDC route blocking on every publish instead of failing fast) — deliberately never `zebridge_enable`d — no catalogue row, so no CDC route — and after its no-PK refusal lifts it correctly *stays* suspended, now for `no_cdc_subject` |
| `mutate.py [table] [id]` | ingress | the last-write-wins round trip: a stale version must lose even when it arrives last |
| `offline.py [table]` | ingress | a write composed **offline** and replayed later: it must be judged on the version it carries, not on when it arrived. Stale replays lose, newer ones apply, equal versions resolve by tiebreak in either order, and a stale update **cannot resurrect a row deleted while the client was away** — the case that silently undoes every deletion if it is wrong. Also pins replay idempotency to its actual shelf life: dedup collapses a replay for 120s, after which correctness rests on LWW and the idempotent upsert, never on `duplicate: true`. ⚠️ Versions are pinned literals on a fixed date, which doubles as the marker for sweeping rows left by an interrupted run |
| `poison.py [table]` | ingress | four unprocessable messages are dead-lettered **once each**, not retried forever |
| `writable.py [writable_table] [readonly_table]` | ingress, schema | edge-writability has two independent statements that must agree — the grant (`has_table_privilege`) and the schema's own capability (a usable version column, a client-mintable key) — computed here in raw SQL as the ground truth. Checks that against three things: the matrix itself (a table granted writes its schema cannot honour is a finding), the schema payload's own published `writable` field (`$KV.schemas.<table>`, NOTES.md §1.11 — the bridge computes the same verdict independently in Zig, at boot and on DDL; this is the drift check), and what a client actually observes sending a real write to a refused table (a definitive verdict, never silence — `permission denied` used to be a SQL error the bridge treated as transient, so the write NAK'd to `max_deliver` and then just stopped, with no dead letter and no trace but one log line) |
| `probe.py [table]` | ingress | the price of publishing schemas to every principal: a client can build a well-formed write for a table it has no grant on. Asserts the **premise** (the schema of an unwritable table really is readable — untested anywhere else), that the refusal is **definitive**, that it arrives **exactly once** and nothing follows it, and that no row landed. ⚠️ Must run as a client principal — as the bridge it proves nothing, since the bridge is allowed to do everything it checks for. `writable.py` covers the refusal's *reason*; this covers its *cardinality*, because silence makes the client retry forever (§7.1) and repetition makes the broker amplify one message into many |
| `crosstenant.py [victim]` | ingress | can a principal read ANOTHER tenant's data by any primitive, not just the documented one? Proves the finding that a subject grant governs core SUBSCRIBE only — a JetStream consumer's `filter_subject` is reader-chosen and unchecked, so the enforcement unit is the **stream**. After the per-tenant split: alice is refused on every CDC primitive; the remaining reachable one is the INIT/snapshot path (§1.12, unbuilt). ⚠️ Resolves reachable-vs-nonexistent against the declared stream set (`grammar.json` prefixes × the live tenant list) so a missing stream is never scored as a refusal. Run as a client principal |
| `endpoint.py` | config | one process, one NATS address — `NATS_HOST=nats-server` beside a working `NATS_URL` must not leave ingress dialling somewhere else |
| `sizing.py` | config | the startup guards on pre-allocated memory: `BASE_BUF` over `max_payload` is refused (isolated with a minimum ring, or the RAM guard fires first and the payload check rots untested), an unfittable ring is refused **naming both the data and metadata terms**, out-of-range values **clamp** while unparseable ones default, and the sizing check agrees with the allocator's own total — two independent computations that disagreed by 40% while the check ignored metadata |
| `envcheck.py` | config | offline: the port in `.env.bridge`'s URLs matches `PG_PUBLISH_PORT` in `.env.admin`, and no admin variable has crept back into the bridge's file. Nothing else can check this — the bridge does not know what compose published, and giving it `.env.admin` would undo the split |
| `adversarial.py` | config | **hostile input at the two entry points — the bridge's OWN code is the target**: 17 malformed/injection msgpack mutation shapes (truncated, non-map, unknown column, column-name and value SQL-injection, NUL bytes, deep nesting) each asserting SURVIVE + refuse-once + no-bad-state (a legit write still flows), plus /enroll param fuzzing. Found the EndOfStream poison-pill DoS (Finding 2). Run as a client principal (omar) — the allow-list confines it to its own lane |
| `race.py` | config | **concurrent-consumer races against nats.zig**: N writers hammer the mutation lane with distinct keys (lost/torn update detectable), THROUGH a broker restart, and poison interleaved with legit under concurrency — the synchronous-per-connection safety ARGUMENT put to the test. ⚠️ owns the bridge and restarts nats-server |
| `chaos.py` | config | **exhaustion and jitter — how clean the bridge REMAINS**: bad NATS creds boot must refuse with a SILENT allocator audit (the early-exit leak regression); the broker is killed and restarted under the bridge (nats_reconnects moves, writes flow after); every bridge PG backend is terminated (pg_reconnects moves, a CDC write AND a mutation round-trip land after — reader and writer both re-attach); 16 idle sockets exhaust :9090 and the receive watchdog recovers it UNASSISTED; finally `leaks` must report 0 on the survivor. ⚠️ **owns the only bridge and restarts nats-server** — every other consumer disconnects during phase 2; destructive to comfort, never to data (JetStream is file-backed, the slot retains WAL) |
| `leaksoak.py` | config | **memory over time, the long-runner's audit**: samples macOS `leaks` (malloc zones — libpq's world; Zig's own allocators are mmap-backed and audited instead by the DebugAllocator's exit report) and RSS around a churn window grinding the REAL verb matrix — INSERT/UPDATE/DELETE on a public table, INSERT/DELETE tenant-scoped (the producer's deltas ride them), bogus enrolls every round plus a SUCCESSFUL mint every 10th (writer CTE + JWT signing + the user_tenants CDC diversion), periodic snapshot requests (the COPY/chunk worker), real edge-write envelopes (the mutation listener's decode/apply/verdict), one sweeper dry-run pass, /metrics and /status renders — asserting ZERO malloc leaks and RSS drift within budget (`ZB_RSS_DRIFT_MB`, default 64 — the event-ring slab is pre-allocated and constant by design, so drift is the signal, not size). `SOAK_SECONDS` scales it from suite-quick to an hours-long soak |
| `connbudget.py` | config | **the connection budget, re-derived from the live system** (policy, never trust): both role `CONNECTION LIMIT`s exist (not -1) and their sum leaves the cluster real headroom; the writer ceiling **bites** (opens limit+2 sleeper connections as `bridge_writer`, expects PG's own `too many connections for role`); a 12-burst on `/enroll` answers a 403/503 mix (permit pool alive and sized under the burst) while `/metrics` returns 200 MID-burst. ⚠️ Briefly saturates the writer role's slots — the live mutation listener may see one refused reconnect during it, which is the ceiling working. Skips the permit test gracefully on an unarmed bridge |
| `check.py` | config | **drift** — every place two copies of one fact can disagree, compared: the live NATS conf vs `zebridge_user_tenants`, snapshot reach vs subscribe reach, the catalogue's public set vs `CDC_PUBLIC`'s bound subjects (drift = a bridge restart due), the catalogue's tenant and LWW columns vs the schema, published tenant-column tables with no routing rule, unsubstituted `${…}` in the rendered conf, orphaned replication slots, and `OPEN_TENANT` — the one env rendered into the guard, the CDC_PUBLIC stream and every read grant — compared across all three. Reads only; changes nothing. ⚠️ Exits non-zero on drift so the verdict travels, but **nothing should `depends_on` it** — the declaration is often legitimately ahead of reality (a `zebridge_enable` migration lands, the bridge has not restarted, NATS not reloaded) and findings inside that window are correct without being bugs. Also runs in compose: `docker compose … run --rm zb-check` |
| `tenant_writes.py` | ingress | the tenant WRITE model, driven end to end: an omitted tenant is stamped from the writer's identity (not a constant — the fail-open bug this pins), a forged tenant and a malformed token are refused, an unmapped principal fails closed, the open tenant is a *mapping* not a default, a restricted user may write AND update open rows (the carve-out), and DELETE integrity holds (alice cannot reach bob's row). The behavioural half of what `check.py` verifies structurally. ⚠️ Sets `zb.principal` inside the transaction as `bridge_writer`; owns a throwaway table it drops |
| `tenant_kv.py` | B | `$KV.tenants.<principal>` — PROTOCOL.md "The Connection Flow" Step 0, the read-side counterpart to `tenant_writes.py`. Four principals each resolve their own tenant correctly (including `john`, mapped to the open tenant rather than unmapped — that distinction is the point), a genuinely unmapped key resolves as a clean miss rather than a Permissions Violation, cross-principal reads are denied (the exact-key grant, not a wildcard — `alice` cannot read `bob`'s mapping even though both resolve `globex`-adjacent tenants), a live `INSERT`/`UPDATE` on `zebridge_user_tenants` propagates to the bucket with **no bridge restart**, and `zebridge_user_tenants`'s own rows never surface as `cdc.zebridge_user_tenants.*` on any tenant stream — the roster-disclosure leak the whole bucket exists to avoid. ⚠️ Live-propagation checks are skipped, not failed, if no bridge answers within 15s; the per-principal and cross-denial checks need only NATS. Does not cover `@nats-io/kv`'s `Kvm.open()` defaulting away from Direct Get (`allow_direct` must be passed explicitly) — that is a `web-consumer`-specific client bug this Python harness cannot reproduce, documented instead in `App.tsx`'s `resolveTenant()` |
| `generations.py` | schema | `zebridge_generations` — the delta-generation producer's memory (NOTES.md §1.13), whole contract: shape (PK `(tenant, tbl, gen)`, `cutoff_lsn` a real `pg_lsn`), **invisible to clients** (no DDL event — `zebridge_is_internal_table` swallows it — not published, no `$KV.schemas` key), the reader executes the LSN-before-snapshot build recipe on its single deliberate write grant, the PK forbids chain forks, **append-only by privilege** (reader has no UPDATE), the writer is refused entirely, and cleanup runs through the DELETE grant pruning will use |
| `objgrants.py` | tenancy | The delta-generations storage prerequisite, verified: per-tenant Object Store buckets scope like CDC_/INIT_ streams — a bucket IS a stream (`OBJ_<bucket>`), so per-stream API grants are the boundary. Edits the LIVE native conf (alice gains only `OBJ_genacme` subjects, restored in `finally` via SIGHUP), then proves she reads her bucket byte-for-byte and is refused the other tenant's by the broker. Finding that rode along: nats-py's `obj.get()` needs `$JS.API.STREAM.NAMES` (subject→stream lookup, names-only leak) — clients that bind the stream explicitly need no such grant |
| `widthguard.py` | schema | The row-width guard closing NOTES.md §1.13's case C ("accepted, then everybody freezes"): `zebridge_install_width_guard` generates a STATIC per-table BEFORE trigger over unbounded columns only (text, unbounded varchar, bytea ×2 for hex, json/jsonb/xml, arrays; bounded-only tables get none — statically safe, zero cost), budget baked into the trigger as a literal, re-derived at each bridge boot from `zebridge_limits` (one row per instance, MIN over the instances carrying the table), refusing with ERRCODE 23514 — already in the listener's permanent class, so an edge write becomes a `rejected` verdict with zero bridge code and a psql write an ordinary ERROR. Proves both doors: the oversized INSERT, the just-under row (ceiling, not tax), the small fattening UPDATE refused atomically, and the same fattening edit as a real alice mutation — the case the ingress payload check cannot see — answered `rejected`/23514 with the row untouched |
| `downtime.py` | durability | **Changes committed while the bridge is DOWN must still reach CDC** — the guarantee a persistent slot exists to provide, and the one nothing tested until finding 4 (the bridge resumed from the WAL *head* instead of the slot, so every change made during an outage was skipped, silently and forever). Reuses ONE slot across two bridge lifetimes and commits all three verbs into the gap between them, then repeats it across a `kill -9` (no handler runs, no final status update — only what PostgreSQL already holds survives). Verified RED on the pre-fix binary, where INSERT, UPDATE *and* DELETE were all lost. No existing scenario could catch this: every other one spins a FRESH slot, and on a fresh slot the WAL head and `confirmed_flush_lsn` are the same position. ⚠️ Owns the only bridge |
| `livebirth.py` | schema | Zero-restart onboarding, recorded from the live-birth exercise: a table CREATEd + `zebridge_enable`d while the bridge runs must cascade completely with no restart — full migration output (grants, guards, width guard, generations-derived, publication LAST), schema pushed to `$KV.schemas` live by the DDL pipeline (the boot preflight never saw the table), the chain manifest derived within a cadence with every cutoff canonical UTC (the manifest-timezone regression check: the window query once rendered stored cutoffs in session timezone against the build's +00), the migration-born width guard refusing an oversized psql UPDATE (23514), and DROP TABLE tombstoning the schema live. Pre-declares the fixture with one catalogue INSERT before the probe bridge boots, and asserts the bridge's own boot reconciliation bound the fixture subject — no temp grammar file, no stream edits. ⚠️ Owns the only bridge |
| `legacybait.py` | schema | The guards that fire AFTER prevention failed — the three §1.8 matrix cells `widthguard.py` cannot reach, exercised on a planted pre-guard 20 KB row: the generation producer's detector warns on its first build (chains carry it, CDC on notice), a legacy-path touch triggers the decode-time QUARANTINE (log + `"suspended":true` in `$KV.schemas`), a fresh boot's preflight re-derives the refusal from the stored data ("a row already stored is N bytes"), and the de-quarantine recipe is proven mechanical: hard-remove the bait (user triggers stepped aside — soft-delete would tombstone it, still stored, still too wide), reboot, preflight passes, the healthy schema republish thaws clients. ⚠️ Owns the only bridge (three probes in sequence) — stop the main bridge first |
| `genproducer.py` | schema | The generation producer live (NOTES.md §1.13 milestones 2+3, deltas included): a probe bridge with `GENERATION_RULES=users:_default` must build g1 unprompted as a full-only generation (`users-g1-full` + chain manifest swapped last), SKIP idle cadences (limited cron queries), ship a touched row as a 1-row DELTA whose `(prev_cutoff, cutoff]` bounds match the manifest, keep the chain continuous (`d[i+1].prev_cutoff == d[i].cutoff`, first delta after the full chains off its cutoff, head reaches the manifest cutoff — the client walk), and prune past depth with the full refreshed inside the kept window — PG rows first, then both object kinds. Checks are structural, never tick-counting: the 5s clamp margin can echo one duplicate delta (absorbed; never a gap), and a pruned object's ADR-20 tombstone still shows in plain `list()` (assert with `ignore_deletes`). Needs the probe-bridge env + admin psql. ⚠️ Owns the only bridge since derivation landed: any running bridge with GENERATIONS_ENABLED derives the full published set and would race the probe's controlled cadence — the probe scopes itself with GENERATION_RULES, now a RESTRICTION intersected with the derived set |
| `dyntenant.py` | tenancy | NOTES.md §9 proven live: a brand-new tenant (no `zebridge_user_tenants` row, no streams) is onboarded against a RUNNING bridge with zero restarts — its `CDC_<T>`/`INIT_<T>` streams provisioned at runtime (the backend's half of the contract, and mandatory: a tenant-routed write with no stream is the §2.19 blocking shape), a `zebridge_user_tenants` mapping that propagates to `$KV.tenants.<principal>` immediately, and a row whose tenant value alone routes it onto `cdc.<tenant>.<table>.insert` in the new stream. Cleans up its streams, mapping and rows |
| `tzguard.py` | schema | `zebridge_timestamp_guard` — the mechanical form of "timestamps are timestamptz" (§7.2's wire format and version clamping need absolute instants). Asserts the guard exists (render.py's vanishing-trigger lesson), a naive-`timestamp` CREATE/ALTER is refused with a message that names the fix, `timestamptz` passes, a mixed migration rolls back WHOLE (nothing half-applied — the property that makes it quarantine rather than a warning), and `schema_migrations` stays exempt so `mix ecto.migrate` can still bootstrap a fresh database. Needs only psql |
| `tls.py` | — | `nats.zig` speaks TLS, JetStream included — the capability that makes bridge/NATS colocation a choice rather than a constraint. Standalone (no stack, no `zb`, no env): provisions its own TLS-only + JetStream `nats-server` from the submodule's test certs, generates and builds a Zig probe **against the vendored `nats.zig` itself** (Python is orchestration only), and asserts both directions: a CA-verified `tls://` connect runs the bridge's shapes (stream create, acked publish, durable pull fetch, ack), and the same connect without the CA is **refused** with `CertificateIssuerNotFound` — proving verification is real, not skipped. Skips cleanly if `nats-server` is not on PATH. Complements `nats.zig`'s own e2e TLS suite, which covers core pub/sub and mTLS but not JetStream |
| `credentials.py` | config | the bridge cannot connect as the admin: no `DATABASE_URL` → refuses to start even with full `PG_*` credentials present; no `DATABASE_WRITER_URL` → ingress off rather than falling back to the read role |
| `faults.py` | config | `REQUESTS` missing → the bridge stops **non-zero** instead of spinning; the consumer deleted underneath it → a status frame is recycled, not acked into a null unwrap |

Exit code is 0 when the assertion holds, so these can be chained in CI later. `burst.py`
and `poison.py` report rather than assert — their evidence is in the bridge's log, which
this harness does not read.

`endpoint.py` and `faults.py` start bridges of their own, and `faults.py` deletes the
`REQUESTS` stream: it saves the config first, recreates it byte-identically afterwards,
and **diffs the result** so a silent restore failure is a test failure rather than a
surprise next week. If it ever aborts between the two, it prints the exact
`nats stream add … --config` line to run.

## Prerequisites for the ingress scripts

The write path is off unless a role and a table are opened for it. The standard path is
one `zebridge_enable(...)` migration, which writes the table's `zebridge_catalogue` row
(LWW columns included) — the bridge reads it at boot, no env needed. To open a table by
hand for a quick probe run, the underlying grant plus the env override also works:

```sql
SELECT zebridge_grant_edge_writes('public.test_types');
```

```bash
POSTGRES_WRITER_USER=bridge_writer POSTGRES_WRITER_PASSWORD=… \
SYNC_RULES="test_types:updated_at,deleted_at" \
./zig-out/bin/bridge --slot my_slot --pub my_pub
```

Without writer credentials the mutation listener does not start at all — deliberately,
rather than falling back to the read role, which has REPLICATION.
