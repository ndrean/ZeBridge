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
| `NATS_NKEY_SEED` | *required*, from `.env` |
| `ZB_PSQL` | `docker exec -i postgres-primary psql -U postgres` |
| `DATABASE_URL` | *required by the bridge-spawning probes* — they start a real bridge, and it no longer has a PG_HOST/PG_USER fallback |
| `ZB_BRIDGE_ARGS` | `--slot zb_probe --pub my_pub --port 9096` — the probes get their own slot and port so they never disturb a running bridge |

Subject and stream names are read from `topology.json`, never hardcoded: one rename must
move the bridge, `nats-init` and this harness together.

## Scripts

| script | scenario | asserts |
| --- | --- | --- |
| `snapshot.py [table] [seed_rows]` | B, C | chunks replay, no duplicate keys, every chunk fits `max_payload`, and the key sequence is **identical to PostgreSQL's own `ORDER BY`** — which is what proves composite keyset pagination. Pass `25000` to force ~3 chunks and exercise a real boundary |
| `stampede.py [table] [n]` | C1 | N concurrent requests → exactly **one** enters the window; the rest are refused by the broker |
| `burst.py [table] [stmts] [rows]` | I | generates a burst large enough to build a backlog, then names the LOOP numbers that mean healthy versus "the reader cannot drain its socket" |
| `speed.py [stmts] [rows]` | I | automates the README's "measured throughput" method end to end: seeds `users`, starts its own bridge at the documented `BASE_BUF`/`RING_BUFFER_COUNT`, drives the same burst, polls `/metrics` for the end-to-end rate, and samples the bridge process's own RSS against its boot-computed "Event ring: N MB" line — so a config or allocator change reports a real number next to the theoretical one instead of asking someone to eyeball the log |
| `wide.py` | B, C | **owns the only bridge** (the snapshot consumer is a fixed durable, so a second bridge competes for the same requests). Three acts: residency stays flat as the table grows 10x — measured inside one warm process, because across two fresh bridges the ring-buffer slab dominates and the numbers come out *inverted*; a row over the message budget is refused by the **pre-scan alone**, isolated with `pg_replication_slot_advance` so CDC cannot refuse it first; and an aborted snapshot purges its orphan chunks and publishes no descriptor |
| `replies.py [table]` | ingress | every write gets a definitive reply: `accepted`, `stale` (row present, newer version won) and `row_deleted` (tombstoned **or** absent) — two of which are zero rows affected and so cannot be told apart from the row count |
| `rowsize.py [table]` | ingress | a row the change feed cannot carry is refused at ingress (`RowTooLargeToReplicate`) with **no table suspended** — before this, one legal 50 KB write earned its sender an `accepted` and cost every other client the table. Also asserts the limit is published as `max_row_bytes`, since a client cannot size a write against a number it cannot read |
| `keys.py [table]` | ingress | a database-allocated (`serial`/`identity`) primary key on an edge-writable table: preflight names it at boot, and the write path **refuses** every mutation with `DbAllocatedKey` rather than letting it succeed and collide with the application's own inserts later. ⚠️ Grants INSERT to make the finding reachable and revokes it afterwards |
| `guards.py [table]` | ingress | the `BEFORE UPDATE`/`BEFORE DELETE` write guards: a writer that forgets the version has it stamped, a writer that sets it keeps its value, a direct `DELETE` becomes a tombstone, and the **sweeper alone** can still reap — the two halves that must both hold, or the soft-delete design deadlocks. ⚠️ Installs guards on a live table and removes them again, since leaving them on turns every other scenario's cleanup `DELETE` into a tombstone |
| `clamp.py [table]` | ingress | a version a year ahead is capped at the database's clock, the row unfreezes once the tolerance passes, the verdict reports the stored value **in §7.2 wire format** rather than PostgreSQL's own printing, and a version inside the tolerance is untouched |
| `invalidate.py` | schema | migrations run against a **live** bridge, and the four caches that must notice: the KV schema (DDL trigger → WAL → KV), the replication thread's relation cache (⚠️ decoded positionally — staleness shifts values rather than erroring), the refusal registry (a keyless table refused, then **fixed without a restart**, then dropped), and the write path's catalog cache — both halves, since a *dropped* column heals through PostgreSQL's `42703` while an *added* one never reaches PostgreSQL at all |
| `mutate.py [table] [id]` | ingress | the last-write-wins round trip: a stale version must lose even when it arrives last |
| `offline.py [table]` | ingress | a write composed **offline** and replayed later: it must be judged on the version it carries, not on when it arrived. Stale replays lose, newer ones apply, equal versions resolve by tiebreak in either order, and a stale update **cannot resurrect a row deleted while the client was away** — the case that silently undoes every deletion if it is wrong. Also pins replay idempotency to its actual shelf life: dedup collapses a replay for 120s, after which correctness rests on LWW and the idempotent upsert, never on `duplicate: true`. ⚠️ Versions are pinned literals on a fixed date, which doubles as the marker for sweeping rows left by an interrupted run |
| `poison.py [table]` | ingress | four unprocessable messages are dead-lettered **once each**, not retried forever |
| `probe.py [table]` | ingress | the price of publishing schemas to every principal: a client can build a well-formed write for a table it has no grant on. Asserts the **premise** (the schema of an unwritable table really is readable — untested anywhere else), that the refusal is **definitive**, that it arrives **exactly once** and nothing follows it, and that no row landed. ⚠️ Must run as a client principal — as the bridge it proves nothing, since the bridge is allowed to do everything it checks for. `writable.py` covers the refusal's *reason*; this covers its *cardinality*, because silence makes the client retry forever (§7.1) and repetition makes the broker amplify one message into many |
| `crosstenant.py [victim]` | ingress | can a principal read ANOTHER tenant's data by any primitive, not just the documented one? Proves the finding that a subject grant governs core SUBSCRIBE only — a JetStream consumer's `filter_subject` is reader-chosen and unchecked, so the enforcement unit is the **stream**. After the per-tenant split: alice is refused on every CDC primitive; the remaining reachable one is the INIT/snapshot path (§1.12, unbuilt). ⚠️ Resolves reachable-vs-nonexistent against `topology.json` so a missing stream is never scored as a refusal. Run as a client principal |
| `endpoint.py` | config | one process, one NATS address — `NATS_HOST=nats-server` beside a working `NATS_URL` must not leave ingress dialling somewhere else |
| `sizing.py` | config | the startup guards on pre-allocated memory: `BASE_BUF` over `max_payload` is refused (isolated with a minimum ring, or the RAM guard fires first and the payload check rots untested), an unfittable ring is refused **naming both the data and metadata terms**, out-of-range values **clamp** while unparseable ones default, and the sizing check agrees with the allocator's own total — two independent computations that disagreed by 40% while the check ignored metadata |
| `envcheck.py` | config | offline: the port in `.env.bridge`'s URLs matches `PG_PUBLISH_PORT` in `.env.admin`, and no admin variable has crept back into the bridge's file. Nothing else can check this — the bridge does not know what compose published, and giving it `.env.admin` would undo the split |
| `check.py` | config | **drift** — every place two copies of one fact can disagree, compared: the live NATS conf vs `zebridge_user_tenants`, snapshot reach vs subscribe reach, `topology.json:public_tables` vs the database, `TENANT_RULES` and `SYNC_RULES` vs the schema, published tenant-column tables with no routing rule, unsubstituted `${…}` in the rendered conf, orphaned replication slots, and `OPEN_TENANT` — the one env rendered into the guard, the CDC_PUBLIC stream and every read grant — compared across all three. Reads only; changes nothing. ⚠️ Exits non-zero on drift so the verdict travels, but **nothing should `depends_on` it** — the declaration is often legitimately ahead of reality (add a table, migrate, set TENANT_RULES, reload NATS) and findings inside that window are correct without being bugs. Also runs in compose: `docker compose … run --rm zb-check` |
| `tenant_writes.py` | ingress | the tenant WRITE model, driven end to end: an omitted tenant is stamped from the writer's identity (not a constant — the fail-open bug this pins), a forged tenant and a malformed token are refused, an unmapped principal fails closed, the open tenant is a *mapping* not a default, a restricted user may write AND update open rows (the carve-out), and DELETE integrity holds (alice cannot reach bob's row). The behavioural half of what `check.py` verifies structurally. ⚠️ Sets `zb.principal` inside the transaction as `bridge_writer`; owns a throwaway table it drops |
| `tenant_kv.py` | B | `$KV.tenants.<principal>` — PROTOCOL.md "The Connection Flow" Step 0, the read-side counterpart to `tenant_writes.py`. Four principals each resolve their own tenant correctly (including `john`, mapped to the open tenant rather than unmapped — that distinction is the point), a genuinely unmapped key resolves as a clean miss rather than a Permissions Violation, cross-principal reads are denied (the exact-key grant, not a wildcard — `alice` cannot read `bob`'s mapping even though both resolve `globex`-adjacent tenants), a live `INSERT`/`UPDATE` on `zebridge_user_tenants` propagates to the bucket with **no bridge restart**, and `zebridge_user_tenants`'s own rows never surface as `cdc.zebridge_user_tenants.*` on any tenant stream — the roster-disclosure leak the whole bucket exists to avoid. ⚠️ Live-propagation checks are skipped, not failed, if no bridge answers within 15s; the per-principal and cross-denial checks need only NATS. Does not cover `@nats-io/kv`'s `Kvm.open()` defaulting away from Direct Get (`allow_direct` must be passed explicitly) — that is a `web-consumer`-specific client bug this Python harness cannot reproduce, documented instead in `App.tsx`'s `resolveTenant()` |
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

The write path is off unless a role and a table are opened for it:

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
