# Scenario scripts

Runnable counterparts to `TEST_SCENARIOS.md`. Each prints the evidence the scenario
claims, so a group in that document is a command rather than a paragraph someone has to
re-derive at 1am. Every script's exit code is its verdict (0 = pass); `run.py` sequences
them.

Deliberately **not** in `python-consumer/`: that is a reference *client*, meant to show
an SDK author what a correct consumer looks like. These are a test harness. They rot for
different reasons and should read as different things.

## Setup

The stack is the native one (`scripts/native/up.sh`: Postgres on 127.0.0.1:5432, NATS on
127.0.0.1:4222 with JWT/operator auth). `scripts/native/jwt-bootstrap.sh` mints the creds
files the scenarios connect with: `bridge.creds` (the bridge's own identity) and one per
client principal — alice (acme), bob (globex), mary (globex), nina (tango), omar (kilo).

```bash
python3 -m venv scripts/scenarios/.venv
scripts/scenarios/.venv/bin/pip install -r scripts/scenarios/requirements.txt

set -a && . ./.env.bridge && set +a          # DATABASE_READER_URL, DATABASE_WRITER_URL, NATS_URL, BRIDGE_PORT
export BRIDGE_CDC_PUBLICATION=my_pub         # named, never guessed (NOTES §10ad)

scripts/scenarios/run.py offline             # no stack needed
scripts/scenarios/run.py live                # against the running stack + bridge on :9090
scripts/scenarios/run.py owns                # each starts its OWN bridge: stop yours first
scripts/scenarios/run.py all                 # offline, then live, then owns
scripts/scenarios/run.py live -k mutate      # a subset by name
scripts/scenarios/run.py --list
```

`run.py` sets `NATS_CREDS` per role: `bridge.creds` for scenarios that act as the bridge,
your `NATS_CREDS` (default `omar.creds`) for those that act as a client. Running one
script by hand, set it yourself:

```bash
export NATS_CREDS=scripts/native/creds/omar.creds     # a client principal
scripts/scenarios/.venv/bin/python scripts/scenarios/mutate.py
```

`.env.bridge` and not `.env.admin`: the probes drive the bridge, and the bridge has no
business seeing admin credentials — that separation is one of the things `endpoint.py`
exists to keep true.

| variable | default |
| --- | --- |
| `NATS_URL` | `nats://127.0.0.1:4222` — the address only; the creds file is the credential |
| `NATS_CREDS` | path to a `.creds` file. `bridge.creds` to act as the bridge, `<principal>.creds` to act as a client, confined exactly as a real one. ⚠️ A scenario that exercises a CLIENT's permissions proves nothing connected as the bridge |
| `ZB_PRINCIPAL` | the principal name when the creds file is named otherwise (else the file's stem) |
| `ZB_PSQL` | the native `psql` as admin: `/opt/homebrew/opt/postgresql@18/bin/psql -h 127.0.0.1 -p 5432 -U postgres -d postgres` (or `ZB_PGBIN`/`ZB_PG_HOST`/`ZB_PG_PORT`/`ZB_PG_USER` to move parts of it) |
| `BRIDGE_CDC_PUBLICATION` | **required** — the publication under test, passed to probe bridges as `--pub` |
| `DATABASE_READER_URL` | **required by the bridge-spawning probes** — they start a real bridge, and it has no PG_HOST/PG_USER fallback |
| `DATABASE_WRITER_URL` | required by `sweeper.py` (the sidecar's own connection) |
| `ZB_BRIDGE_ARGS` | `--slot zb_probe --port 9096` — the probes get their own slot and port so they never disturb the bridge on :9090 |
| `BRIDGE_PORT` | `9090`, the long-running bridge `telemetry.py` and the `live` group talk to |

Subject, stream and KV names come from `grammar.json` (via `zb.py`), never hardcoded:
one rename must move the bridge, `nats-init` and this harness together. Configuration
comes from `zebridge_catalogue` (written by `zebridge_enable`): `zb.rules(table)` reads a
table's LWW/tenant columns from it (a `SYNC_RULES`/`TENANT_RULES` env entry still
overrides per table, as the bridge honours it), and `zb.tenants()` reads the live tenant
list from `zebridge_user_tenants` — tenants are data, not config.

## Groups

`offline` needs no bridge and no NATS — pure SQL and files, seconds each. `live` needs
the long-running bridge on :9090 and the stack. `owns` scenarios start a probe bridge of
their own (`--slot zb_probe --port 9096`) and refuse to run beside another bridge —
`run.py` serializes them, never in a parallel lane. `manual` scenarios are listed but
never run by the battery.

### ⚠️ Run in isolation — `owns` and `manual`

Stop the long-running bridge and close browser tabs first; each restores what it touched
on the way out, but they will disrupt anything live meanwhile.

| scenario | why |
| --- | --- |
| every `owns` scenario | starts the only bridge on the shared probe slot; two probes on one slot compete |
| `chaos.py`, `race.py` | **restart the shared nats-server** and/or kill PG backends — every other consumer disconnects |
| `speed.py` | measures THROUGHPUT — any other load on the box skews the number; hours of machine |
| `leaksoak.py` | long memory soak (`SOAK_SECONDS`) — concurrent churn invalidates the RSS-drift reading; wants a ReleaseFast build |
| `objstore_race.py` | 40 MB get/put race against the object store |
| `burst.py` | throughput driver that leaves rows behind and reports rather than asserts |

## Scripts

### offline

| script | asserts |
| --- | --- |
| `render.py` | envsubst on both templates, applied to a scratch DB — a bad render must not silently eat a trigger |
| `tzguard.py` | naive `timestamp` columns refused at DDL time; `timestamptz` passes; a mixed migration rolls back whole; `schema_migrations` stays exempt |
| `tenant_writes.py` | RLS + tenant stamping on the write path: an omitted tenant is stamped from identity, forged/malformed refused, unmapped fails closed, the open tenant is a mapping. Owns a throwaway table; its throwaway tenants make a running bridge onboard `CDC_tw_*` streams |
| `guards.py [table]` | the version/delete write guards: a forgotten version is stamped, a set one kept, a direct `DELETE` becomes a tombstone, the sweeper alone can still reap |
| `generations.py` | the `zebridge_generations` contract: shape, invisible to clients, append-only by privilege, writer refused |
| `envcheck.py` | `.env.bridge` vs `.env.admin`: no admin variable in the bridge's file |
| `pubname.py` | the publication is named, never defaulted — bridge flags/env and the SQL side both |

### live

| script | role | asserts |
| --- | --- | --- |
| `check.py` | bridge | declared vs actual drift: catalogue vs streams, catalogue columns vs schema, `zebridge_user_tenants` vs NATS, orphaned slots. Reads only; exits non-zero on drift, so nothing should `depends_on` it |
| `telemetry.py [base_url]` | none | the HTTP surface: endpoints, a silent client cannot wedge the server, metrics parse and carry the operator set; Prometheus checked if reachable, skipped otherwise |
| `writable.py [writable] [readonly]` | bridge | grants vs the published write contract: the grant/schema matrix, `$KV.schemas.<table>`'s `writable` agrees, and a refused write gets a verdict, never silence |
| `mutate.py [table] [id]` | client | the LWW round trip: a stale version loses even when it arrives last |
| `replies.py [table]` | client | every write gets a verdict: `accepted`, `stale`, `row_deleted` |
| `offline.py [table]` | client | outbox replay judged by version, not arrival; a stale replay cannot resurrect a row deleted while away |
| `tiebreak.py [table]` | client | equal versions resolved by the catalogue's tiebreak column, in either order; the id is stamped from the envelope, not `data` |
| `clamp.py [table]` | client | a future version is clamped to the database's clock and reported in wire format |
| `widthguard.py` | client | the row-width guard, both doors: oversized psql INSERT refused (23514), a fattening UPDATE refused atomically, the same edit as a real mutation answered `rejected` |
| `rowsize.py [table]` | client | an oversized row is a verdict for its sender, never a suspended table; the limit is published as `max_row_bytes` |
| `probe.py [table]` | client | a client can read the schema of a table it cannot write; the refusal is definitive and arrives exactly once |
| `reaps.py` | bridge | the sweeper's reaps never reach clients as CDC |
| `tenant_kv.py` | bridge | `$KV.tenants.<principal>`: each minted principal resolves its own tenant, cross-principal reads denied (exact-key grant), a live mapping change propagates with no restart, the mapping table never surfaces as CDC |
| `crosstenant.py [victim]` | client | cross-tenant reach by any primitive — expected to find the known hole |
| `dyntenant.py` | bridge | a tenant born at runtime: streams provisioned, mapping propagated, rows routed, zero restarts |
| `invalidate.py` | bridge | the caches that must notice DDL: KV schema, relation cache, refusal registry, write-path catalog cache |
| `keys.py [table]` | client | a database-allocated primary key is refused on the write path (`DbAllocatedKey`), never a later collision |
| `connbudget.py` | bridge | role connection limits exist and bite; `/enroll` bursts answer while `/metrics` still serves |
| `sweeper.py` | bridge | the tombstone GC boundary: tombstones either side of `GC_THRESHOLD_MS`, exactly the right ones reaped. Runs the sidecar SCOPED (`SWEEP_ONLY_TABLES`) on a throwaway fixture it `zebridge_enable`s and drops — never a real table's tombstones |

### owns (each starts its own bridge)

| script | asserts |
| --- | --- |
| `sizing.py` | `BASE_BUF`/ring sizing refusals: payload check, memory guard naming both terms, clamping vs defaulting, guard and allocator agree. Drops the probe slot and its `zebridge_limits` row |
| `endpoint.py` | one process, one NATS address |
| `credentials.py` | no admin fallback: no reader URL refuses to start, no writer URL turns ingress off |
| `downtime.py` | slot resume across kills: changes committed while the bridge is down still reach CDC |
| `decode_integrity.py [rows]` | zero-copy decode never aliases: every decoded value equals PostgreSQL's own |
| `genproducer.py` | generation chains: full, delta, prune — continuous, structurally checked |
| `legacybait.py` | pre-guard oversized rows: detector warns, quarantine fires, preflight re-derives, the recipe thaws |
| `livebirth.py` | a table born and `zebridge_enable`d while the bridge runs cascades completely with no restart |
| `race.py` | 24 writers against ingress, through a broker restart |
| `adversarial.py` | fuzz the two untrusted entry points (mutation payloads, `/enroll`) — survive, refuse once, no bad state |
| `chaos.py` | broker kill, backend kill, socket exhaustion — and `leaks` reports 0 on the survivor |

### manual (listed, never run by the battery)

| script | reports |
| --- | --- |
| `speed.py [stmts] [rows]` | the README's throughput method automated: seeds `users` through `zebridge_enable`, drives the burst, reads `/metrics` and the bridge's RSS. A number, not a verdict |
| `burst.py [table] [stmts] [rows]` | a burst large enough to build a backlog, and which LOOP numbers mean healthy |
| `leaksoak.py` | macOS `leaks` + RSS drift over a churn window (`SOAK_SECONDS`, `ZB_RSS_DRIFT_MB`) |
| `objstore_race.py` | a 40 MB get/put race against the object store |
| `tls.py` | `nats.zig` speaks TLS, JetStream included — optional: the colocated topology does not use it. Standalone, provisions its own `nats-server` |

## Prerequisites for the ingress scripts

The write path is off unless a role and a table are opened for it. The standard path is
one `zebridge_enable(...)` migration, which writes the table's `zebridge_catalogue` row
(LWW columns included) and adds it to the named publication — the bridge reads it live,
no env and no restart:

```sql
SELECT * FROM zebridge_enable('public.test_types',
    writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at',
    tiebreak_col => 'last_writer', tenant_col => 'tenant_id',
    publication => 'my_pub', dry_run => false);
```

Without writer credentials (`DATABASE_WRITER_URL`) the mutation listener does not start at
all — deliberately, rather than falling back to the read role, which has REPLICATION.
