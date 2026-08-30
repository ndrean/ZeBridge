# Test scenarios

What is tested, by which command, and what a pass proves. Current state only — the
history of how each scenario was found is in NOTES.md.

## The stack under test

The native stack (`scripts/native/up.sh`): PostgreSQL 18 on 127.0.0.1:5432, nats-server
on 127.0.0.1:4222 in JWT/operator mode, the bridge on :9090. Credentials are files:
`scripts/native/creds/<principal>.creds` (`bridge`, `zbdoctor`, and the client
principals `alice bob mary nina omar`, each mapped to a tenant in
`zebridge_user_tenants`). Configuration is the catalogue: `zebridge_enable(...)` writes
`zebridge_catalogue`, and a running bridge reloads it live (NOTES §10bj). Seeding is
generation chains (msgpack under zstd, OBJ `gen-<tenant>`, KV `generations`).

Three clients exist, and the same protocol is asserted through each:

| client | where | its suite |
| --- | --- | --- |
| `zb-client-ts` (TS shell + pure core) | `zb-client-ts/` | `pnpm test` — 127 fixture cases in `fixtures/core-fixtures.json` |
| `libzb` (Zig, C ABI) | `libzb/` | `zig build test`; `python/runner.py` runs the SAME 127 fixtures through the C ABI |
| `web-consumer` (vite, OPFS/PGlite) | `web-consumer/` | driven by hand; `window.zb` exposes the index card for scripted checks |

## How to run

```bash
# unit
zig build test                       # the bridge (264)
(cd libzb && zig build test)         # the Zig client (+1 live test, ZB_LIVE=1)
(cd nats.zig && zig build test)      # the vendored NATS client (e2e needs docker)
(cd zb-client-ts && pnpm test)       # the TS core, fixture-pinned

# conformance of the Zig core against the TS fixtures
(cd libzb && zig build && python3 python/runner.py)

# the database, after a migration, with no bridge running
psql -c "SELECT * FROM zebridge_check('orders', 'writable')"
psql -c "SELECT * FROM zebridge_check_all('{\"users\":\"read_only\",\"orders\":{\"mode\":\"writable\",\"version\":\"updated_at\",\"tombstone\":\"deleted_at\",\"tiebreak\":\"last_writer\",\"tenant\":\"tenant_id\"}}') WHERE status = 'ERROR'"
scripts/zbdoctor.py --intent intent.json      # the same, plus the live bridge/NATS gates

# the scenarios (scripts/scenarios/README.md for setup)
set -a && . ./.env.bridge && set +a
scripts/scenarios/run.py offline     # no stack needed
scripts/scenarios/run.py live        # against the running bridge
scripts/scenarios/run.py owns        # each starts its own bridge — stop yours first
scripts/scenarios/run.py --list      # groups, roles, one line each

# the Zig client against the live stack (each exits non-zero on failure)
(cd libzb && python3 python/index_card.py && python3 python/tail.py && python3 python/migrate.py && python3 python/live_enable.py --log <bridge log>)
```

Every scenario's exit code is its verdict. `run.py` sequences a group and fails if any
failed; the `manual` group (`speed`, `burst`, `leaksoak`, `objstore_race`, `tls`) is
listed and never run by it — those report, they do not assert.

## What is asserted, by property

### Schema and CDC

| property | asserted by |
| --- | --- |
| a `CREATE TABLE` + `zebridge_enable` while the bridge runs routes without a restart: rules reloaded, CDC_PUBLIC's filter reconciled, refusal lifted, schema published, `writable` reflects the grants | `livebirth.py`, `libzb/python/live_enable.py` |
| a table taken out of the catalogue is refused live, its clients get a suspension | `live_enable.py` |
| ADD / DROP / RENAME COLUMN reach a running client as an ALTER, the value survives a rename hint, an FK change forces a rebuild that copies the rows | `libzb/python/migrate.py`, libzb unit test `migrateTable` |
| the four caches notice DDL: KV schema, the write path's catalog cache, the relation map, the refusal registry; a dropped table's refusal does not linger | `invalidate.py` |
| a naive `timestamp` column is refused at DDL time | `tzguard.py` |
| a soft delete arrives as an UPDATE with the tombstone set; the sweeper's physical reap never reaches a client; a tombstone-less table still emits deletes | `reaps.py`, `sweeper.py` |
| the zero-copy WAL decode never aliases stale bytes: every CDC value equals PostgreSQL's, field by field, wide rows included | `decode_integrity.py` |

### Bootstrap, resume, gap — generation chains

| property | asserted by |
| --- | --- |
| a chain exists (full + deltas), continues across deltas, prunes, and a client walks it to the same row count as PostgreSQL | `genproducer.py`, `libzb/python/index_card.py` |
| a full is forced when rows were deleted since the cutoff (no resurrection) | `genproducer.py`, bridge unit tests |
| writes committed while the bridge was down — and after a `kill -9` — replay from the slot | `downtime.py` |
| a row written outside the client is in its replica in single-digit ms; a 300-row transaction lands in one poll | `libzb/python/tail.py`, `bench_poll.py` (benchmark) |
| a pre-guard oversized row quarantines the table, boot re-derives it, removing the row lifts it | `legacybait.py` |
| the broker gone for minutes: the bridge waits, ACKs nothing (`confirmed_flush_lsn` holds), the same process resumes, every row lands | `nats_outage.py` |
| the slot invalidated: boot refuses with the recovery; `ZB_FEED_RESTART=1` restarts the feed (streams, chains, manifests); a client's position beyond `last_seq` is a gap and it re-seeds from a fresh full | `slot_loss.py` |
| the client away past retention: the tail it needs is gone → gap → re-seed from the chain → converge | `client_gap.py` |

### The write path (PROTOCOL §7)

| property | asserted by |
| --- | --- |
| every write gets a definitive verdict: accepted / stale / row_deleted / rejected; a pipelined connection never wedges | `replies.py`, `race.py` |
| last-write-wins by version, never by arrival; an outbox replayed out of order converges; no resurrection; dedup by msg_id | `mutate.py`, `offline.py` |
| equal versions resolved by the tiebreak column, order-independently | `tiebreak.py` |
| a far-future version is clamped to the server clock and reported | `clamp.py` |
| an oversized row costs its sender a verdict, not everyone the table; the width guard refuses in PostgreSQL and at the edge alike | `rowsize.py`, `widthguard.py` |
| a database-allocated key refuses edge writes, scoped to the write path | `keys.py` |
| the write guards stamp a forgotten version and turn a psql DELETE into a tombstone | `guards.py` |
| omission stamps the writer's own tenant; forgery is refused by RLS; unmapped principals fail closed | `tenant_writes.py` |
| a table you can read the schema of but not write refuses exactly once | `probe.py` |
| the `INSERT` grant, the published `writable`, and a refused write's verdict agree | `writable.py` |

### Isolation and identity

| property | asserted by |
| --- | --- |
| the bridge cannot fall back to admin credentials; the mutation principal is broker-enforced; an illegal principal token never lands | `credentials.py` |
| cross-tenant reach by any primitive (core SUB, JS consumer, stream info, OBJ chain objects) — the file records the known hole deliberately | `crosstenant.py` |
| `$KV.tenants` exact-key grants; live propagation from `zebridge_user_tenants`; the roster never appears on a CDC stream | `tenant_kv.py`, `dyntenant.py` |
| the two untrusted-byte entry points (mutation envelopes, `/enroll`) refuse, never wedge, never leak | `adversarial.py` |

### Operations

| property | asserted by |
| --- | --- |
| the HTTP surface: `/health` `/status` `/metrics` `/enroll`, slowloris-resistant | `telemetry.py`, `connbudget.py` |
| `BASE_BUF` / ring sizing refusals and clamps, the allocator agreeing with the startup check | `sizing.py` |
| one NATS address, never a second one from a stale env | `endpoint.py` |
| broker kill/restart, PG backend kill, socket exhaustion — the bridge survives each | `chaos.py` |
| declared vs actual drift: catalogue, publication, streams, slots (grants live in JWTs and are skipped loudly) | `check.py`, `zbdoctor.py` |
| both SQL templates render and apply with nothing lost; the publication is named, never guessed | `render.py`, `pubname.py` |
| the two env files agree with the native stack | `envcheck.py` |

## Isolation rules

`owns` scenarios start a probe bridge (`--slot zb_probe --port 9096`) and refuse to
run beside another bridge; `chaos.py` restarts the broker; `sweeper.py` scopes the
sweeper to its fixture (`SWEEP_ONLY_TABLES`). Run `owns` alone and sequentially —
`run.py owns` does. `speed.py` truncates `users`; it is a benchmark, run it by hand on a
disposable database.
