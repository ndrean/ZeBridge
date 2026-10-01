# Test scenarios

What is tested, by which command, and what a pass proves. Current state only. This document is the evidence for the claims in the README. Every [test] link points to a file in scripts/scenarios/ unless named otherwise.

## The stack under test

The native stack (`scripts/native/up.sh`):

* PostgreSQL 18 on 127.0.0.1:5432,
* nats-server on 127.0.0.1:4222 in JWT/operator mode,
* the bridge on :27434.

Credentials are files: `scripts/native/creds/<principal>.creds` (`bridge`, `zbdoctor`, and the client principals `alice bob mary nina omar`, each mapped to a tenant in `zebridge_user_tenants`).
Configuration is the catalogue: `zebridge_enable(...)` writes `zebridge_catalogue`, and a running bridge reloads it live.
Seeding is generation chains (msgpack under zstd, OBJ `gen-<tenant>`, KV `generations`).

Three clients exist, and the same protocol is asserted through each:

| client | where | its suite |
| --- | --- | --- |
| `zb-client-ts` <br> (TS shell + pure core) | `zb-client-ts/` | `pnpm test` — the shared test cases in `fixtures/core-fixtures.json` |
| `libzb` (Zig, C ABI) | `libzb/` | `zig build test`; `python/runner.py` runs the SAME shared cases through the C ABI |
| `examples/05-tables/web-consumer` (vite, OPFS/PGlite) | `examples/05-tables/web-consumer/` | driven by hand; `window.zb` exposes the index card for scripted checks |

## How to run

```bash
# unit
zig build test                       # the bridge
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
# ⚠️ The dev NATS serves TLS on 4222 and the sourced env says tls://localhost:4222. The
# scenarios' clients get no CA, so point them at the plain URL (the server still accepts
# it), and give Node clients the test CA (they upgrade to TLS whenever it is offered):
#   export NATS_URL=nats://127.0.0.1:4222
#   export NODE_EXTRA_CA_CERTS=$PWD/nats.zig/tests/configs/certs/ca.pem
set -a && . ./.env.bridge && set +a
scripts/scenarios/run.py offline     # no stack needed
scripts/scenarios/run.py live        # against the running bridge
scripts/scenarios/run.py owns        # each starts its own bridge — stop yours first
scripts/scenarios/run.py --list      # groups, roles, one line each

# the Zig client against the live stack (each exits non-zero on failure)
(cd libzb && python3 python/index_card.py && python3 python/tail.py && python3 python/migrate.py && python3 python/live_enable.py --log <bridge log>)
```

Every scenario's exit code is its verdict. `run.py` sequences a group and fails if any
failed. The `manual` group is listed and never run by it (`run.py --list` names them),
for one of three reasons:

* **they measure, they do not judge**: benchmarks and soaks (`speed`, `burst_tls`,
  `firehose_tls`, `tls_cost`, `version_index`…) report rates, CPU and memory, and
  take minutes to hours. A number is read by a person against the last one, not
  passed or failed by a script;
* **they need what not every machine has**: PostGIS, pgvector, sqlite-vec, a libzb
  built with DuckDB, or a second database (`blobs`, `vectors`, `pgreplica`,
  `duckdb_replica`). They assert, and are run where those are installed;
* **they take over the stack**: they stop the dev bridge, run their own scratch
  PostgreSQL or bridge on the live slot (`cdc_wall`, `firehose_topology`), and are run
  alone, on purpose.

## What the tests prove, in plain words

Each row: something that could go wrong, what the test shows instead, and the test that shows it (in `scripts/scenarios/` unless named otherwise). The tables after this section give the same ground in more detail.

### Nothing is lost

| what could go wrong | what the test shows | test |
| --- | --- | --- |
| the bridge crashes, or is killed, while PostgreSQL keeps changing | every change made meanwhile arrives after the restart | `downtime.py` |
| the bridge is killed in the middle of a big transaction | the whole transaction is sent again; no row is lost, none is doubled | `txn_kill.py` |
| NATS is down for minutes | the bridge waits, confirms nothing to PostgreSQL, and delivers every row when NATS is back | `nats_outage.py` |
| PostgreSQL restarts under the bridge | the bridge reconnects and carries on from where it was; nothing is lost | `pg_restart.py` |
| NATS and PostgreSQL both go down, in either order, even with the bridge killed too | one bridge survives all four cases and loses nothing | `matrix.py` |
| the NATS stream is full and refuses messages | the bridge stops on its own instead of losing data; after the fix, a restart loses nothing | `stream_full.py` |
| a phone app is killed while it loads a table, twice | it reopens, finishes loading, and holds exactly what PostgreSQL holds | `client_kill.py` |
| the snapshot builder is killed half-way | no snapshot ever points to data that is not there | `chain_kill.py` |

### Nothing comes out wrong

| what could go wrong | what the test shows | test |
| --- | --- | --- |
| an old write, sent by a device that was offline, overwrites newer data | it is refused as `stale`; a write that really is newer still lands | `offline.py`, `mutate.py` |
| an old write brings back a deleted row | the row stays deleted | `offline.py` |
| an old write brings back a deleted row after the sweeper removed its tombstone | the bridge refuses the write (`PredatesGcWatermark`); without that guard the same write does bring the row back, which the test also shows | `gc_resurrect.py` |
| two writes carry the same version | the tiebreak column picks the same winner everywhere | `tiebreak.py` |
| a device's clock is fast or slow | a fast clock wins only until real time catches up; a slow clock can still write | `clockskew.py` |
| a device's clock is far in the future | the version is cut back to the server's time, and the answer says so | `clamp.py` |
| a write gets no answer, or blocks the ones behind it | every write gets an answer, and the write path never stalls | `replies.py`, `race.py` |
| a value changes on the way from PostgreSQL to a device | every value arrives identical, field by field, wide rows included | `decode_integrity.py` |
| two people edit the same row at once and one edit disappears | edits to different fields both survive; the same field ends with one winner | `crdt.py`, `route_crdt.py` |

### Schema changes

| what could go wrong | what the test shows | test |
| --- | --- | --- |
| a column is added, renamed or dropped while devices are connected | every replica follows, and the values are kept | `migrate_both.py` |
| a device is offline during an `ADD` / `DROP COLUMN`, with a write queued | it comes back with the new columns, the queued write is settled, and it equals PostgreSQL | `offline_migrate.py` |
| after a `DROP COLUMN`, writes to the table fail | writes keep working (this test found that they did not, and the fix) | `offline_migrate.py` |
| a device is offline while the table's key changes | it rebuilds the table, loads it again, and equals PostgreSQL | `rekey_offline.py` |
| queued writes meet a new schema (a dropped column, a new required column, a new key) | each one gets a clear answer, and the replicas equal PostgreSQL | `write_stale.py` |
| a column without a time zone makes "newer" ambiguous | the database refuses it when the table is created or altered | `tzguard.py` |
| one oversized row blocks a table for everyone | the row is refused, in PostgreSQL and from a device; only its sender gets the refusal | `widthguard.py`, `rowsize.py` |
| a table that breaks a rule stops everything | only that table is suspended, and it resumes by itself once fixed | `suspension_lift.py`, `legacybait.py` |

### Catching up after being away

| what could go wrong | what the test shows | test |
| --- | --- | --- |
| a device is away longer than the change stream keeps | it reloads the table from a snapshot and converges | `client_gap.py` |
| the change stream is deleted under a connected device | the bridge recreates it, the device starts again from a snapshot, and converges | `stream_wipe.py` |
| the replication slot is lost | the bridge refuses to start and says how to recover; after recovery every device reloads | `slot_loss.py` |
| rows deleted since the last snapshot come back on a device that reloads | a full snapshot is cut instead, so they do not | `genproducer.py` |

### Who can see and change what

| what could go wrong | what the test shows | test |
| --- | --- | --- |
| a device reaches another tenant's data through NATS | each way a client could try is attempted, and the outcome recorded | `crosstenant.py`, `inbox_sniff.py` |
| a device writes into another tenant | PostgreSQL refuses it; a user with no tenant can write nothing | `tenant_writes.py` |
| a device writes under someone else's name, or the bridge falls back to admin rights | neither is possible | `credentials.py` |
| hostile or malformed messages stall or crash the bridge | each one is refused; nothing stalls, nothing leaks | `adversarial.py` |
| a revoked user keeps access | their writes are refused at once; a full revocation also cuts their reads and ends their open session at once | `revoke.py`, `revoke_full.py` |
| a revoked user's data stays on their device | with `--purge`, the device deletes its replica and identity when it reconnects or renews its JWT; a plain revocation leaves them | `revoke_purge.py` |
| an expired JWT fails silently and the device retries forever | it fails with a clear, named error | `jwt_expiry.py` |
| a device in use is cut off when its JWT runs out, or its wrong clock stops it renewing | both libraries hold 40 s JWTs across 100 s and keep receiving; with a clock estimate 2 h ahead or 2 h behind (the JWT expired meanwhile), each renews and connects | `jwt_renew.py` |

### Under load

| what could go wrong | what the test shows | test |
| --- | --- | --- |
| sustained writes slowly degrade | 4 million writes at a steady 21,500 per second for three minutes | `stamp.py` |
| repeated disconnections leak memory or lose messages | memory, files and threads stay flat; no message lost or doubled | `churn.py` |
| NATS dies under a steady stream of changes | the bridge holds back and stops cleanly, then delivers everything when NATS returns | `cascade.py` |
| one client floods the write path and the others wait | its writes are served at the set rate, each answered once, none lost; a user of another tenant is answered within two seconds during the flood. Past the queue cap, its new writes are refused at the door (that phase runs only on a stack built with a small cap, `ZB_RATELIMIT_CAP_PHASE=1`) | `ratelimit.py` |

### Types, extensions and setups

"(by hand)" marks a test of the `manual` group: see [How to run](#how-to-run) for why.

| what could go wrong | what the test shows | test |
| --- | --- | --- |
| a PostGIS geometry or a `bytea` changes on the way | the bytes arrive exact, in the seed and over CDC, and a client's write lands as the same value (by hand: needs PostGIS) | `blobs.py` |
| a pgvector column is unusable on the device | it arrives as the BLOB sqlite-vec reads as is (`vec_distance_L2`), and a client's BLOB lands back as the same vector (by hand: needs pgvector and sqlite-vec) | `vectors.py` |
| arrays lose their shape | SQLite reads them as JSON, PGlite holds native arrays, and a client's arrays land as native arrays (by hand) | `arrays.py` |
| a PostgreSQL or DuckDB replica gets the types wrong | each builds its tables with its own types, and the seed, CDC and a client's write land natively (by hand: needs PostGIS, pgvector, a second database, a DuckDB build) | `pgreplica.py`, `duckdb_replica.py` |
| the link to NATS is encrypted in name only | a connect with the right CA does a JetStream round trip; the same connect without the CA fails on the certificate (by hand) | `tls.py` |
| TLS between the bridge and NATS slows it down | measured: the same event rate as plain TCP (by hand: a measurement) | `burst_tls.py` |
| reads moved to a standby break the bridge | with the reader on a hot standby and the writer on the primary, the slot lives on the standby, a client seeds and receives changes, and its writes land on the primary | `standby.py` |
| two bridges on one database step on each other | each publishes and snapshots only its own tables, neither touches the other's snapshots, one client follows and writes the tables of both, and a schema change is published once (this test found that each bridge deleted the other's snapshots, and the fix) | `multi_bridge.py` |

## The README's claims and their tests

Each claim of the README's feature list, the tests behind it, and how far they go. "Partial" says what is not shown.

| the README says | tests | how far |
| --- | --- | --- |
| clients reconnect without a stampede: a client that was away reloads from snapshots | `client_gap`, `stream_wipe`, `slot_loss` | each device reloads from a snapshot, not from the change feed |
| PostgreSQL never sees a reconnection storm | `client_gap`, `matrix`, `nats_outage` | partial: in these tests devices seed and catch up from NATS, and the bridge resumes from its slot; no test measures PostgreSQL's load while many devices reconnect at once |
| no retry loop for the common case: the library queues, retries and reports verdicts | `race`, `replies`, `offline` | every write gets one verdict, queued writes are sent on reconnection |
| PostgreSQL judges every write | `mutate`, `offline`, `tiebreak`, `clockskew`, `gc_resurrect` | stale writes refused, ties broken the same way everywhere, clocks bounded |
| data travels three ways (changes, snapshots, writes) | `stamp`, `churn`; `speed`, `burst` by hand | the rates are measurements, read by a person; `stamp` and `churn` judge that they hold steady |
| standby read replica | `standby` | the whole path: slot on the standby, seed, live change, write to the primary |
| `--revoke --purge` deletes the local replica | `revoke_purge` | both libraries, connected and on return, at reconnection and at renewal |
| multiple instances (possible, not recommended yet) | `multi_bridge` | two bridges, one database, one NATS |
| the JWT renews itself | `jwt_renew`, `revoke_purge` (F), `jwt_expiry`; real iPhone JWT renewal testing in `scripts/phone/renew` | both libraries keep working past two JWT lifetimes, and through a device clock 2 h off either way; a revoked device's renewal gets the purge answer; on an iPhone 12, libzb renews on the bridge's schedule with the clock moved by hand, open or closed |
| TLS in transit | `tls`, `burst_tls` | the certificate is checked; the cost is measured |
| schema changes reach every replica live | `migrate_both`, `invalidate`, `offline_migrate` | online and offline, both libraries |
| PostGIS and pgvector ready | `blobs`, `vectors`, `pgreplica`, `duckdb_replica` | byte-exact both ways, by hand |
| anti-client flood | `ratelimit` | the rate, always; the backlog cap only on a stack built with a small cap |

## In detail, by property

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
| the producer `kill -9`'d mid-build never leaves a manifest naming a missing object (objects first, manifest swapped last) | `chain_kill.py` |
| a full is forced when rows were deleted since the cutoff (no resurrection) | `genproducer.py`, bridge unit tests |
| writes committed while the bridge was down — and after a `kill -9` — replay from the slot | `downtime.py` |
| a bridge `kill -9`'d MID-delivery of one large transaction: the unacked transaction replays whole, no row lost, the replayed half deduped at the broker | `txn_kill.py` |
| a stream at `max_bytes` refusing publishes: retry budget burns, the bridge stops itself, the slot retains, a restart after repair loses nothing | `stream_full.py` |
| BASE_BUF lowered under stored data: the shrink-gated scan warns at boot, names the table, and stays silent on every non-shrinking boot | `shrink.py` |
| `bridge --diagnose` says everything the boot would decide and changes nothing: exit 0/1, init presence the headline, minimum BASE_BUF computed, shrink a finding | `diagnose.py` |
| two bridges on one slot: the loser refuses in its own words within seconds, no fight, no half-start — and `leaks` reads 0 bytes on the refusal path | `slot_contest.py` |
| a CDC stream deleted wholesale under a live client: deliberate stop, boot recreates, slot replays, client resets to the fresh numbering and converges | `stream_wipe.py` |
| PostgreSQL stopped and restarted under the bridge: refused connections waited out (connected=0 on /metrics), self-reconnect, durable slot, no loss — and `pg_ctl stop` completes in ~1 s, not wal_sender_timeout | `pg_restart.py` |
| the permutation matrix: NATS and PostgreSQL down together in both orders, restored in both orders, plus both down with the bridge killed on top — one process survives four double outages, the 3 a.m. case reboots from the slot | `matrix.py` |
| PostgreSQL restarts under a running sweeper: it warns and retries, and the reconnect re-arms the whole session (prepared statements, principal, UTC pin) — fresh ripe tombstones reaped after | `sweeper_restart.py` |
| the backpressure cascade, observable end to end: broker dies under a steady feed → queue climbs to ~86%, WAL dams behind the slot (~1 MB), bridge halts — then the broker returns and the same process drains it all, 1,199/1,199 rows | `cascade.py` |
| the CLIENT's host SIGKILLed mid-seed, twice: the torn SQLite file reopens, the seed re-applies idempotently — 120k rows, all distinct, equal to PostgreSQL | `client_kill.py` |
| `bridge --revoke` (ADMIN_DATABASE_URL, non-ambient): mapping + unused invites in one command, the three clocks narrated, KV purged by the live bridge, double-revoke distinguishable | `revoke.py` |
| `bridge --init-nats` generates the whole NATS stack (dev: open, 10 s; operator: full JWT, no nsc) — proven by BOOTING the generated conf and round-tripping JetStream on the generated creds | `init_nats.py` |
| the grammar is built in and served: /grammar byte-identical to src/grammar.json with its sha256 header, and a libzb client syncs from `grammarJson` alone — no file copied anywhere | `grammar_served.py` |
| a JWT with a tiny TTL: full invite-code bootstrap (jwt + grammar in one GET), an ordinary client inside the window, then the read door closes AUDIBLY as a named auth error — not a silent forever-retry | `jwt_expiry.py` |
| the HARD kill: `--revoke --conf` + OPERATOR_SEED rebuilds the revocations map from PG, re-signs the account JWT, splices the conf — on reload the live session is kicked and the dead token refused, in seconds | `revoke_full.py` |
| reconnect churn: 50 shuffled NATS bounces + 15 PG fast stop/starts with wasp-swarm writes — RSS/fd/threads flat against a warmup baseline, no sting lost or doubled, one client converges, and `bridge_nats_reconnects_total` equals the log's ground truth (library self-heals + fallback connections — sessions, not bounces; adjacent bounces merge honestly) | `churn.py` |
| clock skew under LWW: a 4 s-fast clock steals the row — audibly (`stale`) and only until the wall clock catches up; a 30 s-slow clock is starved writing from its wrist but writes through with §7.3's rule (libzb's `hlcVersion`); the feed's last word equals PostgreSQL's and no stale write leaves a trace | `clockskew.py` |
| the CRDT ladder's top rung: a jsonb map-of-LWW-registers — blind replace demonstrably loses an accepted intent; state-based merge with reconcile-to-fixed-point loses none of 18 concurrent keys, settles the contested one by its register tiebreak, and terminates | `crdt.py` |
| the capacity stamp: saturated and fault-free for 3 minutes — 4M mutations at a flat 21.5k/s (2 lanes), one consumer sustaining 10.4k rows/s downstream beside it; FAILs on a sagging bucket | `stamp.py` |
| a row written outside the client is in its replica under 10 ms; a 300-row transaction lands in one poll | `libzb/python/tail.py`, `bench_poll.py` (benchmark) |
| a pre-guard oversized row quarantines the table, boot re-derives it, removing the row lifts it | `legacybait.py` |
| a `row_too_large` suspension lifts LIVE once the table can be carried again — after a 30 s anti-flap cooldown — and the descriptor is republished; `zebridge_catalogue.suspended`/`suspended_reason` mirror both transitions for psql | `suspension_lift.py` |
| the broker gone for minutes: the bridge waits, ACKs nothing (`confirmed_flush_lsn` holds), the same process resumes, every row lands | `nats_outage.py` |
| the slot invalidated: boot refuses with the recovery; `ZB_FEED_RESTART=1` restarts the feed (streams, chains, manifests); a client's position beyond `last_seq` is a gap and it re-seeds from a fresh full | `slot_loss.py` |
| the client away past retention: the tail it needs is gone → gap → re-seed from the chain → converge | `client_gap.py` |
| a tenant-scoped table's SHARED (open-tenant) rows ride `CDC_PUBLIC`: a gap there re-seeds that table too, not just the public ones | `shared_gap.py` |

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
