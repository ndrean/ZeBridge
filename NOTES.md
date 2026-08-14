# ZeBridge — working notes

Accumulated findings, decisions, and open questions. Companion to
`COPY_BINARY_PLAN.md` (which holds the forward plan); this file holds *why things
are the way they are* and *what is still undecided*.

---

## 1. Open — decisions deferred, mostly for lack of information

### 1.1 Preflight validation — BUILT (`src/preflight.zig`)

ZeBridge imposes requirements on tables but enforces none of them at startup.
Two failures are currently silent:

- **No single-column PK** → snapshots fail *late*, at request time, per table
  (`getTablePrimaryKey` requires `array_length(i.indkey,1) = 1` and returns
  `error.NoPrimaryKey`). CDC still works, so the problem only shows up when a
  client tries to bootstrap.
- **`TRANSITION_RULES` on a `REPLICA IDENTITY DEFAULT` table** → the feature is
  inert forever, with no error (see 3.2).

Built: one catalog query at startup over `pg_publication_tables` joined to
`pg_class`, classified by a pure `classify(pk_columns, identity, has_rules)` that is
unit-tested without a database. Reports only — it does not refuse to start, since
these can be deliberate choices.

Severity is discriminated, which matters (mechanism in 3.3):

| table shape | verdict |
| --- | --- |
| no PK + DEFAULT/NOTHING | **error** — PostgreSQL rejects UPDATE/DELETE outright |
| no PK + FULL | warning — writes work (whole row is the identity), snapshots do not |
| composite PK | warning — snapshots need exactly one column |
| `TRANSITION_RULES` + not FULL | warning — transitions can never fire |

Verified against deliberately-broken tables: `t_nopk` (error + warning),
`t_nopk_full` (warning only — FULL rescued writes), `t_composite`, and `users` with
rules on a DEFAULT table.

The migration is the right place to *make* these decisions (the Elixir emitter
migration already sets `REPLICA IDENTITY` per table). Preflight is what makes a
wrong decision visible rather than invisible.

### 1.2 RENAME COLUMN forces a full client rebuild

The sharpest remaining gap. `ALTER TABLE ... RENAME COLUMN` is metadata-only in
Postgres — **measured at 10.2 ms**, no table rewrite. But the published schema goes
from `[... email ...]` to `[... email_address ...]`, and a column-set diff reads that
as one removed + one added, so the client rebuilds and **loses its entire local
replica** for a change that touched no data.

Cheapest possible server-side DDL, most expensive client response.

Postgres does distinguish it at the event-trigger level (the rename has its own
command tag), so capturing a `renamed_from` hint in `schema_def` would let a client
do a local `ALTER TABLE ... RENAME COLUMN` and keep its rows. Not built.

### 1.3 RING_BUFFER_COUNT sizing

The original rationale (in `config.zig`) was: 65536 slots ≈ 1092 ms at 60K events/s,
which *covered* one NATS reconnect interval when that was 1000 ms. Reconciling the
hardcoded 2000 ms into config broke that invariant — the buffer now covers about half
a reconnect.

Safe, not broken: a full ring backpressures the WAL reader, so events are delayed,
never dropped. Restoring the old property means `RING_BUFFER_COUNT=131072`
(≈2184 ms, ~4 MB slab) — ⚠️ a memory-doubling decision left

Metrics show PG reconnects dominate and NATS reconnects are rare.
On a *Postgres* loss the producer stops, so the ring **drains** rather than fills —
ring size does nothing for that failure mode. The slot and WAL retention cover it.
So this sizing debate only ever mattered for the rarer failure.

### 1.4 Unbounded `zebridge_ddl_events` — partially closed

Pruning is in (7-day window, run inside both triggers). Still open: whether 7 days is
right, given the table is only a human audit trail. The bridge never reads it — it
reads the WAL — so rows are disposable the moment they commit.

### 1.5 Snapshot / seed design

Sketched, not built. Intent: at most one snapshot per table per `SNAP_RET` (~10 min),
CDC retention longer (~15 min), client drives the sync dance.

Two concerns raised and unresolved:

- **KV cannot hold snapshot data.** JetStream KV values are capped (1 MB default) and
  are last-value-per-key. Chunks must stay in the INIT stream; KV should hold the
  *descriptor* (`{snapshot_id, lsn, timestamp, chunk_count, format}`).
- **The retention arithmetic fails silently.** Snapshot at LSN L with `SNAP_RET=10m`
  and `CDC_RET=15m` gives a client 5 minutes to seed. Exceed it and the CDC stream has
  pruned past L — the client diverges with no error. Needs an explicit invariant
  (`CDC_RET > SNAP_RET + max client apply time`) *and* a way for the client to detect
  the gap (compare seed LSN against the stream's oldest sequence) rather than
  quietly missing rows.

### 1.6 Web client uses core NATS, not JetStream consumers

`nc.subscribe('cdc.>')` receives only messages published while connected — no replay.
Fine for watching chaos live; it is the thing the seed design must solve, since a
client connecting after the fact gets schemas (KV keeps last value) but no history.

### 1.7 Deliberately not fixed

- **`cdc_events_published` undercounts.** The counter increments only in the mutation
  branches, so DDL/SCHEMA events are missing. I am ok: imprecise but rare
  enough not to matter.
- **pk lives inside the `sqlite` object** in the schema payload
  (`{"sqlite":{"columns":[...],"pk":"id"}}`) rather than at the root. Arguably wrong
  — pk is not SQLite-specific — but the browser client reads `val.sqlite.pk`, so
  changing it is a breaking wire change for no functional gain.

---

## 2. Bugs found — with the mechanism, so they are not reintroduced

### 2.1 Heterogeneous batches published under one subject  ← worst of the session

`batch_publisher.zig` built the subject from **the first event in the batch**:

```zig
const batch_subject = try std.fmt.allocPrint(flush_alloc, "{s}.batch",
    .{first_event.getSubject()});
```

...while `flushLoop` drains the SPSC queue indiscriminately, so one batch mixes
operations **and tables**.

Symptom that led to it: a DELETE "vanishing". It had not vanished — it was inside a
message published to `cdc.users.insert.batch`. Nothing was ever lost; each event
still carried its own correct `subject`/`table`/`operation` in the payload. It was
**misrouting**, which for a subject-filtering consumer is equivalent to loss. Worse
than the delete case: a batch spanning two tables published one table's rows under
the other's subject, breaking table isolation.

Fix: group by subject before publishing —
`StringHashMap(ArrayListUnmanaged(usize))` keyed on `event.getSubject()`, with a
separate array preserving **first-appearance order** so a causal sequence
(INSERT then DELETE of the same row) still reaches the stream in order for a
`cdc.>` consumer. Then one message per group, `publishSubjectGroup` doing the
existing single-vs-batch logic per group.

Two details that matter: the subject slices point into each slot's inline
`subject_buf`, which stays valid because slots are not reset until after publishing;
and the per-group `batch_msg_id` derives from that group's own first/last event, so
it stays deterministic and JetStream dedup still works on retry.

**Verified end to end** by decoding the actual stream bytes: `cdc.users.insert.batch`
contained exactly 2 INSERTs and no DELETE; `cdc.users.delete` carried
`{"id":1, name:null, …}` — PK populated, everything else null, as
`REPLICA IDENTITY DEFAULT` dictates.

*Method note:* checking `nats stream subjects` alone was **not** sufficient — that
reads the subject index (names and counts), not payloads. The bug was only truly
confirmed by base64-decoding the message data and walking the msgpack.

### 2.2 DDL schema was queried at processing time, not captured at the LSN

`packDdlToSlot` opened a Postgres connection and queried `information_schema` when it
*processed* the event — reading the catalog as of "now", not as of that LSN.

Two failure modes: a burst of ALTERs makes every event report the *latest* schema;
and on restart, replaying old WAL reports **today's** schema for a historical event.
That defeats the exact ordering guarantee the DDL design exists for.

Fix: the event trigger captures the schema as JSONB **inside the DDL transaction**,
where the catalog is correct, and the bridge just forwards it. Catalog state and LSN
are then bound atomically. Also removes a per-event connection on the WAL thread and
an `error.PgConnectionFailed` path that could fail mid-decode.

**Verified**: two ALTERs in one transaction produced rows with 5 and 6 columns
respectively — each its own point-in-time schema. Under the old code both would have
reported 6.

### 2.3 NATS credentials parsed away and dropped

`nats_publisher.zig` built `nats://user:pass@host`, split on `@` to find the host, and
**discarded the credentials** — never passing them to `JS.CONNECT`. The server
required auth, so the client hung with no error surfaced. Present in *both* `connect`
and `reconnect`. Same defect commit `141a00e` fixed in the snapshot listener, never
applied here. Also stopped logging `nats_url`, which embeds the password (those logs
ship to Loki).

### 2.4 Linux-only syscalls made the binary unrunnable on macOS

`Conn.newInbox` issued a raw Linux `getrandom`; Darwin has no such syscall number, so
the kernel killed the process with **SIGSYS** (exit 140) with no message. The macOS
crash report named it exactly.

**Not a bad vendoring decision.** Zig 0.16 gutted `std.posix` — `socket`, `connect`,
`fcntl`, `clock_gettime`, `nanosleep` are gone (networking moved to `std.Io`).
Upstream g41797/nats 0.0.3 uses `posix.fcntl`, which does not compile on 0.16, so the
patch reached for `std.os.linux` to make it build. The only misstep was raw syscalls
over libc, which silently made everything Linux-only — invisible until the first
native run.

Fix: 16 sites `std.os.linux.*` → `std.c.*` (switches on `native_os`; libc already
linked). Entropy needed a comptime split — **no single libc entry covers both
targets**: Darwin has `arc4random_buf` but not `getrandom`; musl has `getrandom` but
not `arc4random_buf`.

### 2.5 `fetchPut` no longer overwrites the key (Zig 0.16)

`schema_cache.hasChanged` freed `old.key` after `fetchPut`. In 0.16 `fetchPut` sets
only `gop.value_ptr.*` and keeps the map's **existing** key pointer — so this was a
**use-after-free plus a leak** (the new key was never stored). Only fires on an actual
schema change, which is why it survived. Rewritten on `getOrPut`.

Found only because 38 of 51 tests were never running (see 4.1).

### 2.6 Environment strings duped for no reason

0.15's `getEnvVarOwned()` returned owned memory; 0.16's `getPosix()` **borrows** from
the environ block, which outlives the process. The call sites were migrated, the
ownership model was not — the code kept duping and freeing. The tell: NATS vars in the
*same function* were already assigned directly. Removing the dupes let `parseArgs`
drop its allocator entirely and deleted `RuntimeConfig.deinit`, along with its
fragile compare-pointers-against-defaults logic.

### 2.7 Boot schema path published internal tables

`publishBootSchemas` iterates the **publication's** table list, which necessarily
contains `zebridge_ddl_events` (its INSERTs are how schema events travel). The DDL
triggers excluded it; the boot path did not. Clients built a local replica of our own
bookkeeping. Fixed with an `isInternalTable` mirror of the SQL helper.

### 2.8 Pruning would have caused a delete storm

`zebridge_ddl_events` is in the publication and the bridge special-cased it **only in
the `.insert` branch** — so `UPDATE`/`DELETE` on it flowed through as ordinary CDC.
Adding pruning without fixing that would have published
`cdc.zebridge_ddl_events.delete` to every client on each DDL, turning a slow disk leak
into active noise. Both halves fixed together.

---

## 3. PostgreSQL behaviours worth remembering

### 3.1 `ALTER TABLE` serialises against writers — demonstrated

`ADD COLUMN` takes **ACCESS EXCLUSIVE**. Measured wait chain:

```
3047  Timeout/PgSleep  blocked_by=       BEGIN; INSERT …   RowExclusiveLock     granted=true
3053  Lock/relation    blocked_by=3047   ALTER TABLE …     AccessExclusiveLock  granted=false
3066  Lock/relation    blocked_by=3053   INSERT …          RowExclusiveLock     granted=false
```

Everything released within 14 ms of the first transaction committing, strictly in
order (A → ALTER → C).

**The subtle part is C.** Two concurrent INSERTs never conflict — `RowExclusiveLock`
is self-compatible — yet C was blocked by the *ALTER*, not by A. Postgres queues lock
requests, so once an `AccessExclusiveLock` is waiting, everything behind it waits too.
A 13 ms metadata-only migration stalled an unrelated writer for **8 seconds** purely
because one slow transaction held the table.

▶️ Practical consequence: set `lock_timeout` on migrations — not to protect the
migration, but to stop it becoming a queue head that blocks every emitter.

Consequence for CDC: **there is no such thing as an old-shape row arriving after the
DDL event.** A writer that omits the new column produces a valid *new*-schema row
taking the default. Verified in the WAL: `INSERT id=6` (pre-ALTER) → DDL event →
`INSERT id=7` (post-ALTER).

### 3.2 REPLICA IDENTITY governs what UPDATE/DELETE carry

Measured on the same table, same update:

| `relreplident` | UPDATE payload |
| --- | --- |
| `d` (default) | `[demo_col, email, id, inserted_at, name, updated_at]` — **no `old.*`** |
| `f` (full) | adds `old.id`, `old.name`, `old.email_address`, … |

pgoutput emits an old tuple only for FULL (always, `'O'`), or for DEFAULT **when the
key changed** (`'K'`, key columns only). Updating a non-key column on a DEFAULT table
sends no old tuple at all.

Since the entire transition-detection block in `packMutationToSlot` lives inside
`if (old_tuple_data) |old_data|`, `is_transition` can never become true on a DEFAULT
table. Routing then always picks `.data`, never `.transition` — a consumer on
`cdc.<table>.update.transition` receives silence forever, with no error.

DELETE on DEFAULT carries the PK populated and everything else `null` — enough for
delete-by-key, which is all a replica needs.

The trade is real (FULL multiplies WAL volume on wide tables), so the decision is
**per-table**, made in the migration. See 1.1.

### 3.3 No primary key: FULL rescues CDC, but not snapshots

A second, separate consequence of REPLICA IDENTITY — and the one that drove the
severity split in `preflight.zig` (1.1). Measured on PostgreSQL 18:

```
no PK + DEFAULT →  ERROR: cannot update table "nopk_default" because it does not
                          have a replica identity and publishes updates
                   HINT:  To enable updating the table, set REPLICA IDENTITY using
                          ALTER TABLE.
no PK + FULL    →  works
```

The error comes from **PostgreSQL, not the bridge**, and it fires on the write
itself. A table in a publication with no replica identity cannot publish updates, so
UPDATE/DELETE are refused at the source — the bridge never sees them, and no amount
of downstream handling helps. (INSERTs are unaffected: they need no identity.)

`REPLICA IDENTITY FULL` fixes this because the entire row becomes the identity, so
there is always something to identify the old tuple by.

| PK | identity | UPDATE/DELETE | snapshots |
| --- | --- | --- | --- |
| single column | DEFAULT | ✅ key-only old tuple | ✅ |
| single column | FULL | ✅ full old tuple | ✅ |
| none | DEFAULT / NOTHING | ❌ **PostgreSQL rejects the write** | ❌ |
| none | FULL | ✅ | ❌ |
| composite | either | ✅ | ❌ |

**The asymmetry is the point: FULL rescues CDC, it does not rescue snapshots.**
Snapshot chunking uses keyset pagination (`WHERE pk > $last ORDER BY pk LIMIT n`),
which needs exactly one ordered column — `getTablePrimaryKey` enforces
`array_length(indkey,1) = 1` and returns `error.NoPrimaryKey` otherwise. No replica
identity setting can substitute for that, because the constraint is about *ordering
for pagination*, not about *identifying a row*.

Hence preflight treats no-PK-plus-DEFAULT as an **error** (upstream writes are
broken) but no-PK-plus-FULL as a **warning** (CDC works, bootstrapping does not).
Same missing PK, genuinely different severity.

### 3.4 Event triggers

- `ddl_command_end` **does not fire for DROP TABLE** — the object is already gone and
  `pg_event_trigger_ddl_commands()` does not report it. Drops need a separate trigger
  on `sql_drop` using `pg_event_trigger_dropped_objects()`.
- `object_identity` is `public.users` for a table but `public.users.email` for a
  column → normalise with `split_part(..., 2)`.
- `ALTER TABLE ADD COLUMN` emits **both** a `'table'` row and a `'table column'` row →
  dedupe, or the same schema publishes twice.
- Use `to_regclass(...)` rather than `::regclass` — it returns NULL instead of raising
  if the table is gone later in the same transaction.
- **`CREATE INDEX` produces no schema event** (`object_type = 'index'`), which is
  correct: indexes are a local performance decision, not something a replica mirrors.
  Verified — the event count stayed at 6.

### 3.5 Index creation locks

❇️ `CREATE INDEX` takes a **SHARE** lock: blocks writes, allows reads — a long hold on a
big table. `CREATE INDEX CONCURRENTLY` does not block writes but **cannot run inside a
transaction**, so Ecto needs `@disable_ddl_transaction true` (and
`@disable_migration_lock true`). It does two table scans and can leave an INVALID
index on failure, requiring a manual drop.

### 3.6 Postgres 18 packaging

❇️The image expects the volume at `/var/lib/postgresql`, **not** `/var/lib/postgresql/data`
— it stores data in a version-specific subdirectory so `pg_upgrade --link` works.
`docker-compose.full.yml` had the old path and failed to start.

`information_schema` reports `NUMERIC(20,8)` as plain `"numeric"` — the key the type
mapper looks up.

---

## 4. Tooling and method

### 4.1 38 of 51 tests were never running

Zig only collects tests from files the root actually references. `bridge.zig`
force-discovered only `pg_copy_csv.zig`, so eight modules' tests — including
`encoder.zig`, which was being edited at the time — silently never ran. Adding them
surfaced 2.5 immediately.

Also: the `bridge` module lacked `msgpack`/`nats` imports, so `mod_tests` could not
compile anything using them; only `exe_tests` worked.

**Any module with `test` blocks must be listed in the `comptime` block in
`bridge.zig`, or its tests do not exist.**

### 4.2 The vendored NATS library is the only 0.16 port that exists

The upstream clone (`/Users/nevendrean/code/zig/nats`) targets Zig 0.15.2 and does not
build on 0.16 (`zul` uses a removed `@Type` builtin; `std.Thread.Mutex` and `std.net`
are gone). So its test suite had to be ported **into** `src/nats_vendor/`, not the
reverse — running it upstream under 0.15 would validate different code than ships.

8 of 9 test files needed no changes (they exercise the API, not std internals); only
`net_tests.zig` depends on `std.net` and is still unported. Now `zig build test-nats`,
**33/33 passing on both nats 2.11.17 and 2.14.4** — so the NATS server version is not
a friction point; the Zig version was.

Bringing the tests in immediately exposed API drift the vendoring had introduced,
including `try Conn.newInbox()` in `Subscriber.zig` — a genuine compile error in dead
code that Zig's lazy analysis had hidden because the bridge never called it.

### 4.3 Verification lessons

- **Metadata is not payload.** `nats stream subjects` shows names and counts; only
  decoding the message bytes proves content. See 2.1.
- **Beware the tool, not just the code.** A `grep -E` with alternations silently
  returned nothing in this shell, which led to a confident and wrong diagnosis that
  "DDL processing broke". `awk` showed every event had processed correctly. When
  output contradicts something observed moments earlier, suspect the tool first.
- **Check for stale processes.** A zombie bridge from an earlier run held port 9090 and
  answered `/status` as "disconnected" while the real bridge worked fine — briefly
  mistaken for a metrics bug.
- **A host Postgres shadows the container.** It binds `[::1]:5432` and
  `127.0.0.1:5432`; those specific bindings beat Docker's wildcard, so `localhost`
  reaches the *host* server (→ confusing "role bridge_reader does not exist"). Hence
  `PG_PUBLISH_PORT` and using `127.0.0.1` explicitly.
- The `nats:latest` image is **distroless** — no `sh`, no `wget`, so `CMD-SHELL`
  healthchecks can never pass.

---

## 5. Client-side protocol (browser / WASM SQLite)

The bridge publishes schema-before-row, but KV and CDC are **independent
subscriptions** in the client, so the row can still win locally. Observed live:

```
17:37:16  CDC HOLD      unknown column(s) [demo_col] — awaiting schema newer than lsn 24919976
17:37:16  SCHEMA MIGRATE users: +[demo_col] via ALTER (rows preserved), lsn=25429824
17:37:16  CDC DRAIN     Replaying 1 held event(s) for users
```

The protocol that makes the guarantee hold end to end:

1. Row arrives → does the local schema know all its columns?
2. If no → **hold** it (do not drop, do not write partially).
3. Schema arrives → migrate → **drain** the held events.

**Gate on the column set, not on the LSN.** The LSN tells you *where you are*; the
column set tells you *whether you can proceed*. Comparing `ev.lsn > schema.lsn` would
stall during quiet periods, since most rows legitimately have LSNs newer than the last
DDL. The `lsn` field earns its place as the ordering key and staleness marker, not as
the gate.

The two directions are **not symmetric**: a new-shape row needs the new schema, but an
old-shape row applies cleanly to a new schema (missing columns take NULL). Only one
direction needs handling.

Migrations should be additive where possible (`ALTER TABLE ADD COLUMN`) so rows
survive — otherwise "did the client survive?" is trivially true because the replica
was rebuilt empty, hiding any real loss.

---

## 6. Reproducing the tests

```bash
PG_PUBLISH_PORT=55432 docker compose -f docker-compose.full.yml --env-file .env.prod \
  up -d postgres-primary nats-config-gen nats-server nats-init bridge-init

cd emitter && mix run --no-start -e 'path = Path.join([File.cwd!(), "priv","repo","migrations"]); \
  {:ok,_,_} = Ecto.Migrator.with_repo(Emitter.Producer.Repo, fn r -> Ecto.Migrator.run(r, path, :up, all: true) end)'

set -a && source .env && set +a
./zig-out/bin/bridge --slot my_slot --pub my_pub --port 9090

cd web-consumer && npm run dev     # http://localhost:5173
```

Then from IEx in `emitter/`:

```elixir
Emitter.Produce.stream(50, 200, 5)        # background load
Emitter.Chaos.trigger_schema_evolution()  # ALTER + dependent row, one txn
Emitter.Chaos.trigger_table_drop()        # CREATE, insert, DROP → tombstone
Emitter.Chaos.ddl_events()                # what the triggers recorded
```

The NATS library's own suite needs a **no-auth** server on 4222:

```bash
docker stop nats-server
docker run -d --rm --name nats-test -p 4222:4222 -p 8222:8222 nats:2.11 -js -m 8222
zig build test-nats
```
