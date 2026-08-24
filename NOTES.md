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
| `TRANSITION_RULES` + not FULL | warning — transitions can never fire |

Verified against deliberately-broken tables: `t_nopk` (error + warning),
`t_nopk_full` (warning only — FULL rescued writes), and `users` with rules on a
DEFAULT table. `t_composite` used to warn here; since pagination compares the whole
key as a row value, a composite key is no longer a finding at all.

The migration is the right place to *make* these decisions (the Elixir emitter
migration already sets `REPLICA IDENTITY` per table). Preflight is what makes a
wrong decision visible rather than invisible.

### 1.2 RENAME COLUMN silently nulls the renamed column, client-wide

The sharpest remaining gap. `ALTER TABLE ... RENAME COLUMN` is metadata-only in
Postgres — **measured at 10.2 ms**, no table rewrite. The published schema goes
from `[... email ...]` to `[... email_address ...]`, and a column-set diff reads
that as one removed + one added — but checked against `applySchema` (`App.tsx`)
directly, the damage is narrower than "loses its entire local replica":

- The common case is `ALTER TABLE ... DROP COLUMN "email"` followed by
  `ALTER TABLE ... ADD COLUMN "email_address"` — both succeed (SQLite has
  supported `DROP COLUMN` since 3.35), so **the table and every other column and
  row survive**. What's actually lost is narrower: the renamed column's own
  **values**, reset to NULL for every row that existed before the rename.
- Only if the renamed column is part of a PK/unique index does SQLite refuse the
  `DROP COLUMN`, falling back to `rebuildPreservingData` — which still copies
  every column the two schemas share **by name**, so again only the renamed
  column's data is lost, not the table.

Still real, silent data loss — a client offering to edit `email_address` sees
nothing where a value used to be, for every row until some later CDC event
happens to touch it — just not the scale the original wording claimed. Cheapest
possible server-side DDL, a real but smaller client cost than described.

⚠️ **Not a Postgres limitation, and not about sniffing the DDL command.** The DDL
capture query (`init.core.template.sql`, the `columns` JSONB in the event
trigger) joins `information_schema.columns` to `pg_attribute` **by name**
(`a.attname = c.column_name`) and publishes `name`/`type`/`nullable`/
`has_default`/`required`/`oid`/`typtype` — never `attnum`. But `attnum` is
exactly what survives a rename: Postgres renames a column by updating
`pg_attribute.attname` alone, never `attnum` — which is *why* the rename is
metadata-only in the first place. Add `'attnum', a.attnum` to that same
`jsonb_build_object`, and a rename becomes a plain diff — same `attnum`,
different `name`, between this `zebridge_ddl_events` row and the table's
previous one — with no need to inspect `ALTER TABLE`'s command tag at all (which
doesn't cleanly itemize which sub-clause fired). That diff, published as a
`renamed_from` hint in `schema_def`, is what would let a client do a local
`ALTER TABLE ... RENAME COLUMN` and keep its rows. Not built.

⚠️ **Not a storage-engine limitation on either client.** SQLite has supported
`ALTER TABLE ... RENAME COLUMN` natively since 3.25 (2018) — the same primitive
PGlite has, since it's real Postgres. The gap is entirely upstream, in capturing
and forwarding the rename; nothing about the client's own storage blocks doing
this properly once that exists.

### 1.3 CLOSED — RING_BUFFER_COUNT settled at 65536

The original rationale (in `config.zig`) was: 65536 slots ≈ 1092 ms at 60K events/s,
which *covered* one NATS reconnect interval when that was 1000 ms. Reconciling the
hardcoded 2000 ms into config broke that invariant — the buffer now covers about half
a reconnect.

Safe, not broken: a full ring backpressures the WAL reader, so events are delayed,
never dropped. Restoring the old property would mean `RING_BUFFER_COUNT=131072`
(≈2184 ms), doubling memory.

**Settled: leave it at 65536.** Metrics show PG reconnects dominate and NATS
reconnects are rare — and on a *Postgres* loss the producer stops, so the ring
**drains** rather than fills; ring size does nothing for that failure mode, the
slot and WAL retention cover it. So the sizing only ever mattered for the rarer
failure, and the cost of doubling is real: 65536 × 4096 B (`BASE_BUF=12`) ≈
256 MiB already, before per-slot overhead (subject buffer, metadata) — around
300 MB, not a small number to double for a failure mode that already has
~2 seconds of headroom against a genuinely solid 30K evt/s.

### 1.4 CLOSED — `zebridge_ddl_events` pruning is disk hygiene, not correctness

Pruning is in (2-day window, run inline inside both DDL-trigger call sites —
`zebridge_prune_ddl_events()`, not a separate scheduled process; `bridge_sweeper`
never touches this table, it reaps tombstoned rows in `SYNC_RULES`-listed user
tables, a different table set and a different problem).

**Why it needs pruning at all, confirmed by checking every call site:** nothing in
the bridge ever `SELECT`s this table. The bridge only ever consumes it by decoding
its INSERTs off the WAL in real time (`event_processor.zig`), and a reconnecting
client gets the *current* schema from `$KV.schemas` (last-value KV) — neither path
touches the table's own rows. Logical decoding replays from the WAL itself on a
bridge restart too, not by re-reading the table, so a pruned row affects nothing
functional either way. The table exists only so the DDL event trigger has
somewhere to atomically write the schema JSONB inside the same transaction as the
DDL (§2.2) — once that row has been WAL-decoded, it is pure disk cost with zero
remaining purpose.

So the 2-day window is arbitrary, not a correctness parameter — it could be
shorter or longer with no risk either way. Not worth further attention.

### 1.5 CLOSED — snapshot / seed design

Was sketched, not built, with two concerns raised and unresolved. Both are now
built, and match the sketch exactly:

- **"KV cannot hold snapshot data."** Shipped design: `$KV.snapshots` holds the
  descriptor (`{snapshot_id, lsn, chunk_count, ...}`), chunks stay in the INIT
  stream (`init.snap.<tenant>.<table>.<snapshot_id>.<chunk>`) — the split this
  concern asked for.
- **"The retention arithmetic fails silently... needs a way for the client to
  detect the gap."** Built as `descriptorStillFresh()` (§1.6a): before trusting a
  cached descriptor, the client compares its LSN against the CDC stream's
  oldest-available LSN and discards it if the gap is unrecoverable, rather than
  seeding from a descriptor the stream can no longer back up. Found live and
  fixed the same way §1.6a describes — including the case this section's own
  arithmetic warns about (a cached descriptor outliving what the stream can still
  cover).

### 1.6 CLOSED — web client uses real JetStream consumers

`subscribeStreams()` (`App.tsx`) opens a JetStream `consumer.consume()` per stream
with `deliver_policy: last > 0 ? StartSequence : All, opt_start_seq: last > 0 ? last
+ 1 : undefined`. A client connecting after the fact replays from its own persisted
`_zebridge_stream_seq`, not just live traffic. The same call runs again on
reconnect (§1.6a below), so a dropped connection resumes rather than silently
missing events.

Stale-descriptor handling is covered separately (§1.12, "A cached descriptor has
no expiry of its own"). One gap that check still cannot see: it catches the CDC
stream aging out from under a descriptor, not a policy change (§2.18's RLS fix)
that makes the *rows themselves* wrong without moving any LSN — those six
descriptors had to be purged by hand.

#### 1.6b Reconnect now re-syncs CDC, not just the outbox

Reconnecting used to call `flushOutbox()` only; `subscribeStreams()` ran once, at
mount. A client that reconnected after a real outage kept the outbox promise
(pending writes got resent) but never resumed consuming — other clients' changes
made during the outage stayed invisible until a manual page reload.

Fixed: reconnect now re-runs `subscribeStreams()` as well, which reuses its own
gap-check (persisted seq vs. stream retention) to resume correctly rather than
resubscribing blind. Driven by two independent signals, since `nc.status()` alone
did not reliably report every outage shape: its own `reconnect` event, and an
active `nc.rtt()` poll every 10s (mirroring the existing `/bridge/health` poll).
Both share one `resyncing` guard so they cannot double-fire. Verified live:
freezing `nats-server` disconnects every client with no cherry-picking, and on
resume all clients — not just the one that sent a write — catch up without a
reload.

#### 1.6c Table suspension now freezes client writes too, not just reads

A suspended table (`src/refused_tables.zig` — no PK, an undecodable column type, a
row too large for `BASE_BUF`, or `TENANT_RULES` naming a missing/misplaced
column) already reached the client as a signal: `App.tsx`'s schema watch sets
`suspended()` off the `{"suspended":true,"reason":…}` payload and shows a banner
saying local rows are "frozen." The banner text was wrong — nothing gated
`sendMutation`, so a click on a suspended table still queued the outbox entry and
optimistically applied the write.

That is the worst shape of the §1.9 gap (a permanently-rejected write with no
echo to correct it): suspension drops every CDC event for the table at the
source, so the optimistic guess would never be confirmed *or* corrected — not
until an operator fixes the shape, which could be indefinite, and it would affect
every write to that table, not one bad payload.

Fixed: `sendMutation` checks `suspended()[table]` first and refuses client-side —
no outbox entry, no optimistic apply, just a logged refusal. The banner text now
says writes are refused, not just reads frozen.

**Verified this is table quarantine's only trigger — a bad payload cannot cause
it.** `refused_tables.refuse()` is called from exactly two places,
`event_processor.zig` and `snapshot_listener.zig`, both reacting to a table's
*shape* from the DDL/CDC/snapshot path. `mutation_listener.zig` — the per-write
path — never calls it. A write PostgreSQL refuses (bad constraint, wrong type, a
client bug that will fail identically every retry) ends at
`publishVerdict(..., "rejected", …)` on `mutation_ack.<principal>.<msg_id>`,
addressed to that one sender only; the table and every other client are
untouched. The mutation listener's own oversized-payload guard (line ~1205,
`error.RowTooLargeToReplicate`) is built on exactly this principle and says so in
its own comment: *"a bad write should cost its sender a verdict, not cost every
reader a table."* So a client with a deterministic bug in its write path
penalizes only itself — other, correct clients keep writing to the same table
without disruption.

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

### 2.12 Pipeline mode wedged ingress, silently and only after a while

Introduced by the pipelining work itself. The drain loop broke out of the result loop as
soon as it saw `PGRES_PIPELINE_SYNC`:

```zig
if (st == c.PGRES_PIPELINE_SYNC) break;
```

⚠️ **The sync is not the last thing libpq emits** — a trailing null follows it, and
`PQexitPipelineMode` **fails while any result is unconsumed**. The `defer` discarded that
failure (`defer _ = c.PQexitPipelineMode(conn)`), so the connection stayed in pipeline
mode. The next mutation entered a pipeline that was already open, its results misaligned
against the statements that produced them, and the listener eventually blocked in
`PQgetResult` waiting for something the server had already sent.

**Nothing reported it.** The bridge logged its usual `LOOP`/`METRICS` lines, `connected=1`,
`slot_active=1`; CDC kept flowing. Only ingress stopped. Mutations piled up unacked with
`Outstanding Acks: 1` and an acknowledgement floor frozen tens of seconds behind the
stream, and clients saw "no verdict at all" — indistinguishable from a slow bridge.

Diagnosed from PostgreSQL rather than from the bridge: `pg_stat_activity` showed the
`bridge_writer` session **idle in `Client/ClientRead`** with the classify statement as its
last query. The server had finished and was waiting for the client; the client was waiting
for the server.

⚠️ **It is not deterministic, which is why it survived several green suite runs.** The
misalignment only becomes a block once a particular sequence of statement shapes lines up,
so a short scenario can pass with the connection already in a bad state. That is the
lesson: a passing write-path scenario says nothing about whether the *connection* is still
usable afterwards.

Fixed by draining to null after the sync, and by checking `PQexitPipelineMode` — logging
and `PQreset`ing on failure, since a connection that cannot leave pipeline mode is unusable
and a reset is cheaper than a listener that looks alive and accepts nothing.

Guarded by a soak in `scripts/scenarios/replies.py`: a run of mutations must produce a
verdict for **every** one, which is what a wedged connection cannot do.

### 2.10 A `u16` column offset silently corrupted wide rows  ← worst failure mode

`CDCEvent.ColumnView.name_offset` was `u16`, and it indexes into `data_buffer`, whose size
is `2^BASE_BUF`. At the 16 KB default that is unreachable. At `BASE_BUF=19` the buffer is
512 KB, so the first column written past byte **65535** tripped
`@as(u16, @intCast(self.data_len))`.

⚠️ **The Debug crash is the lucky case.** In Debug it panics — "integer does not fit in
destination type" — and the bridge dies loudly, which is how this was found. In
**ReleaseFast the cast is undefined behaviour**: it wraps, every later column is read from
the wrong offset, and the event decodes as garbage with nothing reporting an error. A
release build would have shipped corrupt rows to every client.

Two things made it reachable rather than theoretical:

- the sizing guards **accept** `BASE_BUF=19` — it is a valid configuration, refused only
  against `max_payload` (needs ≤ 19 here) and against RAM (needs `RING_BUFFER_COUNT`
  halved per step). Nothing connected buffer size to the offset type;
- the offset was computed **before** the bounds check that would have described the
  problem, so an oversized row hit the narrowing cast on its way to the error meant to
  catch it.

Found by verification B of the snapshot-memory plan: a table of 200 × 256 KiB rows,
snapshotted at `BASE_BUF=19`. It never fires on the narrow tables the suite otherwise uses.

Fixed by widening to `u32` (matching `value_offset`, which was always `u32`), moving the
bounds check ahead of the cast, and adding two unit tests in `batch_publisher.zig` —
**both verified to fail** with the old types reinjected.

⚠️ The same review turned up `column_count: u8` guarded by `if (column_count >= 512)`, a
check that could never fire: the counter panics on increment at 255 first. A 256-column
table would have crashed instead of receiving `TooManyColumns`. Widened to `u16`.

### 2.11 A refused snapshot always blamed the primary key

`snapshot_listener` reported every refusal as `(no primary key)` — in the log and in the
`init.snap.error` payload — regardless of the registry's actual reason. A table suspended
for `row_too_large` therefore sent its operator to add a key it already had. Found while
snapshotting the same wide fixture, which has a perfectly good `bigint PRIMARY KEY`.

The registry knew the reason and simply had no getter; `refused_tables.reasonFor()` now
exposes it, and both the log and the payload carry the real reason plus its `fixHint()`.

### 2.9 One `PQconsumeInput` per message made bursts quadratic  ← biggest win

`receiveMessage` called `PQconsumeInput` before **every** `PQgetCopyData`. That is the
wrong side of a libpq detail: `PQconsumeInput` → `pqReadData` slides the *unread
remainder* of `conn->inBuffer` back to the front of the buffer. During a burst that
buffer holds megabytes of WAL, so the memmove cost is proportional to the backlog —
O(backlog) per message, **O(n²) overall**.

The libpq idiom is the opposite: consume once, then call `PQgetCopyData` repeatedly
until it returns 0. One `needs_input` flag on the stream implements it.

| same load, same build, only `wal_stream.zig` changed | before | after |
| --- | --- | --- |
| `recv_ms` of a 15 000 ms interval | 14 884 (99.2%) | 130 (0.9%) |
| `proc_ms` (decode + pack) | 112 | 1 191 |
| `idle` iterations | 0 | 10 790 |
| throughput | ~1.2k msg/s | 1.44M events in one interval |
| CPU | 100% of a core | 31% while draining |

**Why it survived nine months** (the line dates from 2025-11-28) is the instructive
part, and all three reasons are about measurement, not code:

1. **It only bites once you are already behind, and then keeps you there.** Every load
   test until 2026-08-15 used small transactions at a trickle
   (`Produce.stream(5, 1000, 10)` = 10 rows/tx every 5 ms). A bridge that keeps up never
   accumulates a backlog, so the quadratic term stayed numerically zero. The first
   1000-rows-per-transaction burst made it appear instantly — and self-reinforcing:
   behind → slower → further behind.
2. **The one "is it backed up?" gauge pointed the wrong way.** `queue_usage_percent`
   watches the *flush* side; it read 0% precisely **because** the starved main thread
   never filled the queue. The healthiest-looking number was a symptom.
3. **The WAL loop had no instrumentation at all.** The bug lived in the only path with
   no metric on it, which is not a coincidence: unmeasured code is where bugs last
   longest. Adding the `LOOP` line (4.4) turned a week of speculation into a five-minute
   diagnosis — `recv_ms=14884 / proc_ms=112` is not a reading anyone argues with.

The lesson that generalises: **a benchmark number without a method is worse than no
number**. The README claimed "~50k evt/s" the whole time; nothing could fail against it
because nothing said how it was obtained. It now ships with machine, build mode,
transaction shape and row count, so the next regression has something to contradict.

---

### 1.11 CLOSED — the schema payload now publishes "writable"

`CLIENT_WRITES.md` designed the schema's write contract as
`"sync": { "version": …, "tombstone": …, "writable": true }`. The `writable` half went
unbuilt for a while — what shipped was flat (`version_column`, `tombstone_column`,
`mutation_timeout_ms`, `replica_identity`) with no writability signal — even though
`users` measurably publishes `version_column: "updated_at"` and refuses every mutation
with SQLSTATE `42501`, because writability is a **grant**
(`zebridge_grant_edge_writes`), and grants weren't in the payload.

Not a withheld decision, once checked: `SYNC_RULES`, `TENANT_RULES`, and preflight's
grant check (`logVerdict`/`reportVersionColumns`) already concluded the right answer
per table at boot and logged it — `'users': outbound-only — the writer has no INSERT
privilege`. The verdict was just print-only: two local counters for one summary line,
nothing retained for `appendWriteContract` to read back.

Built as `src/writable_tables.zig` — a table→bool registry, same append-only/atomic
shape as `refused_tables.zig` (no `std.Io.Mutex`, so entries are appended and never
removed, readers hold name slices safely). Two writers populate it:

- **Boot**: `reportVersionColumns` records `usable and not key_db_allocated` per
  table — the database-allocated-key case matters here specifically because
  `logVerdict` alone would call such a table "writable" (it has grant and version
  column), while `mutation_listener.zig` refuses every write to it anyway
  (`DbAllocatedKey`). The published fact has to be what a client can actually rely
  on, not just what the schema shape permits.
- **Runtime**: `reportTable`, called from `event_processor.reportEdgeWritability` on a
  `CREATE TABLE` DDL event, now runs the *same* grant + key-allocation query the boot
  path does — it used to hardcode `granted: true` ("no privilege lookup in hand"),
  which would have reported a table as writable that the writer has no grant on. Fixed
  by threading `writer_role` through instead of assuming.

`appendWriteContract` (`event_processor.zig`) reads the registry and publishes
`writable: true|false`, or `null` for a table that raced the publish before its own
report landed — never guessed either way. `PROTOCOL.md` §3 now documents the field.

⚠️ **Found live, extending `writable.py` into a drift check against this field**: a
*suspended* table's payload never went through `appendWriteContract` at all —
`publishSuspension` (both copies, `event_processor.zig` and `snapshot_listener.zig`)
builds a smaller, separate payload (`{table, suspended, reason, lsn}`) on a different
code path, so `writable` was silently absent rather than `false`, even though preflight
had already concluded `false` for it. Not wrong exactly — the client's `suspended` gate
(§1.6c) already blocks writes to it independent of `writable` — but a client checking
`writable` alone, without also checking `suspended`, would have read the missing field
as "unknown, might be writable." Fixed: both `publishSuspension` copies now hardcode
`"writable":false` (never read from the registry — a suspended table is *always*
unwritable, regardless of grant, since no CDC event will ever confirm or correct an
optimistic write to it).

### 1.10 `ColumnView` could store lengths only — 12 B → 8 B

`CDCEvent.ColumnView` is replicated `max_columns × RING_BUFFER_COUNT` times, so its width
is a direct multiplier on startup memory. It currently holds
`{name_len: u8, name_offset: u32, value_tag, value_len: u32}` = **12 bytes**.

⚠️ **Narrowing the field does not help, and this was measured rather than assumed.**
Unpacked, every integer from `u17` to `u32` occupies 4 bytes — `@sizeOf(u21) == @sizeOf(u32)`
— so `u21` is free *and* pointless. The only smaller step is `u16` at 2 bytes, which caps
the offset at 65535 and is exactly the wrap bug of §2.10. A `packed struct` does reach 8
bytes, but puts a shift+mask on every column access in the hottest loop.

The saving is available by storing **less**, not narrower. `addColumn` lays each column
down contiguously, so the whole offset chain is derivable from the lengths:

```
name_offset[0] = 0
name_offset[i] = name_offset[i-1] + name_len[i-1] + value_len[i-1]
```

Dropping `name_offset` gives `{name_len, value_tag, value_len}` = **8 bytes**, measured —
the packed struct's saving with no bit manipulation. (`value_offset` was already removed
on the same reasoning, §2.10.) All three production readers already iterate
`columns[0..column_count]` or take `columns[0]`, so a running offset drops straight in.

⚠️ **The catch is that it makes the array sequential-only.** `columns[i]` stops being
meaningful standalone, and a later random access would silently read the wrong bytes —
the same failure class as the `u16` wrap. So it should ship as an **iterator** yielding
`(name, value)` slices, with no offset accessor at all, making the contract structural
instead of a comment.

**Not done because the ratio is poor.** Sizing `max_columns` to reality (512 → 128) took
metadata from 1614 MB to 462 MB on a 262,144-slot ring; this would take it to 334 MB. An
order of magnitude less win, for a refactor of the decode path. Worth doing only if the
bridge is being pushed into a genuinely memory-tight deployment.

### 1.9 CLOSED — a durable client outbox, and how optimistic apply reconciles with CDC

The bridge holds up its end of §7.1: every write gets a definitive reply on
`mutation_ack.<principal>.<msg_id>` (§7.4b). The client now holds up the other
half — the optimistic row and the intent-to-send commit land **together**:

```sql
BEGIN;
  UPDATE users SET name = ? WHERE id = ?;  -- what the user sees, right away
  INSERT INTO outbox (…) VALUES (…);       -- the intent to send
COMMIT;
```

Built via SQLocal's `transaction()` API — `outboxPut(row, tx.sql)` and
`applyEvent(table, {..., lsn: Number.MAX_SAFE_INTEGER, optimistic: true}, tx.sql)`
inside one `transaction()` call in `sendMutation`. The table is `_zebridge_outbox`
— renamed from an earlier `outbox`, since a bare name could collide with a
genuinely replicated table of that name, and PROTOCOL.md §7.1 now specifies the
name and schema for all three of the client's bookkeeping tables (it is not
something a client author should have to invent). It lives in the same SQLite
file as the replica, not a second database (an earlier, separate two-database
WIP attempt was scrapped and rebuilt this way).

All three bookkeeping tables — `_zebridge_sync`, `_zebridge_stream_seq`,
`_zebridge_outbox` — are now created together in `initSyncState()`, the first
thing `initNats()` awaits, rather than each owning its own init path. The outbox
table used to create itself in a standalone module-level promise that happened
to resolve before `initSyncState()` ran in practice; nothing enforced the
ordering, it just worked out that way. `initSyncState()` now creates all three
directly and resolves a deferred `outboxInit` promise when done, so the outbox
helpers (which run later, only after a connection exists) still `await
outboxInit` without needing their own creation path.

`DB_NAME` stays timestamped by default (`zebridge_${Date.now()}.sqlite3`) — the
incognito, rebuild-from-scratch dev loop is unchanged. A stable name
(`zebridge_<principal>.sqlite3`, durable across reloads) is opt-in via
`VITE_DURABLE`, exactly as planned: a durable outbox is pointless in a file wiped
every reload, and forcing it on by default would have traded the dev loop's main
value — no cache or leftover state to explain a result away — for nothing.

#### How the optimistic write and the CDC echo reconcile

There is no version check at the SQLite layer — the optimistic apply and every
CDC/snapshot apply run the identical `INSERT … ON CONFLICT DO UPDATE SET col =
excluded.col`, an unconditional overwrite. Ordering is what makes this correct:

- The optimistic write's sentinel LSN (`Number.MAX_SAFE_INTEGER`) always clears
  the per-table LSN gate, so it applies immediately, and — deliberately — never
  advances `state.lsn` or `globalSyncState.lsn` (`App.tsx` around `applyEvent`'s
  resume-bookkeeping block). If it did, every later real CDC event for that table
  would compare `< MAX_SAFE_INTEGER` and never apply again.
- When the real CDC event for that write arrives, its real `lsn` is still `>
  state.lsn` (untouched by the optimistic write), so it passes the gate normally
  and runs the same upsert — overwriting the optimistic guess with whatever
  Postgres actually holds. Accepted-unchanged: a no-op. LWW picked a different
  winner: the guess is silently replaced.
- The echo is also what clears `pendingWrites` and pops the outbox row (matched
  on primary key, not on `msg_id` — so *someone else's* echo of the same row
  correctly clears our entry too, since the row reached an observable state and
  LWW already decided whose write that was).

#### 1.6d CLOSED — a rejected or row_deleted write now reverts, not just logs

`stale` self-corrects — the winner arrives over CDC and overwrites the guess.
`rejected` and `row_deleted` don't: Postgres never applied the mutation, so no
CDC event for it exists, and the row sat wrong in the local replica indefinitely
until some unrelated write happened to touch it.

Fixed with `revertOptimisticWrite(msgId, mode)`. `outboxPut` now captures a
`before` snapshot — the row exactly as it stood, read inside the same
transaction as the optimistic apply, so it reflects what was really there rather
than whatever the replica holds by the time a verdict shows up. `before` is
`null` when the write was an INSERT (there was no prior row).

The two verdicts revert to different truths, not the same one:

- **`rejected`** — the write never applied, so the correct state is whatever was
  there **before** it. Restore `before` (or delete the row, if `before` is
  null — the write was an INSERT that never happened).
- **`row_deleted`** — the row is *confirmed gone* server-side. Restoring
  `before` would resurrect data that no longer exists anywhere; the only correct
  local state is "no row", regardless of what `before` says.

⚠️ **Guarded, not unconditional.** Something can touch the row between the
optimistic apply and the verdict arriving — a real CDC event from another
client, or a second optimistic write from this same client (a double-click).
Reverting blind would clobber state that is *more* correct than what's about to
be written. So the revert only fires if the row still shows exactly what this
write's own optimistic apply produced (compared against the outbox's stored
`payload`, not re-derived); otherwise it just drops the outbox entry and leaves
the row alone — the same behaviour as before this existed, and correct, because
something else has since explained what's there.

Reads the outbox row itself, not in-memory `pendingWrites` — a write replayed by
`flushOutbox` after a reload has no in-memory entry, but the durable row (with
its `before`) is exactly what a revert needs, so this works identically whether
the write was sent this session or resent on reconnect.

⚠️ **A durable replica can boot stale**, which the timestamped default never had
to face: a queued outbox write can be older than the GC watermark (§7.1 MUST-6) —
the tombstone that should have overruled it may already be reaped, and replay has
no way to tell "stale" from "safe to resend" once that evidence is gone. Not yet
addressed; only surfaces with `VITE_DURABLE` after a long outage.

### 1.8 Read authorization — what the publication can and cannot filter

Writes are authorized by the DBA's grants and RLS, and the bridge enforces nothing (§7.0)
— **per table, per column and per row, all available today** with no schema change, because
a write is a statement and the DBA's rules apply to statements. Reads have no equivalent:
every client subscribed to `cdc.>` receives every published table's changes.

That asymmetry is the same property seen from two sides. The write path is safe *because*
it goes through the database; the read path is unscoped *because* it does not. Two PostgreSQL features look like the answer and each rules the other out.

**RLS is not one of them.** It is evaluated against `current_user` at query time, and
logical decoding has no query and no user. Measured: a table with `USING (owner='alice')`
returned **1 row** to a `SELECT` by `bridge_reader` and emitted **2 INSERT records** to the
WAL, bob's included. So RLS filters the snapshot and is bypassed by CDC — the two paths
describe different tables, and the snapshot is the one that obeys.

What pgoutput *does* honour is the publication's **column list** and **row filter**. Both
are DBA-authored, both live in the database, and the snapshot path now applies them too
(`getTableColumns` reads `prattrs`; the chunk loop ANDs in `pg_get_expr(prqual, prrelid)`).
Before that the snapshot shipped rows the change feed would never mention again — no
update, no delete, no tombstone — so they sat in the replica permanently, uncorrectable.

⚠️ **But the two filters exclude each other on any table that is written to**, and
PostgreSQL enforces it at the source rather than leaving the bridge to cope:

| configuration | UPDATE / DELETE |
| --- | --- |
| RI `DEFAULT` + row filter on a non-key column | **refused** — *"Column used in the publication WHERE expression is not part of the replica identity"* |
| RI `FULL` + column list omitting any column | **refused** — *"Column list used by the publication does not cover the replica identity"* |
| RI `FULL` + row filter, no column list | permitted |

So hiding a column forces `DEFAULT`, which restricts row filters to key columns; filtering
by an owner column forces `FULL`, which forbids hiding anything. And the failure is loud in
the worst way — adding a publication filter can stop the *application's* own writes, not
merely the bridge's.

#### ✅ Unless the tenant column is part of the key — then all of it works

The exclusion above is a consequence of the filter column sitting *outside* the replica
identity. Put it inside and every constraint dissolves at once. Measured, with
`PRIMARY KEY (tenant_id, uid)` and RI `DEFAULT`:

| | result |
| --- | --- |
| `WHERE (tenant_id = 'a')` on the publication | UPDATE and DELETE **permitted** — the column is in the replica identity |
| column list `(tenant_id, uid, note)`, omitting `secret` | **accepted** — it covers the replica identity |
| DELETE payload | **carries `tenant_id`** — it carries the key, so the delete is routable |
| moving a row between tenants | a **key change**, so CDC emits `old.tenant_id` as well |

That last row matters more than it looks: the "row leaves the bucket" case — where one WAL
record has to become a removal for the old owner and an insert for the new — needs the old
owner's identity, and a key change is the one UPDATE shape that supplies it under
`DEFAULT`. A tenant column outside the key gets none of this.

**So the schema rule is one line: for a table that needs per-tenant reads, the tenant
column belongs in the replica identity.** No `FULL`, no write amplification, no choosing
between hiding a column and filtering rows.

⚠️ **In the replica identity — not necessarily in the primary key.** Changing a key is
expensive: every referencing foreign key follows it, and clients key their local replicas
on it. `REPLICA IDENTITY USING INDEX` gets the same result for two statements and no key
change (measured — DELETE carried the tenant, the column list still hid `secret`, and the
published `pk_columns` were unchanged because the bridge reads `indisprimary`, not the
replica identity):

```sql
CREATE UNIQUE INDEX t_ri ON t (tenant_id, uid);   -- NOT NULL columns, unique, not partial
ALTER TABLE t REPLICA IDENTITY USING INDEX t_ri;
```

The primary key stays `uid`, so **no client changes** and no FK churn. The cost is one
index per table and remembering that dropping it silently drops the replica identity with
it.

Two ways to use it, and they trade differently:

- **A publication (and slot) per tenant.** PostgreSQL does the filtering, so a bridge bug
  cannot leak across tenants — the rows never enter the process. ⚠️ Bounded by
  `max_replication_slots` / `max_wal_senders`, both **10** in this compose, so it fits "a
  small number of tenants" literally and stops there.
- **One slot, and the bridge appends the tenant to the subject**
  (`cdc.<table>.<op>.<tenant>`), with NATS allow-lists enforcing who may subscribe. No slot
  ceiling, and it reuses the mechanism that already authorizes writes — but the rows do
  pass through the bridge, so isolation is the bridge's correctness rather than the
  database's.

The second scales; the first is defence in depth. They compose: filter per tenant *and*
tag the subject, if the tenant count stays small enough to afford the slots.

`FULL` also has a standing cost: every UPDATE and DELETE writes the entire old row to WAL,
and every CDC event carries `old.*` for every column, which is per-event buffer pressure
(`BASE_BUF`) as well as write amplification.

**Why `FULL` is nonetheless the only basis for per-row routing.** Under `DEFAULT` the
bridge cannot evaluate an ownership rule on the events that matter:

| event | payload under `DEFAULT` | can the bridge route it? |
| --- | --- | --- |
| INSERT | full new tuple | yes |
| UPDATE, key unchanged | new tuple only | new owner yes; **old owner unknown** |
| UPDATE, key changed | new tuple + old key | same, plus the old key |
| DELETE | **old key only** | **no** — zero owner attributes |

A delete carries no tenant column at all, so it cannot be addressed to the principal who
held the row. `FULL` supplies the old tuple and closes both gaps.

⚠️ Even then, an UPDATE that *moves* a row between owners needs the bridge to emit two
events — a removal to the old owner, an insert to the new — because one WAL record has to
become two subjects. Nothing in PostgreSQL does that; it is bridge work, and it is the
same "row leaves the bucket" problem PowerSync solves with bucket membership.

#### If RLS is enabled, `bridge_reader` needs a policy — not `BYPASSRLS`

RLS applies to the snapshot (it is a `SELECT`) and the reader has **one session for all
tenants**, with no principal to resolve — so every RLS-protected table snapshots as **0
rows** while CDC ships everything. The reader therefore has to be exempted somehow, and the
two ways are not equivalent:

| | `shop` (should replicate) | `payroll` (should never be read) |
| --- | --- | --- |
| `ALTER ROLE bridge_reader BYPASSRLS` | 2 rows ✓ | **1 row ✗** |
| `CREATE POLICY reader_all ON shop FOR SELECT TO bridge_reader USING (true)` | 2 rows ✓ | **0 rows ✓** |

`BYPASSRLS` is a *role attribute*: it applies to every table in the database. Combined with
the blanket `GRANT SELECT ON ALL TABLES IN SCHEMA public` this file already issues — plus
the matching `ALTER DEFAULT PRIVILEGES` — it means the bridge process can read every table
that exists or ever will. A per-table permissive policy is the same capability scoped to
the tables actually replicated, and it **fails closed** for anything nobody considered.

⚠️ Either way the reader ends up able to see every *row* of the tables it replicates, so
read confidentiality becomes the bridge's correctness (does the snapshot apply `prqual`?)
rather than the database's. The write path keeps the opposite property: `bridge_writer`
must never hold `BYPASSRLS`, and with RLS in place a fully compromised bridge still cannot
write outside a principal's tenant. **A bridge bug can leak reads; it cannot forge writes.**

⚠️ And RLS guards only the snapshot door. A table accidentally added to the publication is
shipped by pgoutput whatever its policies say — *not publishing it* is the only control
that reaches CDC.

#### The subject invariant: `<stream>.<identity>.<everything that varies>`

Every subject that carries data should put the identity token **immediately after the
stream prefix**, before the table, the operation, and any suffix.

⚠️ **The reason is not that the subject is hard to guess.** A client can name
`cdc.globex.orders.insert` perfectly and still get `Permissions Violation` — the allow-list
is checked on every subscribe, and knowing the name buys nothing. Publishing the grammar in
PROTOCOL.md is therefore safe, and opaque tenant ids buy no security.

The reason is that **grants stay stable under schema change**. One grant, `cdc.acme.>`,
covers every table, every operation, and every suffix — including ones that do not exist
yet. Measured the hard way: with the identity *last*, batching appended `.batch`
(`batch_publisher.zig`) and turned `cdc.orders.insert.acme` into
`cdc.orders.insert.acme.batch`, which no tenant grant matched. A tenant's rows arrived
under light load and stopped under heavy load — a leak-adjacent bug whose trigger is
*volume*. Identity-first absorbed the new suffix with no permission change.

So the property is: **adding a table, an operation or a suffix can never widen or break a
grant.** That is what makes it a safety guard rather than a naming convention.

Where the invariant holds today — 3 of 9:

| pattern | identity-first |
| --- | --- |
| `cdc.<tenant>.<table>.<op>` (when TENANT_RULES is set) | ✅ tenant |
| `mutation.<principal>.<table>.<operation>` | ✅ principal |
| `mutation_ack.<principal>.<msg_id>` | ✅ principal |
| `cdc.<table>.<op>` (unscoped) | ❌ no identity at all |
| `mutation_error.<table>` | ❌ keyed by table |
| `init.snap.<table>.<snapshot_id>.<chunk>` | ❌ keyed by table |
| `snapshot.request.<table>` | ❌ no identity |
| `$KV.schemas.<table>` | ❌ every client reads every table's columns |
| `$KV.snapshots.<table>` | ❌ no identity |

The snapshot rows are what increment 2 must change: `init.snap.<tenant>.<table>.…` and
`snapshot.request.<principal>.<table>`.

**What the invariant does not cover**, and should not be claimed to:

- **A stolen credential.** Full access to that identity; subject shape is irrelevant. This
  is JWT's job — not obscurity, but *binding* (a scoped signing key derives the grant from
  the identity with `{{name()}}`, so the two cannot drift apart) and *expiry* (a leak has a
  bounded window).
- **A publisher bug.** NATS enforces who may subscribe; nothing verifies that the bridge
  tagged a row with the right tenant. That is model B's residual risk and no grammar closes
  it.
- **Anything not behind an identity token** — six of the nine patterns above.

#### The cost of per-tenant snapshots — measured, and it hinges on clustering

A tenant-scoped read path means one snapshot per (table, tenant) rather than per table.
That sounds like N× the work and mostly is not: each row belongs to exactly one tenant, so
the N snapshots *partition* the table — summed, they carry one snapshot's worth of rows.
It is also demand-driven, so N is the number of tenants that actually connect.

⚠️ **The I/O, though, depends entirely on physical layout.** Measured on 200k rows across
20 tenants, 6,250 blocks total:

| | heap blocks read for one tenant |
| --- | --- |
| rows interleaved (the natural insert order) | **6,250** — the whole table |
| after `CLUSTER t USING t_zb_ri` | **313** — its own 1/20th |

Unclustered, every tenant's snapshot scans nearly the entire heap, so N tenants really do
cost N full scans. Clustered on the tenant-leading index, the N snapshots together cost
about *one* scan — which is what a single shared snapshot would have cost anyway.

The index required for the replica identity (`(tenant_id, <pk>)`) is exactly the right
cluster key, so it is one more line in the same migration. ⚠️ `CLUSTER` takes an
`ACCESS EXCLUSIVE` lock and is **not maintained** — new rows land wherever there is space,
so the locality decays and the command has to be repeated. Declarative partitioning by
tenant gives the same locality permanently, with no lock and no repetition, and is the
better answer once tenant counts grow.

**And the multiplication lands on the right side.** A single shared snapshot sends every
tenant's rows to every client — the leak this whole section exists to close, and N× the
bandwidth per client. Per-tenant snapshots invert that: bridge-side total work stays flat,
client-side work drops by a factor of N.

**Where this leaves the design.** Per-table secrecy (a column that never leaves the
database) is a publication column list, and it works today. Per-principal routing is a
bridge concern regardless — Postgres has one slot and no notion of who is subscribing — so
it means a DBA-declared owner column becoming a subject token (`cdc.<table>.<op>.<owner>`)
with NATS allow-lists enforcing it, exactly as `mutation.<principal>.…` works for writes.
That path requires `REPLICA IDENTITY FULL` on any table it applies to.

### 1.12 Snapshot reach must equal subscribe reach — the tenant axis is half-built

§1.8 answers "what can the *publication* filter". This is the other half: what each
**transport path** can be restricted to, and where they disagree.

The rule worth adopting: **a client must not be able to dump what it cannot subscribe to.**
A snapshot obtainable for a feed you have no grant on is not a convenience, it is the CDC
authorization bypassed with extra steps.

| axis | discloses | CDC path | snapshot path |
| --- | --- | --- | --- |
| schema | the *shape* of a table | `$KV.schemas.>` — wholesale | same bucket, same grant |
| table | that rows exist, and their content | grant `cdc.*.<t>.>` instead of `cdc.>` | grant `init.snap.<t>.>` + `snapshot.request.<t>` |
| column | which columns | publication column list (§1.8) | same list — `snapshot_listener.zig:470` |
| row / tenant | which rows | tenant is a **subject token**: `cdc.<tenant>.>` | ✅ **built and measured** — see "The design, in three parts" below |

Two of these are already solved and simply unused. **Column** scoping works on both paths
through the publication column list. **Table** scoping is expressible on both paths today —
every subject carries the table — and is a *configuration* gap, not an architecture one:
the shipped conf grants `cdc.>`, `init.>` and `snapshot.request.>` wholesale. Tightening it
is a conf edit and no code, and the invariant above is what makes it checkable: for every
table, the `init.snap.<t>.>` grant must match the `cdc.*.<t>.>` grant.

**Row scoping was the architecture gap; it is now built, in the three parts described
below** — `init.snap.<tenant>.<table>.<snapshot_id>.<chunk>` carries the tenant, and
`snapshot.request.<tenant>.<table>` carries the audience it was requested for.

The original finding, kept for the record: `bob` (tenant `globex`) published
`snapshot.request.test_types` (no tenant token, the old shape) — a table whose rows are all
`acme` — and the bridge answered him with a data chunk. The row payload was not decoded, so
that was not a verified row-level leak; it was that the **request-and-delivery path was
unscoped**, which `snapshot_listener.zig` said by containing no reference to `tenant`
anywhere in its 2093 lines. Both now false: the audience half is enforced by the
`INIT_<TENANT>`/`INIT_PUBLIC` stream split (mirroring the CDC split — a JetStream consumer's
`filter_subject` is reader-chosen, not ACL-checked, so the stream itself is the boundary,
`scripts/scenarios/crosstenant.py`), and re-measured directly against fresh data: `bob`
denied both `INIT_ACME`'s stream info and `$KV.snapshots.acme.test_types`'s descriptor
(`scripts/scenarios/tenant_kv.py` check 4).

#### Where enforcement actually lives, per shape

The cleanest statement of L2. Note the last column: it is the one that decides how much
discipline the deployment needs.

| | CDC | SNAP | coherence |
| --- | --- | --- | --- |
| **T2L2-single** | PG — publication row filter | PG — the same filter, read back via `pg_get_expr(prqual)` | **structural**: one predicate serves both paths, so they cannot disagree |
| **T2L2-multi** | bridge routes by subject + NATS grants | PG — RLS via `zb.tenant`, taken from that same subject | **structural**, once one token drives both: the tenant is granted by NATS, routed on, and filtered by — see part 2 |

⚠️ In the multi shape PostgreSQL enforces **nothing** on the CDC path — the WAL carries every
tenant's rows and the split happens in the subject. The snapshot path is still a Postgres
policy, so the two paths are enforced by different subsystems; what keeps them coherent is
that both are keyed on the **same NATS-granted tenant token**, not on two derivations of it.
Filter the snapshot by `zb.principal` resolved through `zebridge_user_tenants` instead, and
the coherence drops to conventional: subject says `acme`, mapping says `globex`, and globex
rows are published where acme's clients read them.

What remains for a drift checker is narrower and no longer a leak: the NATS conf defines read
scope while `zebridge_user_tenants` defines write scope, and under N-1 they should name the
same tenant for a given principal. Disagreement gives inconsistent read/write reach.

#### The design, in three parts

The first two are not alternatives. Postgres decides the **contents**; the subject decides
the **audience**. Doing only the first gives correctly-filtered dumps published where the
wrong tenant can read them. The third makes the client's own tenant authoritative instead of
guessed.

1. **Contents — in PostgreSQL, not in the bridge.** ✅ BUILT. The snapshot opens its *own*
   connection (`snapshot_listener.zig`, `generateIncrementalSnapshot`), separate from
   replication. `zb.tenant` is set on it from the authenticated request subject — the same
   `set_config(…, is_local => true)` the mutation listener already uses for `zb.principal` —
   and `zebridge_scope_reads_by_tenant()` (`init.core.template.sql`, wired into
   `zebridge_enable()` for every tenant-scoped table, read-only or writable) adds the reader
   `SELECT` policy filtering on `current_setting('zb.tenant', true)`. Measured end to end: a
   `test_types` snapshot pulled as `alice` (tenant `acme`) against a table holding both
   tenants' rows decoded to exactly the `acme`-tagged row, nothing from `globex`
   (`tenant_kv.py` check 4).

   §1.8 measured that **RLS filters the snapshot** and is bypassed by CDC. That asymmetry is
   exactly what makes this work: the snapshot obeys the policy, and CDC is scoped by the
   subject instead. The bridge builds no predicate — it relays a token and PostgreSQL decides
   the rows.

   ⚠️ **`zb.tenant`, not `zb.principal` resolved through `zebridge_user_tenants`.** Both would filter correctly in isolation, but only one cannot disagree with the subject the chunks are published on — see part 2. Resolving the principal through the mapping table is a *second* derivation of the tenant, and two derivations of one fact eventually differ. The read path therefore never touches `zebridge_user_tenants`; that table is the write path's.

   ⚠️ This does **not** touch `zb_reader_all USING (true)`. That policy exists so the *replication* connection sees every tenant; a change feed bounded by RLS goes silently incomplete rather than correctly partitioned. Different connection, different policy.

   ⚠️ If `zb.tenant` is unset the predicate is NULL and **every** row is excluded — the same fail-closed shape the write path has, where a missing `zb.principal` refuses every write.
   Silent emptiness is the correct failure here, but it looks exactly like an empty table, so it needs to be distinguishable in the descriptor (see the closed filter-vs-refuse question).

1. **Audience — tenant-keyed, filtered by the same token it is routed by.** ✅ BUILT:

```txt
       snapshot.request.<tenant>.<table>        grant: snapshot.request.acme.>
       init.snap.<tenant>.<table>.<id>.<chunk>  grant: init.snap.acme.>
       $KV.snapshots.<tenant>.<table>           grant: $KV.snapshots.acme.>
       cdc.<tenant>.>                           grant: cdc.acme.>      (already exists)
```

   ⚠️ **The tenant in the subject is granted, not asserted.** A client cannot name a tenant it does not hold, because NATS refuses the subject — the same thing that makes `mutation.<principal>.…` trustworthy. This is why it is safe to route on it.

   ⚠️ **The predicate uses the SAME token.** The bridge sets `zb.tenant` from the authenticated subject and the read policy filters on `current_setting('zb.tenant')` — *not* on `zb.principal` resolved through `zebridge_user_tenants`. That distinction is the whole design. Two derivations of one fact can disagree: subject says `acme`, mapping says `globex`, and globex rows are published on acme's subject. One token cannot disagree with itself, so the coherence is **structural** rather than conventional — the property that
   made the single-tenant shape safe, obtained here without a bridge per tenant.

   ⚠️ **The descriptor must be keyed too, not just the chunks.** `$KV.snapshots` is `max_msgs_per_subject=1`; keyed by table alone the second requester's descriptor overwrites the first's and a client resolves someone else's `snapshot_id` and `lsn`.

   Chosen over principal-keyed (`init.snap.<principal>.…`) because dumps are **shared across a tenant's principals**. Principal-keyed makes the serialized snapshot worker run once per principal per table — a connection storm every time a fleet restarts — for no correctness gain once N-1 removes the ambiguity argument. The read path needs no mapping lookup either way; `zebridge_user_tenants` is left to the write path.

   Residual drift, for `zbctl check` rather than for the design: the NATS conf defines read scope and `zebridge_user_tenants` defines write scope, and under N-1 they should name the
   same tenant. A disagreement gives a principal inconsistent read/write reach — worth catching, but **not** a cross-tenant leak, because reads are filtered by the token they
   are routed by.

1. **Identity — a `tenants` bucket, so the client stops guessing.** `$KV.tenants.<principal>`
   → the tenant, published by a trigger on `zebridge_user_tenants` and propagated exactly as
   schemas already are (trigger → WAL → KV). No new mechanism.

   ✅ **Client side shipped.** `App.tsx` used to have **two** answers and neither was
   authoritative: `VITE_TENANT`, a build-time env var deciding the consumer's
   `filter_subject`, and a displayed tenant inferred from data that had already arrived
   (`SELECT tenant_id FROM … LIMIT 1`, reasoning "RLS only ever showed us rows for our own
   tenant"). They could disagree silently — `VITE_TENANT=globex` on alice's dev server
   authenticated fine and then looked like an idle database, nothing catching the mismatch.
   Both are gone; `resolveTenant()` now asks `$KV.tenants.<principal>` fresh on every
   connect, before `subscribeStreams()` builds the stream list, and the displayed tenant is
   the same value rather than a second, independently-inferred one.

   ⚠️ **The subject is not the bare key.** Direct Get's scopable subject is
   `$JS.API.DIRECT.GET.KV_tenants.$KV.tenants.<key>` — the bucket's own internal
   `$KV.<bucket>.<key>` subject as the suffix — not `$JS.API.DIRECT.GET.KV_tenants.<key>`
   as first guessed. Measured against a live `nats-server`: the wrong grant produced a
   `Publish Violation` naming the actual subject, which is what fixed it.
   `nats-server.conf.template` carries the corrected grant, exact-key per principal, and
   `CONSUMER.*` is correctly absent — no consumer path exists for a plain `get()` in
   either client tested.

   ⚠️ **But "does it use Direct Get" is a client-library question, not just a protocol
   one — first checked against the wrong client.** The `nats` Go CLI (`nats kv get`)
   stayed on Direct Get with no fallback, which is what the claim above was based on.
   The actual consumer — `@nats-io/kv`'s `Kvm.open()`, used by `App.tsx` — does not:
   `Bucket.bind()` (the code path `open()` uses to attach to an *existing* bucket) never
   asks the server whether the stream has `allow_direct` set; it trusts whatever
   `opts.allow_direct` was passed and **defaults to `false`** when omitted.
   `resolveTenant()` calling `kvm.open(topology.kv.tenants)` with no options therefore
   went straight to `$JS.API.STREAM.MSG.GET.KV_tenants` — the unscopable, body-selects-
   the-key path this design deliberately does not grant — and was refused, even though
   `nats stream info KV_tenants` shows `allow_direct: true` server-side. Fixed by passing
   `{ allow_direct: true }` explicitly to `kvm.open()`. The lesson generalizes: a claim
   like "this client stays on Direct Get" has to be checked against the specific client
   library actually shipping, not against whichever tool was on hand to test with.

   ✅ **Fed live.** `zebridge_user_tenants` INSERT/UPDATE is special-cased in the WAL loop
   (`bridge.zig`), routed to `packTenantToKvSlot` (`event_processor.zig`, mirrors
   `packDdlToSlot`'s pattern — publishes the bare tenant string, no JSON wrapping), plus a
   one-shot `publishBootTenants()` backfill of existing rows at startup. Measured live, no
   bridge restart: an `INSERT` into `zebridge_user_tenants` for a throwaway principal showed
   up in `$KV.tenants.<principal>` within the poll window, and a subsequent `UPDATE`
   reassigning its tenant did too (`tenant_kv.py` check 3). ⚠️ **`DELETE` is not handled** —
   deliberately deferred, not an oversight: a revoked or reassigned-away mapping leaves a
   stale `$KV.tenants` entry behind, since only the insert/update branches are special-cased.
   A client reading a stale entry would resolve a tenant that no longer has a row for it in
   `zebridge_user_tenants` — worth closing before this ships anywhere principals get
   deprovisioned.

   ⚠️ **Granted per principal — `$KV.tenants.alice`, never `$KV.tenants.>`.** The wholesale
   grant would hand every client the full principal→tenant map, which is a roster of who else
   exists. Same rule as `mutation_ack.<principal>.>`, and for the same reason.

   ⚠️ **Key order is `principal` → `tenant`, not the inverse, and that is a security
   decision rather than a naming one.** `$KV.tenants.<principal>` is an exact key: the grant
   `$KV.tenants.alice` has nothing to wildcard into. Invert it to
   `$KV.tenants.<tenant>.<principal>` and the *natural* grant becomes `$KV.tenants.acme.>`,
   which enumerates every principal in the tenant — a membership roster handed out by
   convenience rather than by decision. Same rule the conf already states for
   `mutation_ack.<principal>.<msg_id>`: an identity token is only a useful grant if
   everything that can vary sits after it.

   The argument is **blast radius, not forgeability**. A principal cannot be forged inside
   the model — that is what makes it a subject token rather than a payload claim; NATS
   refuses `mutation.bob.>` from alice. But if one principal's credentials leak,
   principal-first means the attacker learns that principal's tenant and nothing more, where
   tenant-first would hand them the full membership list as a target list for the next
   credential. Note also that the tenant id is already semi-public — it is in the CDC
   subject, in row data, and on screen — so what is worth protecting is the **map**, not the
   tenant.

   🔶 **To be challenged.** This is reasoned, not measured. Worth attacking specifically:
   whether a per-principal key defeats KV `keys()` enumeration for a client granted only its
   own key, and whether the trigger's publish path can leak the roster even when the grants
   cannot.

   ⚠️ It also settles part 2's open question cheaply: if the bridge resolves principal→tenant
   to build `init.snap.<tenant>.…`, that lookup and this bucket publish the *same fact*. Build
   the bucket and the routing token is already computed.

#### Cardinality: N-1 for clients, N for infrastructure

`zebridge_user_tenants` is `PRIMARY KEY (principal, tenant_id)` — **N-N by schema**. Adding a
tenant to a principal is therefore an `INSERT` (`ON CONFLICT (principal, tenant_id) DO
NOTHING`), never an `UPDATE`.

**The rule is that client principals hold exactly one tenant.** Broadening someone's access
means moving them to a tenant with wider scope, not accumulating tenants against their name.
Everything downstream depends on this: `$KV.tenants.<principal>` can carry a single value
rather than a list, `init.snap.<principal>.<table>` is unambiguous, and there are no merged
`acme ∪ globex` dumps that silently go stale when one mapping is removed.

⚠️ **But the schema must stay N-capable, and the exception is not an oversight.**
`zb_sweeper` is mapped to every tenant it may reap, and that N-ness *is* the audit
mechanism — `zebridge_audit_sweeper()` answers "which tenants will never have their
tombstones reaped?" by reading exactly those rows. `bridge_sweeper.zig` records that the
alternative was tried and rejected: a policy exempting principal-less sessions
(`USING (current_setting('zb.principal', true) IS NULL)`) made *forgetting* to set a
principal the unsafe default — backwards — and made the sweeper's reach invisible to
`SELECT * FROM zebridge_user_tenants`. A named principal fails closed on omission (0 rows)
and stays auditable in the same table as every other writer.

So N-1 is **a checked rule, not a constraint**, and deliberately so: the constraint one
actually wants — "no *client* principal has more than one tenant" — cannot be expressed as a
primary key, because a key cannot know which principals are clients and which are
infrastructure. Making it a `zbctl check` assertion renders it visible instead of implicit.

Note the browser client already assumes N-1 and could not express anything else: it builds
`cdc.${TENANT}.>` from the single tenant `resolveTenant()` resolves — a single string, not a
list, mirroring the bucket it reads.

#### The cost, which is real and bounded

Filtered dumps cannot be shared: different contents, different messages. The multiplier is
the **tenant** count, not the principal count — principals sharing a tenant share a dump.

⚠️ **And, since the schema payload's `tenant_column` fix, the multiplier only applies to
tables `TENANT_RULES` actually scopes — not to every table indiscriminately.** Before that
fix, a client's `snapKey`/request subject used its own resolved tenant unconditionally, so a
genuinely tenant-agnostic table (`users`, every internal `zebridge_*` table) got a redundant
per-tenant snapshot per principal — five principals, five copies of identical content, none
of them sharing. The schema now carries `tenant_column` (the bridge already had this from
`TENANT_RULES` for CDC routing, `EventProcessor.tenant_rules`; it just wasn't told to
clients), and `App.tsx`'s `effectiveTenantFor(table)` uses it: the client's own tenant for a
table with a real `tenant_column`, `topology.open_tenant` for one without. Measured live:
`alice` (acme) and `bob` (globex) ended up with the *identical* `snapshot_id` for `users`
after this fix, and correctly different ones for `test_types` — so the multiplier below is
now bounded to the tables that genuinely need it, which for most schemas is a small
fraction.

| stream | limit today | effect |
| --- | --- | --- |
| `INIT` | `max_age=604800s`, `max_msgs_per_subject=-1` | chunk bytes × tenants, hard 7-day ceiling |
| `KV_snapshots` | `max_msgs_per_subject=1`, `max_age=0` | one descriptor per (table, tenant) — stays tiny |
| `REQUESTS` | `max_msgs_per_subject=1`, `max_age=60s` | window becomes per-principal-per-table |

Storage is bounded by time and small. The sharper pressure is that **snapshots run one at a
time**: N tenants means N sequential dumps of the same table, so `SNAP_RET_SECONDS` stops
being optional — a request queued behind a long snapshot expires unread at 60s.

⚠️ Note the multiplication is a consequence of *filtering at all*, not of where the rule
lives. Putting the filter in PostgreSQL keeps the bridge stateless; it does not make the
dumps shareable again.

#### Still open

- ~~Filtered snapshot or refusal?~~ **CLOSED: filtered.** Not merely because it mirrors CDC,
  but because the invariant forces it. A client whose *feed* is filtered and whose *dump* is
  refused can never bootstrap the rows it is entitled to see, so it holds a permanently
  incomplete replica while receiving updates for rows it does not have — worse than either
  extreme, and undetectable from inside the client.

  The auditability objection stands and is answered rather than dismissed: a filtered
  snapshot *is* indistinguishable from a small table, so make the filtering **visible**. The
  descriptor already carries `row_count`; it should also record the principal the filter was
  applied for, so "did this client get a subset, and whose?" is answerable from the
  descriptor alone instead of by inference.
- Schema disclosure stays wholesale. Defensible — knowing `orders` has a `price` column is a
  far smaller disclosure than its rows — but it is currently a default, not a decision, and
  it invites targeted probing. What must then be **proved** is not that a probe is denied but
  that it is denied *definitively and once*: silence means the client retries forever (§7.1),
  which turns ingress into a treadmill at no cost to the prober, while *repetition* makes the
  broker amplify one probe into many. `writable.py` already covers the refusal's **reason**
  (row absent, verdict present, right SQLSTATE); `probe.py` covers its **cardinality** and the
  disclosure premise itself — measured: `alice` reads all 5 columns of `users`, writes to it,
  and is refused once as `DbAllocatedKey` with nothing following.

#### A cached descriptor has no expiry of its own — found live, fixed

A client accepted **any** existing `$KV.snapshots` descriptor unconditionally, however old —
the only trigger for a fresh generation was *no* descriptor existing at all
(`App.tsx`'s snapshot-request block: `if (entry) { desc = decode(...) }`, no further check).
Nothing tied a cached descriptor's validity to whether the relevant CDC stream still covers
the gap back to its own LSN watermark.

Measured live: `users`' descriptor sat at a stale 1-row watermark while Postgres held 4 rows,
because a `CDC_PUBLIC` purge (routine dev-environment cleanup — itself safe by the gap-check
design above, which is exactly what should have caught a client whose *own* `local` seq fell
off the stream) orphaned a descriptor a *different* client had already cached, with nothing
tying the two together. The client had no way to tell the cached watermark could no longer be
bridged to "now."

Fixed with `descriptorStillFresh(js, tenant, desc)` in `App.tsx`: before accepting a cached
descriptor, fetch the oldest message still available on the CDC stream that tenant's writes
land on (an ephemeral pull consumer at `first_seq` — reuses the exact
`deliver_policy: StartSequence`/`opt_start_seq` shape the main CDC subscription loop already
uses, deliberately to avoid a new NATS grant) and compare its LSN against the descriptor's
watermark. Oldest-available postdates the watermark → the gap is unrecoverable → discard the
descriptor and request a fresh one. An unverifiable case (stream currently empty, but a
descriptor exists) is treated as unsafe rather than assumed fine — the only two explanations
are "nothing written since boot" (a harmless extra regeneration) and "everything aged out
from under this exact descriptor" (the failure this exists to catch), and an empty stream
alone cannot tell them apart. Verified via a deliberate reproduction: purged `CDC_ACME`,
reconnected, and watched all five affected tables correctly log
`Cached snapshot for <table> is orphaned … requesting a fresh one instead` and regenerate.

This is the same *shape* of problem the Object Store race below is about — a cached read has
to know when it stops being safe to trust — solved here inside the current architecture
(compare against live stream state) rather than via Object Store's own signals. Worth keeping
in mind for whoever eventually does that work: the mechanism would need re-deriving, not
reusing, since Object Store's own `watch()`/meta-freshness answers "is a newer object
available", not "is the CDC history to bridge from this one still there."

#### Object Store for the bulk path — one part measured, one still open

Two separate questions, easy to conflate because they surfaced in the same conversation.

**Should delivery move to NATS Object Store at all**, independent of anything below? Today a
snapshot is: generate on request, publish as `chunk_size`-row (10,000, `config.zig:380`)
messages on `INIT_<TENANT>`/`INIT_PUBLIC`, point `$KV.snapshots.<tenant>.<table>` at the
`snapshot_id`, and make the client do a two-step dance — watch the KV key for a descriptor,
then derive a `filter_subject` and run an ephemeral pull consumer against the stream to
reassemble the chunks (`App.tsx`, the whole `snapshotPromises` block). Object Store collapses
that: one `put()` per generation, one `get()` per client, no separate descriptor bucket (the
`ObjectInfo` — size, digest, chunk count — already carries what `$KV.snapshots` exists to
carry), no hand-rolled ephemeral consumer. It would also stop `INIT_<TENANT>` accumulating
every generation's chunks until `max_age` expires them (§ "The cost of per-tenant snapshots"
above: chunk bytes × tenants × however many generations land inside the 7-day window) — an
Object Store `put()` on the same key overwrites, so only the latest generation's bytes are
ever held. Against that: this is not a small change. It removes `INIT_<TENANT>`/`INIT_PUBLIC`,
`$KV.snapshots`, and four of the five `subjects.snapshot_*_pattern` entries in `topology.json`,
and touches `snapshot_listener.zig`, every stream-creation script, and `App.tsx`'s replay block
— roughly the footprint of the INIT split just finished. And crosstenant.py's finding (a
JetStream consumer's `filter_subject` is reader-chosen, not ACL-checked — the *stream* is the
real boundary) presumably applies to Object Store buckets the same way, which would mean
per-tenant buckets, not per-tenant keys inside one shared bucket — **unverified**, not assumed;
check whether an exact-key grant on `$O.<bucket>.C.>`/`$O.<bucket>.M.>` scopes the way
`$KV.tenants`'s exact-key Direct Get grant does (§1.12 part 1) before leaning on it.

**Separately**, a reseed-on-cadence "leaf" (build it up to date, drop the TTL, replay only the
CDC since the leaf's own cutoff) changes something the current design gets for free: today a
snapshot is immutable at birth — a new generation gets a new `snapshot_id`, the old one's
chunks stay valid and complete under their *own* subject until they separately age out, so a
client mid-fetch is never affected by a new generation starting elsewhere. There is no shared
mutable identity for two writers to race on. A leaf reintroduces exactly that: the object name
is now reused across every refresh, so a client's `get()` can race a `put()` of the next
revision.

**Measured** (`scripts/scenarios/objstore_race.py`, `nats-py`'s `ObjectStore` — 20 rounds
across two runs, 40 MB object, `put()` fired 0–20 ms into a ~2 s `get()`, plus one deliberate
run with `put()` fired exactly 0.5 s into the transfer and a 20 s wait after): a torn read
(part-old, part-new content) **never occurred, and cannot by construction** — reading
`nats-py`'s `put()`/`get()` source confirms every `put()` writes its chunks under a brand-new
nuid-keyed subject and only swaps a single ROLLUP meta message once they're all written; `get()`
resolves the nuid once, up front, and reads only that nuid's chunks for its whole transfer, so a
byte published under a different nuid can never reach an in-flight reader. What *does* happen,
every single time a `put()` lands while a `get()` is still receiving chunks (20/20, including
the deliberate 0.5 s-in case): **the in-flight reader hangs forever** — still unresolved 20 s
after the race, not merely slow. `put()`'s last step purges the *old* nuid's chunks once the new
object is fully written; a reader still subscribed to that now-purged subject is left waiting on
an `OBJ_NO_PENDING` marker that will never arrive, and nothing times it out. The store itself is
unaffected — a fresh `get()` issued right after a hung one returns the new revision cleanly — so
this is a per-reader liveness bug, not a data-integrity one. No torn reads is the right guarantee
to have needed proof of; the hang is the one a leaf design has to plan around, not assume away.

**Also measured**: the hang only afflicts a reader already mid-transfer *before* the swap —
one that instead watches first and only reads after being told a fresh revision exists never
hits it. `ObjectStore.watch()` is a JetStream consumer on the object's *meta* subject alone
(never the chunk data), and that meta message is published exactly once per `put()`, only
after every chunk is already durably written — an atomic, native "available" signal, not
something to hand-roll as a separate flag key. 5/5 rounds of watch-then-`get()` (wait for the
meta update, then fetch) resolved cleanly in ~1.6–1.9s each, every time, against the identical
race that hung 20/20 when the reader was already attached beforehand. This is not new
machinery either — it is the exact shape `App.tsx` already runs today, `kv.watch({key: table})`
in `waitForDescriptor` against `$KV.snapshots`, just Object Store providing the same signal one
level down, on the object itself instead of a separate descriptor.

That narrows the risk rather than removing it: a *watching* reader is still exposed if its own
read overlaps the *next* refresh after the one it just watched for — now a function of refresh
cadence versus realistic read duration, not something every read risks. Two ways to close that
residual gap, and they compose rather than compete. Cheap: keep reading straight off the one
leaf key, discipline every call site to watch-then-fetch (never hold a `get()` open across an
unbounded wait), and size the refresh cadence so it's comfortably longer than the slowest
realistic read. Unconditional: still don't overwrite in place — keep a tiny KV entry per
tenant/table naming the *current* object key (`snapshots.acme.test_types` →
`test_types-gen-042`, mirroring `$KV.snapshots` today), have the refresh job `put()` the new
generation under a **new** key, and swap the pointer last, itself watchable the same way. A
reader that resolved the pointer keeps reading an object nothing will ever overwrite, so the
hang becomes structurally impossible rather than merely unlikely — old generations purge on a
grace delay after the swap, once no reader could plausibly still be attached to them. Same
snapshot_id-per-generation shape already in production, just an object instead of a
chunk-message run underneath each generation.

### 1.13 Pipeline the flush thread — encode the next batch while NATS acks the current one

`batch_publisher.zig`'s `flushLoop` is fully synchronous today: drain events into a batch,
`flushBatch` → `doPublish`, wait for the publish to return, only then start draining the
next batch. Confirmed by reading the code, not assumed — `doPublish`'s own comment says so
directly ("the publish calls below are synchronous").

The idea: while NATS is acking the batch just sent, the flush thread could already be
draining and encoding the *next* batch, so it is ready to send the instant the ack for the
current one lands, instead of only starting then. The size of the win depends on what the
ack wait actually is. It is not network RTT — the bridge and `nats-server` are colocated on
one VPS (§0 elsewhere in this file), so that part is already small. It is plausibly disk
write/fsync time on the NATS side for a file-backed JetStream stream, which colocation does
not shrink at all.

Not yet measured, so not yet built. Before writing any pipelining code: log the time between
issuing a publish and its ack returning, for real batches under real load. If that number is
a few ms and batches are frequent, this is a real win. If it is sub-millisecond, it is not
worth the added complexity. Judge any resulting change the same way every other perf change
in this file is judged — against the README burst benchmark, by iters delta, not by
intuition about which side "must" be slow.

### 1.14 CLOSED — `web-consumer`'s schema watch no longer uses `kv.watch()`; it is a pull consumer

The leak's mechanism, confirmed in `@nats-io/jetstream` 3.4.0 and reproduced live
(`scratchpad/watchprobe.mjs`, a Node script that hands `getPushConsumer` exactly what
`kv.watch()` does):

* `kv.js`'s `_buildCC` returns a config carrying `filter_subject` and `deliver_policy`.
  `getPushConsumer(stream, cc)` (`jsmstream_api.js:62`) is overloaded on its second
  argument: a string binds an existing consumer, an object that satisfies
  `isOrderedPushConsumerOptions` (`types.js:22` — true for *any* object with
  `filter_subject`/`deliver_policy`/`headers_only`…) goes to `getOrderedPushConsumer`.
  So every KV watch is an **ordered** consumer. The `KV_WATCHER_<nuid>` name `kv.js` sets
  is overwritten with `oc_<nuid>_1` (`jsmstream_api.js:115`), and the 5s
  `idle_heartbeat` `kv.js` asks for is overwritten with 30s (`:104`). The probe confirms
  it: `ordered=true`, name `oc_…_1`, `hb=30s`.
* An ordered consumer heals by replacement: `reset()` (`pushconsumer.js:64`) deletes
  the current consumer and creates `<prefix>_<n+1>`. It fires on two missed heartbeats
  (`IdleHeartbeatMonitor`, `maxOut: 2`, checked every 30s) or on a delivery-sequence gap.
  The delete is denied here — `$JS.API.CONSUMER.DELETE` is withheld from clients on
  purpose, and that stays — so each reset leaves the old consumer to its 5-minute
  inactivity threshold. `reset()` also never clears the monitor's `missed` counter, so
  once heartbeats stop arriving **every** 30s tick resets again: the cadence observed.
* Flow control is on (`flow_control: true`) but alice has no `$JS.FC.>` publish grant,
  so a flow-control reply would also be a Publish Violation. Not the trigger on an idle
  bucket, but the same shape of problem: the library assumes grants the server withholds.

What *starts* the resets was not reproduced this session: the probe ran 110s idle
(three heartbeats on time, no reset), then across a bridge restart (nine KV puts,
`dseq 10–18` in order, no reset); the browser tab in the foreground ran 6 minutes on
one consumer. Last session's observation was made while watching the server log in a
terminal — a hidden tab, where Chrome throttles timers and, after 5 minutes, runs them
about once a minute — or across reconnects. Either would starve the monitor. Not chased
further, because the fix does not depend on it.

**Fix (`App.tsx`, `watchBucket`)**: the watch is a plain named-ephemeral **pull**
consumer on `KV_<bucket>` — `deliver_policy: LastPerSubject`, `filter_subject:
$KV.<bucket>.<key>`, `ack_policy: none` — the primitive the CDC path already uses. A
pull consumer has no reset path (a missed heartbeat is just the next pull), and it needs
only grants every principal already holds: `CONSUMER.CREATE`/`CONSUMER.INFO`/`MSG.NEXT`
on `KV_schemas` and `KV_snapshots`. Entries are shaped as the callers already read them
(`key`, `operation` from the `KV-Operation` header, `value`, `delta = pending`). Both
`kv.watch()` sites moved: `watchSchemas()` and `waitForDescriptor()` (which leaked one
abandoned `oc_` consumer per snapshot attempt for the same reason). One behaviour gained:
an empty bucket reports `pending: 0` up front, so `watchSchemas()` resolves instead of
waiting for a last-entry marker that never comes.

Verified in Chrome: initial replay of all schemas on load, a live `ALTER TABLE users ADD
COLUMN` applied 1s after the DDL, `nats consumer ls KV_schemas` shows one consumer per
page (a random name, no `oc_` prefix), and zero Publish Violations in the server log.
`Kvm`/`kv.get()` stay in use for direct reads — only the watch changed.

### 1.16 `decode_integrity.py` has the same stale-`INIT`-stream bug `snapshot.py` had

`zb.TOPOLOGY["streams"]["init"]` is `"INIT"` — a generic label in `topology.json`, not
a real stream name; only `INIT_ACME`/`INIT_GLOBEX`/`INIT_PUBLIC` exist since §1.12's
tenant split. `decode_integrity.py:265` still passes it directly to
`js.pull_subscribe(..., stream=...)`, same shape of bug already found and fixed in
`snapshot.py` this session (§1.6/earlier work). Confirmed live: NATS server log shows
`Publish Violation - Subject "$JS.API.CONSUMER.CREATE.INIT...."` for a real client
connected as alice — the request never reaches a real stream. Fix is the same pattern
already applied to `snapshot.py`: resolve the correct `INIT_<TENANT>`/`INIT_PUBLIC`
stream name from `topology.json`, and add the tenant token to the chunk-filter subject
(`init.snap.<tenant>.<table>.<snapshot_id>.>`, not `init.snap.<table>.<snapshot_id>.>`).
Not yet applied — found right before a deliberate infra teardown, no time to verify a
fix in the same session.

### 1.17 CLOSED — `applySchema` failed on a dropped column while `<table>_view` existed

Seen twice this session on a fresh page load, with the schema push itself arriving
fine: `ALTER TABLE users DROP COLUMN zb_probe_col` in PostgreSQL reaches the client and
`applySchema` logs `SCHEMA ERROR: … SQLITE_ERROR: error in view users_view: no such
table: main.users`. The `ADD COLUMN` half of the same round-trip applies cleanly.
SQLite's `ALTER TABLE … DROP COLUMN` re-validates every schema object that references
the table, and `users_view` is one; the copy-based `rebuildPreservingData` fallback
takes the `DROP TABLE` + `RENAME` route with the view still defined, which is where the
"no such table" comes from. Fix: drop `<table>_view` *before* the
alteration; the recreate after it was already there, just too late. Verified in Chrome:
the same `ADD COLUMN` / `DROP COLUMN` round-trip now applies both halves via ALTER with
rows preserved. Found while verifying §1.14.

### 1.15 CLOSED — the `DbAllocatedKey` "registry leak" was `keys.py` publishing to a malformed subject

`keys.py` reported three failures in one run: no verdict for the refused `users` write,
an unrelated `test_types` write that never landed, and `bridge_refused_tables` at 3
instead of the expected 2. Reproduced against a fresh Docker stack with `LOG_LEVEL=info`;
the bridge log names the cause in one line:

```txt
⚠️  Malformed mutation subject 'mutation.127.0.0.1.users.insert' (error.MalformedSubject):
    dead-lettering, and no verdict is addressable
⚠️  Malformed mutation subject 'mutation.127.0.0.1.test_types.insert' ...
```

`keys.py` read the principal out of `NATS_URL` with `rsplit("@")` — under nkey auth the
URL is `nats://127.0.0.1:4222`, no user, so the "principal" became the host and the
subject gained two tokens. `mutate.py` and `rowsize.py` already fall back to `alice` for
exactly this case; `keys.py` did not. Both writes were dead-lettered before the write
path ever saw them, which is why no verdict came back (the subject is what failed, so
there is no address) and why the second write "leaked".

The third number was the environment, not the write path: `demo_key_migration` (the
emitter's no-PK-then-composite-key demo fixture, §1.15's own fix) is not in
`topology.json`'s `public_tables`, so preflight refuses it at boot with `no_cdc_subject`.
The baseline is 3, not 2 — the "confirmed by boot log" 2 predated that fixture. And
`keys.py` compared against **0**, which can never hold on a stack with any refused
table.

**The write path is correct.** With the subject fixed, the bridge publishes
`{"status":"rejected","reason":"DbAllocatedKey"}` on `mutation_ack.alice.<msg_id>`, the
following `test_types` write is `accepted`, the registry stays at 3, and `keys.py` passes
7/7. `mutation_listener.zig` still holds no reference to `refused_tables` — the invariant
its comment claims held all along.

Fixed in `keys.py`: principal via `ZB_PRINCIPAL` / URL user / `alice`, same rule as the
sibling scripts; the tenant for the cross-table write is looked up for that principal
(`zebridge_user_tenants`), falling back to `OPEN_TENANT`; and the registry check is
before-vs-after, like `invalidate.py`, not against zero. One wording fix in the bridge:
the operator-fault log line said "the schema and SYNC_RULES disagree" for every
operator fault, including `DbAllocatedKey`, which is a key-shape problem — now generic,
deferring to the reason already logged by `tableMeta`.

Lesson, the same one as §2.19 and `mutate.py`'s own comment: a scenario that fails on
"the client cannot tell X from Y" should first check the bridge log for the *subject*
it published. A malformed subject produces exactly the symptoms of a write-path bug,
and two unrelated-looking failures from one message are a hint that the message never
got as far as the code under test.

### 2.13 A wildcard inbox made every pull request spawn another

`jetstream.zig` `fetch` mints a unique reply subject per request but reads from a
**wildcard** subscription (`<prefix>.*`), so it also receives replies to *earlier*
fetches still parked on the server until their `expires`.

A late 408 therefore ended the *current* fetch. The caller re-fetched at once, leaving
another request parked, which produced another late 408 — self-sustaining, settling at
`expires / period` parked pulls.

Measured on a completely idle stream: 500 ms expiry, a new fetch every 48 ms,
`num_waiting=10`, ~21 pull requests/sec. Two consumers made ~17 msg/s in *and* out with
nothing to deliver. Unchanged by log level, which is what ruled out debug output.

Fix: ignore any **status frame** whose subject is not this fetch's reply subject, and keep
waiting. Status frames only — JetStream delivers a *data* message with its original subject
(routed by subscription id), so matching those against the inbox drops every real message.
`test-e2e` caught exactly that (`jetstream_pull_test`, "basic fetch": expected 2, got 0).

Callers could not have caught it: `fetch` reports 408 in `batch.err`, not as an error
return, so `catch { continue; }` never fires and an empty `batch.messages` looks normal.

### 2.14 The WAL idle path slept on a timer instead of the socket

`PQgetCopyData` in async mode returns 0 the moment libpq's buffer is drained, so the
1 ms sleep woke the loop ~1000×/s to find nothing: `iters=11617 idle=11616` per 15 s
window, ~4% CPU against an idle database.

Fix: `poll()` on `PQsocket`, with the timeout only bounding the keepalive check. Wakes
the instant PostgreSQL writes, so latency is unchanged.

⚠️ Not a latency bug — publish latency measured 8.6–13.8 ms (median 10.4) commit→NATS
both before and after. An early 41 ms reading was a measurement artifact: the harness
spawned a Python interpreter per message.

### 2.15 `Kvm.open()` on `$KV.snapshots` was missing `allow_direct` — same bug class as §1.12's `$KV.tenants` fix, never applied to the second bucket

`resolveTenant()`'s `kvm.open(topology.kv.tenants, { allow_direct: true })` fix (§1.12 part
3) was never mirrored onto the snapshot-replay code's own `kvm.open(topology.kv.snapshots)`
call — it opened the bucket with no options, defaulting `allow_direct` to `false`.

`$KV.schemas` tolerates this silently, because it is deliberately wholesale-granted
(`$JS.API.STREAM.MSG.GET.KV_schemas` — schema disclosure stays unscoped by design). `$KV.snapshots`
is not: only the exact-key Direct Get path is granted per tenant, so every `get()`/`watch()`
against it was a genuine `Publish Violation`.

Found via `nats-server.log`, not guessed: every one of five test principals hit
`Publish Violation - Subject "$JS.API.STREAM.MSG.GET.KV_snapshots"`, five times each in a
sub-20 ms cluster — matching `SNAPSHOT_REQUEST_ATTEMPTS` exactly. `waitForDescriptor`'s
`kv.watch()` was throwing immediately rather than actually waiting the timeout, so the
5-attempt retry loop burned through in milliseconds instead of minutes, then gave up and
`syncedTables.delete(table)`d — silently discarding every subsequent CDC event for that
table, no error, no symptom but a permanently-wrong count. Some clients still showed partial
data because a plain `snapKv.get()` (issued once, before the broken retry loop) apparently
resolves via a different internal path that happened to succeed when a descriptor already
existed from another client's earlier request — explaining the "some counts wrong,
inconsistently, across principals on the same tenant" symptom rather than a uniform failure.

Fixed the same way as the original: `{ allow_direct: true }` on the `kvm.open()` call.

### 2.16 Removing a format fallback and removing crash containment are not the same diff

Part of this session's wire-format cleanup (schema always JSON, CDC/snapshot always
MessagePack, `--json` removed — see `PROTOCOL.md` §3/§4) correctly stripped the now-dead
`catch { JSON.parse(...) }` fallback from `App.tsx`'s snapshot-chunk and CDC-event `decode()`
calls, since the bridge can no longer produce JSON on either path. But the instruction also
removed the `try`/`catch` entirely, not just the JSON guess — leaving a bare `decode(msg.data)`
with nothing catching a genuine decode failure.

Both call sites sit inside a fire-and-forget `void (async () => { for await (...) })()` — an
un-awaited IIFE the outer `try`/`catch` around consumer setup cannot reach. An uncaught throw
inside either loop silently ends the whole loop for that table (snapshot replay) or that
entire stream — every table it carries (CDC consumption) — with nothing visible but an
easy-to-miss unhandled promise rejection. No error banner, no crash, just permanently stalled
sync.

Restored crash containment at both sites: catch, log a visible `SYS ERROR` (table/stream, seq,
the actual error), `ack()`, and `continue` — skip the one bad message, not the whole pipeline.
The lesson to keep: a format fallback (try msgpack, guess JSON) and crash containment (don't
let one bad message kill the loop) can live in the same `try`/`catch` and look like the same
diff to remove, but they are two different guarantees. Removing the first because the bridge
proved it unnecessary does not make the second unnecessary too.

### 2.17 `mutation.<principal>.<table>.update` was routed through the INSERT-shaped upsert, so a genuine partial update could fail on a column it never touched

`applyUpsert` (`mutation_listener.zig`) handled both `insert` and `update` mutations
identically: `INSERT (cols from data) VALUES (...) ON CONFLICT (pk) DO UPDATE SET ...`.
PostgreSQL must be able to satisfy the INSERT branch even when only the UPDATE branch can
ever fire for a row the client already knows exists — every `NOT NULL` column with no
database-level default has to be present in `data`, or the statement is rejected before
`ON CONFLICT` is even considered.

A correctly-written partial update (`{some_text, updated_at}` only, deliberately omitting
every unchanged column — exactly what `PROTOCOL.md` §4's UPDATE omission rule already asks a
*reader* to expect) against a table whose `inserted_at` is `NOT NULL` with no default failed
`23502`, reproduced live via two different principals' own Update-button presses through the
UI. This was `PROTOCOL.md` §7.2's documented "item 2" limitation ("`data` omits a NOT NULL
column that has no DEFAULT. The INSERT fails, forever") — but that limitation only makes
sense for a genuine INSERT; nothing about an UPDATE against a row that already has an
`inserted_at` should need to re-supply it.

Fixed with a dedicated `applyUpdate` — a real `UPDATE ... SET ... WHERE pk AND
version-guard`, mirroring `applyDelete`'s WHERE-guard/tie-break shape (there is no `EXCLUDED`
pseudo-table in a plain UPDATE, so the tie-break reuses the same bound parameter position
instead of `EXCLUDED.col`) rather than `applyUpsert`'s INSERT-shaped one. `.insert` still
goes through `applyUpsert` — correctly, since client-generated-key retry-safety needs upsert
semantics — `.delete` is untouched, only `.update` moved.

⚠️ **This also changes a documented protocol invariant, not just fixes a bug** — see
`PROTOCOL.md` §7.4's "There is no 'row not found'" section, corrected in the same pass as
this fix: an `update` naming a key that does not exist no longer silently creates a phantom
row. It now runs through the exact same classify logic `applyDelete` already used (`affected
== 0` → check whether the row exists/is tombstoned → `.row_deleted` if not, `.stale`
otherwise), verified by reading `buildClassify`/the pipeline result handling in
`mutation_listener.zig` rather than assumed. A client relying on the old "update always
succeeds" behavior needs to handle `row_deleted` on update the same way it already does on
delete.

### 2.18 `zebridge_scope_reads_by_tenant()` had no open-tenant carve-out — audience said everyone, contents said only the open tenant itself

`zebridge_scope_writes_by_tenant()`'s write policy has always granted every principal
read/write on a row mapped to the OPEN tenant (`init.write.template.sql`, `zb_tenant_write`:
`... OR tenant_col = '${OPEN_TENANT}'`) — SECURITY.md's own "Open means open to READ"
already documented this as the intended behavior. `zebridge_scope_reads_by_tenant()`'s read
policy (`init.core.template.sql`, `zb_reader_all`), added later for snapshot scoping,
never got the same clause — it only matched `coalesce(zb.tenant,'')='' OR tenant_col =
zb.tenant`, nothing else.

CDC doesn't apply RLS (it isn't a query), so the open tenant's rows already reached every
client correctly via `CDC_PUBLIC`'s wildcard subject — audience was right. But a snapshot
*is* a query, gated by this exact policy, and a non-open-tenant client's snapshot connection
never had `zb.tenant` equal to the open value, so the policy never matched — contents was
wrong. A client's local copy of an open-tenant row ended up depending on whether a live CDC
write for it happened to arrive while connected, not on what its own snapshot returned:
reconnect before any further write touched that row, and the row silently vanished from a
fresh replica. Found live on the `counter_tenant` demo table — three tenants' worth of rows
in Postgres, every non-open-tenant client reporting a count one row short of what CDC alone
would eventually converge them to, and inconsistently so depending on connection timing.

Fixed by adding the identical carve-out to the read policy:
`... OR tenant_col::text = '${OPEN_TENANT}'`, mirroring `zb_tenant_write` exactly. Verified
live: `SET LOCAL ROLE bridge_reader; SELECT set_config('zb.tenant','acme',true);` against
`counter_tenant` now returns both `acme`'s own row and the `_default` row. Every existing
`$KV.snapshots` descriptor for a tenant-scoped table predates this fix and was purged —
a cached descriptor's own staleness check (§1.12) only catches a CDC-coverage gap, not "the
RLS policy that generated this content has since changed," so a policy fix like this one
needs its own manual purge, not just a restart.

### 2.19 A table with no CDC route didn't fail fast — it blocked, retried the identical dead end, and could take the whole bridge down

Every published table's `cdc.<table>.<op>` needs a stream willing to accept it:
tenant-scoped tables are covered by `CDC_<TENANT>` (one exists for every declared
tenant), untenanted ones only if `topology.json`'s `public_tables` lists them —
exactly the list `nats-init`/`scripts/native/up.sh` use to build `CDC_PUBLIC`'s
subject filter. Nothing enforced that a table actually satisfies one of those two
conditions before the bridge tried to publish for it.

⚠️ **The failure this produces is not "CDC silently doesn't arrive."** `Publisher.publish`
(`src/nats_publisher.zig`) is synchronous and ack-waiting — it sends the JetStream
publish and blocks for the reply. When no stream's subject filter matches, nothing
ever generates that reply, so the call doesn't fail fast, it blocks for the full
client-side publish timeout (~5s), then fails with `NoStreamResponse`/`Timeout`,
reconnects, and retries — the **identical** unrouted publish, which fails the
identical way. Enough of that exhausts `batch_publisher`'s 6-attempt outer retry
budget and hits `FATAL: stopping bridge to prevent WAL overflow` — the bridge's own
deliberate safety shutdown, correctly triggered, for a cause that had nothing to do
with what it guards against.

Found live via `invalidate.py`'s `SCRATCH` fixture (a dynamically-created test
table, never declared in `topology.json`): its act 3 "after the fix" `INSERT`
reliably reproduced the full sequence — 5s stall, reconnect storm, and (once,
while chasing it) an actual bridge shutdown. Confirmed empirically with debug
timestamps around every publish call: `js.publish(cdc.zb_invalidate.insert)`
entered at one timestamp and didn't fail until **5 seconds later** — not a race,
a genuine wait for a reply that would never come — and `nats stream subjects
CDC_PUBLIC`/`CDC_GLOBEX` confirmed no stream's filter matched that subject at all.

This is the empirical version of the risk SECURITY.md §1.4's migration checklist
already named ("a new public table needs one `nats stream edit CDC_PUBLIC` subject
addition") — what was missing was *what happens if that step is skipped*, and the
answer turned out to be far worse than "no CDC for that table."

**Fixed at the source, not by making publishing more resilient.** A new
`Topology.isCdcRoutable(table, is_tenant_scoped)` (`src/topology.zig`) answers
the question locally, from the bridge's own boot-time config — no NATS round trip,
no dependency on live stream state (an operator manually widening a stream's live
subjects without also updating `topology.json` and restarting does **not** make
the bridge trust it, deliberately: the bridge's belief about what's routable comes
from its own declared config, the same way `SYNC_RULES`/`TENANT_RULES` already
work, not from probing NATS at runtime). A table that fails it is refused with a
new reason, `no_cdc_subject` (`src/refused_tables.zig`) — checked in two places,
mirroring `no_primary_key` exactly: `preflight.run`'s boot pass (a second pass
over every published table, independent of the PK/identity findings loop) and
`event_processor.zig`'s DDL-driven runtime path (`packDdlToSlot`, right where
`no_primary_key` already sits). Once refused, the existing machinery does the
rest — `shouldDrop()` drops its CDC events before they ever reach `doPublish`, and
`publishSuspension` tells clients why, so the write-side gate (§1.6c) also blocks
optimistic writes to it.

⚠️ **`zebridge_user_tenants` is exempt, not `isInternalTable`.** Its row changes
never touch `cdc.<table>.<op>` at all — a dedicated path transforms them into
`$KV.tenants.<principal>` instead, deliberately, so the tenant roster is never
broadcast (§1.12). It still needs every *other* preflight check (no-PK, version
columns, writability), so it isn't globally internal — just excluded from this
one, by name, in both call sites.

**A real, separate gap this surfaced along the way:** `zebridge_gc_watermark`
was never in `topology.json`'s `public_tables`, despite PROTOCOL.md §7.5
explicitly documenting `cdc.zebridge_gc_watermark.update` as a subject clients
read. Its CDC route had silently never worked in this deployment — nothing had
triggered a live GC-watermark update yet to expose it. Fixed: added to
`public_tables`, and `CDC_PUBLIC`'s live subjects edited to match
(`topology.json` changing alone does not retroactively update an
already-created stream — the same manual step SECURITY.md §1.4 describes).

**Verified end to end**, with debug instrumentation added and then fully
reverted (`git diff` confirmed clean before continuing): a table created with no
`public_tables`/`TENANT_RULES` entry is now refused **immediately** at DDL time
— a real row insert against it completes in milliseconds (measured: 32ms,
psql overhead included) instead of blocking, and `bridge_nats_reconnects_total`
stays at `0` throughout. `invalidate.py`'s act 3 updated to match the new,
correct behavior: `SCRATCH`'s `no_primary_key` refusal still lifts without a
restart, but it now correctly *stays* suspended afterward, for `no_cdc_subject`
— it was never declared routable, and making it fully resume would need
`topology.json` changed and the bridge restarted, defeating the "no restart
needed" point that act is actually making (about the no-PK refusal, not about
CDC routing).

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
| composite | either | ✅ | ✅ |

**The asymmetry is the point: FULL rescues CDC, it does not rescue snapshots.**
Snapshot chunking uses keyset pagination, so it needs an *ordering*, which only a key
provides — no replica identity setting can substitute, because the constraint is about
*ordering for pagination*, not about *identifying a row*. The number of key columns is
not part of that constraint: pagination compares the whole key as a row value
(`("a","b") > (…) ORDER BY "a","b"`), so composite keys page like any other.

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

### 4.4 What each metric actually answers

Added after 2.9, where three plausible-looking numbers all failed to locate a 300×
regression. Each of these exists because a question could not be answered without it.

| metric | question it answers |
| --- | --- |
| `bridge_wal_confirmed_lag_bytes` | **Is the bridge behind?** WAL past `confirmed_flush_lsn`, which moves on every ACK. |
| `bridge_wal_lag_bytes` | **Will the disk fill?** WAL past `restart_lsn`, which PostgreSQL only advances at checkpoints — so it plateaus at a few MB on a perfectly healthy bridge and can never answer the first question. Confusing the two cost an afternoon. |
| `bridge_queue_usage_percent` | Is the **flush** side backed up? Silent about the reader. |
| `bridge_cpu_seconds_total` | How busy is the process, across all threads? `rate(...[1m])` = cores used. A single-threaded reader at `1.0` is *by definition* the bottleneck. |
| `bridge_max_rss_bytes` | Is memory what was configured? Should sit near `2^BASE_BUF × RING_BUFFER_COUNT` + ~400 MB metadata, since the slab is pre-allocated, not grown. |
| `bridge_refused_tables` | Is any table suspended — no PK, undecodable column, oversized row? Alert on `> 0`; the log line says which and why. |

And the `LOOP` line next to `METRICS` every 15 s, which is the reader's profile:

```txt
LOOP iters=1407805 idle=10274 recv_ms=139 proc_ms=1494 cpu=31%
```

| field | reading |
| --- | --- |
| `iters` | WAL loop iterations in the interval |
| `idle` | iterations that found nothing and slept 1 ms — **high `idle` is good**: the bridge is waiting on PostgreSQL, not struggling |
| `recv_ms` | ms inside `receiveMessage` (libpq + framing) |
| `proc_ms` | ms decoding tuples and packing them into the ring buffer |
| `cpu` | process CPU over the interval, all threads |

The failure signature to recognise: **`recv_ms` approaching the interval length with
`idle=0` and `cpu≈100%`** — a reader that cannot drain its socket. The healthy shape is
the opposite: `idle` in the thousands, `recv_ms` in the tens.

CPU comes from `getrusage(RUSAGE_SELF)` rather than `/proc`, so it is one POSIX call on
both platforms — but note `ru_maxrss` is **bytes on Darwin, kilobytes on Linux**, one of
the few genuinely divergent POSIX fields (normalised in `utils.maxRssBytes`).

---

### 4.5 The setup, in two times and four places

The layers do not live in the SQL files. That is the single thing that makes this hard to
hold in your head, so it is worth stating plainly before the tables: **a layer is a
capability assembled from four places, and half of it is not SQL at all.**

#### Two times, because a function cannot scope a table that does not exist

The templates *define* functions. The DBA *invokes* them — afterwards, once the
application's own migrations have created the tables. Nothing in
`init.{core,write}.template.sql` applies itself to anything: every mention of
`zebridge_grant_edge_writes`, `zebridge_install_write_guards`,
`zebridge_scope_writes_by_tenant` or `zebridge_scope_publication_to_one_tenant` outside its
own `CREATE OR REPLACE` is a comment.

```
T0  init.core.template.sql [+ init.write.template.sql]
        roles, functions, DDL triggers, an EMPTY publication
T1  your application's migrations
        the tables themselves
T2  DBA activation, per table
        the function calls above + ALTER PUBLICATION … ADD TABLE
T3  bridge environment  + restart
        TENANT_RULES, SYNC_RULES
T4  NATS conf + reload  (reload NATS, not the bridge)
        per-principal subject grants
```

#### Four places, per layer

| layer | SQL @T0 | SQL @T2, per table | bridge env @T3 | NATS conf @T4 |
| --- | --- | --- | --- | --- |
| **1** full read | core: reader role, publication, DDL triggers | `ALTER PUBLICATION … ADD TABLE t` | — | `cdc.>`, `init.>` |
| **2** scoped read | + `zebridge_user_tenants` | tenant column exists; `ALTER PUBLICATION … (cols)` for column rights | `TENANT_RULES=t:col` ⟵ **restart** | `cdc.<tenant>.>` per principal |
| **3** write | write file: writer role, guards | `grant_edge_writes(t)`, `install_write_guards(t,…)`, `scope_writes_by_tenant(t,col)` | `SYNC_RULES=t:…` ⟵ **restart** | `mutation.<principal>.>`, `mutation_ack.<principal>.>` |

⚠️ **Only the env vars need a bridge restart.** Granting a principal a tenant
(`INSERT INTO zebridge_user_tenants`) is resolved per statement inside the RLS policy and
takes effect on the next mutation; adding a NATS principal reloads **NATS**, not the bridge.
Conflating these three is what makes the workflow feel clumsier than it is (SECURITY.md).

#### Multi vs single tenant are alternatives, not a nesting

This is the distinction that keeps collapsing. They are two different ways to *be* layer 2,
and they exclude each other:

| | scoped by | shape | CDC | snapshot |
| --- | --- | --- | --- | --- |
| **multi-tenant** | the **subject** — `TENANT_RULES` + per-tenant NATS grants | one bridge, N tenants | ✅ | ❌ (§1.12) |
| **single-tenant** | the **publication row filter** — `zebridge_scope_publication_to_one_tenant(t, col, value, pub)` at T2 | one bridge **per tenant** | ✅ | ✅ |

The single-tenant shape is coherent across both paths for one reason: the filter lives in
the publication, and *both* CDC and the snapshot read the publication
(`snapshot_listener.zig:1584`). The multi-tenant shape scopes CDC by subject, which the
snapshot has no equivalent of — that is §1.12.

#### Layer 2 is barely SQL

Its CDC half needs nothing from the write file: no writer role, no RLS, no mapping table.
The principal→tenant decision is enforced by NATS grants (`alice` holds `cdc.acme.>`), not
by PostgreSQL. Tenant-scoped **read-only** CDC is therefore reachable with the core file
alone, plus a tenant column, `TENANT_RULES`, and conf.

Only layer 2's *snapshot* half needs `zebridge_user_tenants`, for the reader RLS policy —
which is why that table belongs in **core** rather than the write file if §1.12 is ever
built. The split boundary is read-vs-write **capability**, not which tables are involved.

Column rights are orthogonal to all of it and also live at T2:
`ALTER PUBLICATION my_pub SET TABLE orders (id, total)` bounds both paths with no bridge
involvement — subject to §1.8's exclusion, which dissolves only when the tenant column is
inside the replica identity.

#### The entry point — BUILT (`zebridge_enable`, core template)

T2 used to be a sequence of remembered calls, which is why the whole refused to come into
focus. One statement now composes it and **prints what it cannot reach**:

```sql
SELECT * FROM zebridge_enable('public.orders',
                              tenant_col    => 'tenant_id',
                              writable      => true,
                              version_col   => 'updated_at',
                              tombstone_col => 'deleted_at');   -- dry run by default
```

⚠️ **`dry_run` defaults to TRUE.** It grants, guards and enables RLS in one statement;
showing the plan first is the cheap half of that trade.

It returns a `(step, status, detail)` plan covering T2 *and* the T3/T4 lines it can never
set — `TENANT_RULES=…`, `SYNC_RULES=…`, and the NATS grants — because those live outside the
database and always will.

Three things it enforces that a remembered sequence did not:

- **Scoping before publication.** `zebridge_publication_guard` refuses
  `ALTER PUBLICATION … ADD TABLE` on a table with neither a row filter nor RLS, so grants,
  guards and RLS run *first* and the publication last. Discovered by writing it in the wrong
  order and being refused mid-apply — which is exactly the half-applied activation the
  preflight now predicts with an ERROR row instead.
- **A scoping decision is mandatory.** Publishing unscoped is refused up front, naming the
  three ways out: `writable => true` with a `tenant_col` (enables RLS), `public_reason => …`
  (records the decision in `zebridge_public_tables`), or
  `zebridge_scope_publication_to_one_tenant()` for the one-bridge-per-tenant shape.
- **Profile awareness.** `writable => true` against a `ZB_PROFILE=readonly` database fails
  with that named reason rather than `function does not exist`. It lives in the core half and
  dispatches write-path calls through `EXECUTE` precisely so this stays possible.

⚠️ Writing it also surfaced a live bug in `zebridge_scope_writes_by_tenant`: its `RAISE
NOTICE` used `%s` where PL/pgSQL's placeholder is `%`, so it printed
`TENANT_RULES=orderss:tenant_id` — the value followed by a literal `s`. A DBA copying that
line would have configured a table that does not exist. Fixed.

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

---

## 7. Database Schema and Roles

### Tables

#### Server-Side Tables (`init.*.sql`)
| Table Name | Role / Description |
| :--- | :--- |
| `zebridge_ddl_events` | The DDL transport mechanism. Because PostgreSQL's logical replication (WAL) natively ignores DDL statements (like `ALTER TABLE`), ZeBridge relies on event triggers (`zebridge_ddl_trigger_fn` and `zebridge_drop_trigger_fn`) to intercept schema changes and log them as `INSERT`s in this table. These `INSERT`s *are* emitted via the WAL, allowing the bridge to read schema changes in strict stream order with CDC data and publish them to NATS KV. |
| `zebridge_public_tables` | Registers tables that are deliberately published unscoped to all tenants. It acts as an escape hatch for public shared datasets (e.g., product catalogs or currency lists) and justifies why they bypass the scoping guards. |
| `zebridge_gc_watermark` | Tracks the oldest standing tombstone. Read by clients (via CDC) to determine the maximum allowed offline window before their soft-deleted rows are completely swept and discarded. |
| `zebridge_user_tenants` | Maps NATS principals to their corresponding PostgreSQL tenant IDs. Used by RLS policies and triggers to ensure edge writes correspond to the principal's tenant and route deletes correctly. |

#### Client-Side Tables (`web-consumer/src/App.tsx`)
| Table Name | Role / Description |
| :--- | :--- |
| `_zebridge_outbox` | Holds optimistic edge writes sent by the client that are pending a confirmation/verdict from NATS and the bridge. |
| `_zebridge_sync` | Stores the `global_last_lsn` (PostgreSQL WAL position) successfully applied by the consumer, allowing it to discard already-seen CDC events. (Contains a legacy `global_last_seq` column). |
| `_zebridge_stream_seq` | Tracks JetStream sequences (`last_seq`) grouped by stream. Lets the consumer detect if it has fallen off the back of a stream's retention window when reconnecting. |

<br>

### PostgreSQL Functions (`init.*.sql`)

The functions are divided into read-only configurations, edge-write guard configurations, and composition helpers.

| Function | Usage / Role |
| :--- | :--- |
| **Helpers / Entry Points** | |
| `zebridge_enable()` | The main orchestration entry point that combines grants, guards, RLS policies, and publication logic. Can dry-run to print its plan or apply configurations in a single call. |
| `zebridge_is_internal_table()` | Checks if a table belongs to ZeBridge (or standard migration tools) to hide it from schema publications and avoid pointless client-side syncing. |
| **Read/CDC Operations** | |
| `zebridge_scope_reads_by_tenant()` | Enables RLS on a table to restrict snapshot `SELECT` operations to the active session's tenant. |
| `zebridge_publication_guard()` | Event trigger function protecting against unscoped `ALTER PUBLICATION ... ADD TABLE`. Rejects publications of tables without row-filters or RLS unless they are registered as public. |
| `zebridge_audit_publications()` | Audits the database to ensure no tables are published without appropriate tenant scoping. |
| `zebridge_ddl_trigger_fn()` | Event trigger running on `ddl_command_end` to capture modified table schemas directly from the catalog and log them as `INSERT`s into `zebridge_ddl_events`. |
| `zebridge_drop_trigger_fn()` | Event trigger running on `sql_drop` to inform clients when tables are permanently deleted by logging the event into `zebridge_ddl_events`. |
| `zebridge_prune_ddl_events()` | TTL function that deletes events older than 2 days from `zebridge_ddl_events` to bound table growth. |
| `zebridge_widest_row()` | Scans a table's data types to evaluate its byte size floor, ensuring it fits inside NATS message ceilings. |
| `zebridge_oversized_defaults()` | Detects column default values that would break the NATS message size budget. |
| **Write/Ingress Operations** | |
| `zebridge_grant_edge_writes()` | Grants `SELECT`, `INSERT`, `UPDATE`, and `DELETE` on a table to the `bridge_writer` role to permit edge mutations. |
| `zebridge_install_write_guards()` | Helper function that sets up triggers for `bump_version`, `soft_delete`, and `guard_tenant` on a table. |
| `zebridge_remove_write_guards()` | Clears the write guard triggers, useful when an admin needs to perform physical cleanups on a table that is restricted to soft-deletes. |
| `zebridge_bump_version()` | Write trigger ensuring that any write omitting the version column (e.g., `updated_at`) gets appropriately stamped with the current timestamp. |
| `zebridge_soft_delete()` | Write trigger converting physical `DELETE` statements on tables with a tombstone column into soft-deleting `UPDATE` operations. |
| `zebridge_guard_tenant()` | Write trigger enforcing that a row's tenant is present and valid against NATS subject parameters (no whitespace or wildcards). Discards or falls back to the writer's mapped tenant. |
| `zebridge_scope_writes_by_tenant()`| Multi-tenant configuration for edge writes. Sets up RLS and forces the creation of a unique `REPLICA IDENTITY` index (combining PK and tenant) so PostgreSQL routes `DELETE`s properly over CDC. |
| `zebridge_scope_publication_to_one_tenant()` | Single-tenant alternative that uses a PostgreSQL publication row filter (`WHERE tenant = 'acme'`) to pin an entire publication to a specific tenant value. |
| `zebridge_audit_write_guards()` | Audits all tables and reports which ones possess ZeBridge's write guard triggers. |
| `zebridge_audit_sweeper()` | Checks if the internal sweeper principal (`zb_sweeper`) has any unreachable tenants, thereby helping track tenants whose tombstones won't get collected. |

---

## 8. Configuration Orphans

The following constants in `src/config.zig` are declared but never referenced by any other `.zig` source file. They are effectively dead code:

- **PostgreSQL (`Postgres`)**: `connection_timeout_ms`, `replication_receive_timeout_ms`, `wal_sender_timeout_seconds`, `max_wal_retention_gb`
- **HTTP (`Http`)**: `metrics_path`, `health_path`
- **WAL Monitoring (`WalMonitor`)**: `warning_threshold_bytes`, `critical_threshold_bytes`
- **Snapshots (`Snapshot`)**: `max_concurrent_snapshots` (commented as declared/never read), `poll_interval_ms`
- **Metrics (`Metrics`)**: `log_interval_seconds`, `debug_enabled`
- **Retries (`Retry`)**: `flush_stall_timeout_ns`
- **Threading (`Threading`)**: `wal_monitor_threads`, `snapshot_generator_threads`, `http_server_threads`, `main_loop_sleep_ms`
- **Buffers (`Buffers`)**: `conninfo_buffer_size`, `url_buffer_size`

---

## 9. Dynamic Tenant Provisioning & JWT Auth

ZeBridge's runtime publisher is entirely dynamic and does **not** rely on the `tenants` array in `topology.json` during operation. The `topology.json` tenant list is currently only used for a boot-time pre-flight check (to ensure streams exist before starting) and by the `nats-init` script to physically create the streams.

When migrating to a **JWT / Operator Authentication** model, multi-tenancy can be managed dynamically with zero config reloads or bridge restarts:
1. **Backend provisions stream:** The application backend dynamically creates the `CDC_<TENANT>` and `INIT_<TENANT>` JetStream streams via the NATS API.
2. **Backend mints JWT:** The backend embeds the exact tenant boundaries (`cdc.<tenant>.>`) into the short-lived NATS user JWT.
3. **Link User in DB:** A new mapping is inserted into `zebridge_user_tenants`.

Once these steps occur, ZeBridge seamlessly picks up the new `tenant_id` from the Postgres WAL row, dynamically constructs the `cdc.<tenant>.<table>.<op>` subject, and publishes it. Since the stream was already created in JetStream by the backend, the data lands perfectly. 

> [!NOTE]
> **To Be Tested:** This fully dynamic mechanism needs to be empirically verified. The test should involve manually provisioning a new tenant stream in NATS, inserting a new user/tenant mapping row directly into `zebridge_user_tenants` (e.g., a new tenant that is intentionally omitted from `topology.json`), and verifying that ZeBridge routes and publishes the runtime CDC events to the new stream correctly without any restarts.
