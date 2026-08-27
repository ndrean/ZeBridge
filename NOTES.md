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

### 1.2 CLOSED — RENAME COLUMN travels as a `renamed` hint; clients keep their values

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

**Built 2026-08-24, exactly as sketched above.** Three pieces, one per layer:

- **Trigger** (`init.core.template.sql`): the columns capture gains `'attnum',
  a.attnum` (bridge-facing, like `oid`/`typtype` — never forwarded to clients).
  Where `last_def` is already in hand for the dedup check, a
  `jsonb_to_recordset` self-join on attnum with differing names produces
  `'renamed': {"new_name": "old_name"}`, merged into `schema_def`. The dedup
  comparison strips `renamed` from **both** sides — it describes the transition,
  not the shape, and comparing it would emit one spurious event on the first
  unrelated DDL after a rename. A `last_def` written by the older trigger has no
  attnum, joins nothing, and yields no hint — the first rename after the upgrade
  degrades to the old drop+add behaviour, and every one after is detected.
- **Bridge** (`event_processor.zig`): `renamed` passes through at the payload
  root, next to `table` (PROTOCOL.md, schema payload). Boot schemas never carry
  it; absent means nothing renamed.
- **Client** (`App.tsx` `applySchema`): the add/drop diff is computed *as if*
  the renames had already happened, and the honoured pairs (old name present
  locally, new name not) run as `ALTER TABLE … RENAME COLUMN` — after the view
  drop (§1.17), before the add/drop loops and the copy-based fallback, both of
  which match columns by name and so see post-rename names.

Verified live, both halves of the upgrade boundary: a rename whose previous
event predated the trigger upgrade produced no hint (documented degradation);
the next one produced `{"name": "full_name"}` in `zebridge_ddl_events`, the KV
payload carried `"renamed":{"name":"full_name"}` at the root, and the client
logged `~[full_name→name] via ALTER (rows preserved)` with no error. Value
survival is SQLite's own RENAME COLUMN guarantee once that path is taken; what
the hint changes is *which* path is taken.

### 1.3 CLOSED — RING_BUFFER_COUNT settled at 32768

The original rationale (in `config.zig`) was: 32768 slots ≈ 1092 ms at 30K events/s,
which *covered* one NATS reconnect interval when that was 1000 ms. Reconciling the
hardcoded 2000 ms into config broke that invariant — the buffer now covers about half
a reconnect.

Safe, not broken: a full ring backpressures the WAL reader, so events are delayed,
never dropped. Restoring the old property would mean `RING_BUFFER_COUNT=131072`
(≈2184 ms), doubling memory.

**Settled: leave it at 32768.** Metrics show PG reconnects dominate and NATS
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

- 1 : undefined`. A client connecting after the fact replays from its own persisted
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

### 1.7 CLOSED — both former deliberately-not-fixed items are resolved (2026-08-24: schema events counted separately; pk moved to the payload root)

- **`cdc_events_published` undercounts — resolved sideways (2026-08-24).** The counter
  increments only in the row-event branches, so DDL/SCHEMA publishes were invisible.
  Folding them in was the wrong fix: that counter's value is *trusted to equal row
  events delivered* — the README burst method and `speed.py` both poll it for exact row
  counts, and rare schema traffic mixed in would skew exactly those measurements. So
  schema publishes now have their own counter, `bridge_schema_events_published_total`
  (DDL schemas, suspensions, drop tombstones), incremented at the same
  publish-acknowledged point as the CDC one. Neither counter is imprecise any more, and
  neither's meaning moved.
- **pk lived inside the `sqlite` object — resolved, root-only (2026-08-24).** The key
  comes from `pg_index`, a fact about the table, not a dialect — yet it lived only in
  the `sqlite` block because the SQLite clients consumed it first; a PGlite-style
  client building from the `pg` block would have had to reach into another dialect's
  block for its key. `pk`/`pk_columns` now live **at the root, and only there**, next
  to the other dialect-independent facts. A duplicated-in-both interim was considered
  and rejected: two copies of one fact eventually differ, and with no installed base
  yet there is nothing a duplicate would protect. Both in-repo readers moved in the
  same change (web `applySchema`, Flutter `db_manager.dart`) — this IS a wire break,
  taken deliberately while breaking is still free.

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

### 1.10 CLOSED — `ColumnView` reached 8 bytes, by packing, not by dropping offsets

Superseded by `44a941d` (2026-08-21): `CDCEvent.ColumnView` is now a `packed struct`
of `{name_len: u8, name_offset: u24, value_tag (u8-backed), value_len: u24}` — exactly
64 bits, 8 bytes against the previous 12, a third off the dominant
`RING_BUFFER_COUNT × max_columns` memory term.

Both of this section's original claims aged badly, in instructive ways:

- **"A packed struct puts a shift+mask on every column access"** — only when fields
  straddle byte boundaries. These widths were *chosen* so every field starts at a bit
  offset that is a multiple of 8, and access compiles to a load plus at most one
  register shift / hardware bitfield-insert — verified by disassembly (the struct's own
  doc comment). Now enforced by a `comptime` block asserting the 64-bit size and the
  byte alignment of every field, so a reorder or re-widening fails the build instead of
  silently getting slower.
- **The store-lengths-only iterator design was never needed.** It bought the same 8
  bytes at the cost of making the array sequential-only — the packed struct keeps
  `columns[i]` random-access and needed no decode-path refactor.

The `u24`s are byte offsets into `data_buffer`, sized to its hard ceiling
(`absolute_max_event_data_buffer_log2` = 24 → 16 MiB) — not column counts. The
column count is `column_count: u16` with `MAX_COLUMNS` clamped to 8–1600
(PostgreSQL's own table-width cap) in `args.zig`. And the `u16→u24` widening of
`name_offset` is §2.10's fix riding along: `u16` was the silent wide-row corruption.

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

#### Delta generations — catch-up without touching PostgreSQL (sketch, 2026-08-24)

Context for this sketch: the client-side motivation is that replaying missed CDC on top
of a snapshot is very slow even inside one transaction, and the cadence-built
per-tenant snapshot (the "leaf" idea above) removes most of that replay. The question
was whether a client that already holds data can catch up incrementally instead of
re-fetching the full object.

**Not by chunk index.** Today's chunks are keyset-paginated row-count windows, so chunk
N's boundaries depend on every row before it — one insert anywhere shifts every later
boundary, and "which chunks am I missing" means nothing across generations. The
partial-last-chunk worry is a symptom of that instability, not a special case to solve.

**Not by on-demand delta either, and the reason is the storm.** A delta query
(`WHERE version > my_max_version`) is keyed by *client state*, not by (table, tenant).
The full-snapshot path survives a thousand simultaneous arrivals because they collapse —
`REQUESTS` is `max_msgs_per_subject=1`, one request enters the window, everyone reuses
the result (`stampede.py`). A thousand clients each carrying their own `max_version` ask
a thousand different questions: nothing dedupes, every arrival is a PostgreSQL scan.
On-demand delta is the one variant that re-couples PG load to client arrival rate —
exactly what the cadence design exists to break.

**The shape that works: key deltas by generation, not by client.** The cron job that
builds generation N also publishes "delta N−1 → N" in the same pass — one extra bounded
query per tenant/table per tick (`WHERE version > cutoff(N−1)`), cost independent of
client count — and keeps the last k deltas. A connecting client asks PostgreSQL nothing:
it reads the pointer (current full object + delta lineage), knows its own last
generation M, and fetches the N−M small delta objects from NATS, applying them in order
as plain upserts — last-write-wins is already the house rule (§1.9), and tombstones ride
in the delta so deletes apply too. Too far behind, or older than what tombstone
retention can vouch for → take the full object. Every artifact is shared and cacheable;
fan-out lands on NATS (and on per-tenant leaf nodes at the edge, now reachable over TLS
— see NATS_ZIG_NOTES.md §4), while PostgreSQL's total load is tenants × tables per
cadence, flat whether ten clients connect or ten thousand. Classic base-plus-
incrementals, and it composes with the pointer-swap design above: the pointer just
grows a chain.

The bucket-digest alternative (stable PK-range partitions + per-bucket digests,
PowerSync-style) solves the storm the same way — buckets are precomputed per generation,
clients self-select — so the storm argument does not choose between them; it only rules
out on-demand anything. Generation-deltas are less machinery: no partitioning scheme,
no digests, just cutoffs the version column already provides. Buckets are the
escalation if delta scans on very large tables ever hurt, or if clients must be
arbitrarily stale without ever taking a full object.

**Client/producer contract, settled in discussion 2026-08-24:**

- **Clients track watermarks, never generation numbers.** A client that also tails
  CDC is always ahead of the last generation it touched, so "which Gxx am I at" is
  the wrong coordinate. Its position stays what it already is: per-(tenant, table)
  max applied version plus the stream positions in `_zebridge_sync`/
  `_zebridge_stream_seq`. Each generation artifact carries its own coordinates —
  `{gen, prev_cutoff, cutoff, cutoff_lsn}` — and reconnection is a comparison: CDC
  resumable → tail as today; fell off retention → apply every delta whose
  `cutoff > my_watermark`, oldest first, then tail the CDC stream skipping events
  with `lsn ≤ cutoff_lsn` (every event carries `lsn`; no stream-seq coordinate is
  needed, and over-replay is free under the guarded upsert anyway).
- **Delta apply MUST be a version-guarded upsert** (`… DO UPDATE WHERE
  excluded.version >= current.version`), not the CDC path's arrival-order LWW: a
  client that tailed CDC past a delta's cutoff and then applies that delta would
  otherwise regress rows to older values. The guard makes overlap in either
  direction free — double-applied deltas are no-ops, CDC-fresher rows survive —
  and is precisely what buys "no generation bookkeeping needed."
- **Producer state lives in PG, not read back from NATS**: a restarted bridge must
  know the last cutoff, and "the bridge never reads its own output back" is a
  founding invariant — so `zebridge_generations (tenant, tbl, gen, cutoff_version,
  cutoff_lsn, built_at)`, append-only, pruned past chain depth k, doubling as the
  audit trail.
- **Cutoffs need a clock-skew margin**: versions are timestamps writers may supply
  themselves (within the clamp tolerance), so a row committed inside generation N's
  window can carry `version ≤ prev_cutoff` and be missed by every delta forever.
  Query `version > prev_cutoff − clamp_tolerance` — the slight overlap is free
  under the guarded upsert — and take the cutoff inside the generation's own
  `REPEATABLE READ` transaction so cutoff and content agree.
- **The table is BUILT and contract-tested (2026-08-24)** —
  `zebridge_generations (tenant, tbl, gen, cutoff_version, cutoff_lsn pg_lsn,
  built_at)` in `init.core.template.sql`, applied live, proven by
  `scripts/scenarios/generations.py` (7/7): invisible to clients (internal-listed,
  unpublished, no KV key), the reader runs the full LSN-before-snapshot recipe on
  its **single deliberate write grant** (INSERT+DELETE, no UPDATE — append-only by
  privilege; the content query must run as the reader for SELECT-everywhere +
  `zb.tenant` RLS, and the bookkeeping row must share its transaction), the PK
  forbids chain forks, and the writer holds nothing. Milestone 2 (the producer)
  is built — next bullet.
- **`cutoff_lsn` is the producer's own to stamp, no thread coordination**: any
  connection can `SELECT pg_current_wal_lsn()`, and the snapshot listener already
  does exactly this ("Snapshot started at LSN" / the descriptor's `lsn`). Ordering
  rule, overlap-never-gap: read the LSN **before** establishing the REPEATABLE READ
  snapshot, so a commit in the window is visible in the generation AND ≥ the
  recorded LSN (replayed twice, absorbed by the guard) — the reverse order makes it
  invisible AND below the LSN: skipped by both paths, lost.
  (`CREATE_REPLICATION_SLOT … USE_SNAPSHOT` pairs snapshot and consistent-point
  atomically if the window ever needs to be exactly zero.)
  **Amended by the first live tick (2026-08-24):** `cutoff_version` is captured
  INSIDE the snapshot transaction (`now()` = txn start, the instant the content is
  consistent with), but the row is INSERTed only after the object and pointer are
  live. The first run did it the other way and a NATS failure after COMMIT left a
  row pointing at an object that never existed — skip-if-unchanged then trusted it
  forever. With the row last, a crash in the window means no row: the next tick
  rebuilds the same gen number (same object name, overwritten; pointer re-put) —
  duplicate work, never a gap, never a dangling pointer.

- **Storage decided: Object Store, per-tenant buckets, KV pointer retained
  (2026-08-24).** Generations (fulls and deltas) are named immutable objects in
  `OBJ_<TENANT>` buckets — `ObjectInfo` supplies size/chunking/SHA-256 digest (client
  integrity checks for free), the library handles `max_payload` chunking in both
  directions, and `nats.zig` upstream already ships Object Store with streaming
  (its own e2e suite covers it). The pointer stays a tiny KV entry — Object Store
  has no pointer primitive — and becomes the chain manifest
  (`generations.<tenant>.<table>` → current gen + delta list + cutoffs), swapped
  last per the pointer-swap layout. ACL follows the crosstenant rule: a bucket IS a
  stream (`$O.<bucket>.C.>`/`.M.>`), so per-tenant buckets, granted per bucket —
  the grant-scoping shape is **verified** (`scripts/scenarios/objgrants.py`,
  2026-08-24): alice, granted only `OBJ_genacme`-named API subjects, reads her
  bucket byte-for-byte and is refused `genglobex` by the broker
  (`permissions violation for publish to "$JS.API.STREAM.INFO.OBJ_genglobex"`) —
  the bucket is the boundary, same as CDC_/INIT_. One client-library finding rode
  along: nats-py's `obj.get()` resolves subject→stream via `$JS.API.STREAM.NAMES`
  before subscribing, so a nats-py consumer needs that grant (names-only metadata
  leak) — a client that binds the stream explicitly (nats.zig can) needs no such
  grant, which is how the real consumers should do it. Strategically: the generation producer is NEW
  code on a cadence trigger, built BESIDE `snapshot_listener` — `INIT_*`/
  `$KV.snapshots`/the request window keep serving existing clients unchanged, and
  the old path is retired later, not refactored first. The two subsystems share no
  mutable state, so coexistence is safe by construction.

- **Milestone 2 BUILT and live-tested (2026-08-24): the producer loop** —
  `src/generation_producer.zig`, a `WalMonitor`-shaped thread beside
  `snapshot_listener` (no shared mutable state), enabled only by
  `GENERATION_RULES=users:_default;test_types:acme,globex` (same grammar as
  TENANT_RULES, but the columns ARE tenants — nothing derived from topology.json,
  the dyntenant lesson), paced by `GENERATION_CADENCE_SECONDS` (default 600, min 5)
  and `GENERATION_CHAIN_DEPTH` (default 6). Per tick per pair, fresh PG+NATS
  connections (at cadence the handshake is noise, and no tick inherits a half-dead
  socket): last gen from `zebridge_generations` → skip-if-unchanged (one `EXISTS`
  on the SYNC_RULES version column with the 5s clamp margin — idle cadences touch
  nothing) → LSN before REPEATABLE READ + `zb.tenant` → full content as msgpack
  `{columns, rows, gen}` (text-mode values) → object `<table>-g<N>` into
  `gen-<tenant>` (bucket auto-created) → KV `generations` pointer
  `<tenant>.<table>` = `{gen, object, bucket, cutoff_version, cutoff_lsn, rows}`
  swapped last → THEN the bookkeeping row (see the amended ordering above) → prune
  `gen ≤ N − depth`: PG rows first (the authority, via the DELETE grant), then the
  objects they named. Failures are logged per pair and retried next cadence, never
  fatal to the bridge. Proven end-to-end by `scripts/scenarios/genproducer.py`
  (6/6): g1 unprompted, object decodes with exact row count, pointer coherent and
  never dangling, idle ticks skip, a touched row yields g2 with the pointer
  advanced, chain pruned to depth with the g1 object gone. The margin means a write
  can echo into one duplicate build on the following tick — absorbed by the guarded
  upsert, by design. Deltas and the chain manifest are milestone 3 — next bullet.

- **Milestone 3 BUILT and live-tested (2026-08-24): deltas and the chain.** Every
  generation after the first ships a **delta** — `WHERE version > prev_cutoff −
  margin`, same predicate as skip-if-unchanged, so a built delta is never empty —
  and a **full** is built at g1 then refreshed whenever the last one would age out
  of the kept window (`gen − F ≥ depth − 1` keeps the jump-in point always inside
  `gen > N − depth`). Objects: `<tbl>-g<N>-delta` / `<tbl>-g<N>-full`, msgpack
  `{columns, rows, gen, kind, cutoff, prev_cutoff}` — self-describing, bounds
  carried in-band. The KV pointer is now the **chain manifest**, swapped last:
  `{gen, bucket, cutoff_version, cutoff_lsn, full: {gen, object, cutoff},
  deltas: [{gen, object, prev_cutoff, cutoff}…]}`. The client contract stays
  watermark-based, never gen numbers: apply, in order, every delta whose cutoff is
  past your watermark, provided the oldest reaches back to it; otherwise full +
  the deltas after it; a 404 mid-walk (pruned under you) means re-read the
  manifest and fall back to the full — never a gap. `zebridge_generations` gained
  `prev_cutoff` (stored, not derived: after pruning, the oldest kept delta's lower
  bound names a row that is gone) and `has_full`; template + idempotent ALTERs.
  Prune deletes PG rows first (the authority), then both object names per gen —
  the kind that never existed 404s quietly; a deleted object leaves an ADR-20
  tombstone (zero-size meta) that `list()` still shows, which is a *client-library
  listing detail*, not surviving data (the scenario's first FAIL was exactly that
  misread). The retention check found its home at producer wiring in `bridge.zig`:
  depth × cadence is computed and logged at boot, compared against
  `GC_THRESHOLD_MS` when that is visible in the bridge env, warned loudly when the
  sweeper window is smaller. Proven end-to-end by `scripts/scenarios/genproducer.py`
  (5/5): g1 full-only with the jump-in point set, idle skip, a touched row riding
  a 1-row delta with bounds matching the manifest, chain continuity
  (`d[i+1].prev_cutoff == d[i].cutoff`, first delta after the full chains off its
  cutoff, head reaches the manifest cutoff), prune to depth with the refreshed full
  inside the window and fetchable. Remaining: client apply (guarded upsert over
  deltas) + migration off `$KV.snapshots`, then retiring the snapshot path.

- **Names centralized, grants real (2026-08-24).** The KV bucket and object-bucket
  prefix moved out of `config.Generations` into topology.json
  (`"generations": {"kv": "generations", "bucket_prefix": "gen-"}`) — one file,
  three readers, parsed in `topology.zig` as an *optional* section (cdc_streams
  style: absent means the defaults, not an error) and injected into the producer at
  wiring. Only pacing (`GENERATION_CADENCE_SECONDS`, `GENERATION_CHAIN_DEPTH`)
  stays in config/env. The manifest KV is now pre-created centrally
  (`up.sh` + compose nats-init, `--history=1`, CLI-created so direct gets are on —
  this nats.zig `KVConfig` has no direct knob, so the producer's fallback
  `createBucket` makes a non-direct bucket: it exists only for a bucket nobody
  provisioned); per-tenant `OBJ_gen-<tenant>` buckets stay runtime-created by the
  producer, the dyntenant shape. And the ACL story left the spike: every principal
  in `nats-server.conf.template` (and the live native conf, SIGHUP'd) now carries
  `$KV.generations.>` subscribe + the objgrants-verified per-tenant grant block —
  `DIRECT.GET.KV_generations.$KV.generations.<tenant>.>` works precisely because
  manifest keys are `<tenant>.<table>`, tenant first. `$JS.API.STREAM.NAMES` is
  deliberately absent (bind the stream explicitly). Probed live as alice: reads her
  manifest and `OBJ_gen-acme`, refused globex's manifest and `OBJ_gen-globex` by
  the broker; `genproducer.py` re-passed 5/5 with every name read from
  topology.json.

- **The CLIENT side BUILT (2026-08-24): web-consumer applies the chain.**
  `App.tsx` seeds each table from its generation chain BEFORE the snapshot
  request path — `applyGenerations()` returns false for every "not this way"
  outcome (no `generations` in topology, no manifest, no pk yet, unknown columns,
  object pruned twice) and the snapshot path is the unchanged fallback, retiring
  only when every table has a chain. The walk is watermark-based, never
  gen-based: a `_zebridge_generations` table stores the last-applied cutoff per
  table; deltas with `cutoff > watermark` apply in order when the oldest reaches
  back (`prev_cutoff ≤ watermark`), else full + the deltas after it; a 404
  mid-walk re-reads the manifest once and restarts from ITS full. Rows land
  through a version-GUARDED upsert (`… DO UPDATE SET … WHERE excluded.v >
  local.v`) — unlike the CDC applier's plain upsert, because delta overlap is
  deliberate (margin rule) and the guard makes re-delivery free; the guard
  column arrives IN-BAND (`version_column` in manifest and payloads — added
  because the schema descriptor still lacks it, the noted client gap). The full's
  `DELETE FROM` shares the apply transaction. After apply, `state.lsn =
  cutoff_lsn` (hex `pg_lsn` → number) so the CDC gate skips pre-cutoff events.
  Object reads use `@nats-io/obj` (added via pnpm — npm's arborist crashes on
  this pnpm layout): `getBlob` rides `STREAM.MSG.GET` + a consumer, both already
  granted; no `STREAM.NAMES` needed, as intended. The main bridge now runs the
  producer (`.env.bridge`: `GENERATION_RULES="users:_default;test_types:acme,globex"`,
  30s demo cadence with the depth × cadence warning documented beside it) —
  first-tick fulls and manifests verified live. Remaining: retire the snapshot
  path once every table has a chain, and move `version_column` into the schema
  descriptor properly.

**Caught by the SQL console's own output (2026-08-24): the version guard compares
STRINGS, and the two wire formats disagreed.** CDC events carry timestamps as
`2026-08-24T20:25:23.217730Z` (ISO, UTC); generation artifacts carried PG text mode
in session-local time (`2026-08-24 21:26:55.379164+02`). `' '` sorts before `'T'`,
so in durable mode an offline client catching up on deltas would compare a chain
value against a CDC-written local one and wrongly lose — newer rows silently
skipped, the exact failure the guard exists to prevent. Fixed at both ends: the
producer runs `SET LOCAL timezone TO 'UTC'` inside the snapshot transaction (text
mode then renders `…+00`), and the client normalizes chain values to the CDC shape
with pure string surgery (`' '→'T'`, `+00→Z` — a `Date` round-trip would truncate
microseconds). A wire-format change resets chains: old `+02` artifacts were wiped,
gen numbering restarted. The lesson generalizes: the guard's comparability is part
of the wire contract — any value the guard touches must have ONE canonical text
form across every path that can write the column.

**Retirement carries two survivors (2026-08-25).** (1) `measureWidestRow` lives in
the snapshot listener but protects CDC: a row at or above the message budget
suspends the table (unreplicable, not just unsnapshottable). Retiring the snapshot
path must MOVE this check into the generation producer's build transaction (same
REPEATABLE READ, same cheap `octet_length`-on-TOAST trick, same suspension
publish), not delete it. (2) Size guards differ per path by design: objects have no
per-message budget — the object store chunks the payload into 128 KiB stream
messages, so wire overflow cannot happen — but generation builds are memory-bound
instead (libpq buffers the full SELECT, the arena holds the full payload ≈ 2× table
size on the bridge; getBlob + decode ≈ 2× on the client). The 100M-row remedy is
already half-shipped: nats.zig's `ObjectStore.put(meta, reader)` streams (putBytes
is its wrapper), so cursor-FETCH batches + incremental encode give a constant-memory
producer, and the JS lib's `get()` already returns a ReadableStream for the client
half. Gate it behind that table's `zebridge_generation_overrides` row when the day
comes.

**Case C CLOSED (2026-08-25): the width guard, both doors, one trigger.**
`zebridge_install_width_guard(tbl)` — installed by `zebridge_enable` as its own
step — generates a STATIC per-table BEFORE INSERT/UPDATE trigger over unbounded
columns only (text, unbounded varchar, bytea ×2 for the hex rendering,
json/jsonb/xml, arrays; a bounded-only table gets NO trigger — statically safe,
zero hot-path cost; live: users needed none, test_types guards 4 columns). Budget
in the `zebridge_limits` table (internal-listed; ONE ROW PER INSTANCE since 2026-08-26 — see §10b — default 16384 = 2^BASE_BUF
— one UPDATE when BASE_BUF changes, not a re-install). It RAISEs ERRCODE 23514
(check_violation): class 23 is already in the mutation listener's permanent set, so
fix 2 cost ZERO bridge code — an edge write becomes verdict `rejected`/23514, a
psql write an ordinary ERROR, both atomic in the writer's own transaction. Fix 1
(the measureWidestRow retirement survivor) landed as a zero-cost measurement in the
generation producer's encode loop: a legacy row at or over the buffer logs a loud
per-build warning (chains carry it; CDC will suspend on touch). Proven by
`scripts/scenarios/widthguard.py` (6/6) and end-to-end from the browser. Three
deployment lessons paid for on the way: (1) the generated trigger MUST be
SECURITY DEFINER with a pinned search_path — it reads `zebridge_limits` as whoever
writes the row, and bridge_writer had no grant: every guarded edge write died 42501
until then; (2) jsonb travels the mutation wire as a JSON STRING — nested maps are
refused (`UnsupportedPayloadType`), scalars only, by design; (3) a test minting
hardcoded FUTURE versions collides with the version clamp and loses to LWW as
`stale` before the guard is asked — clamp and LWW both behaved per spec while
refuting a broken test. Still queued from the original trace: nothing — the
producer detects, the trigger prevents, the verdict charges the sender.
Coverage closed same day: `legacybait.py` (7/7) exercises the three matrix cells
`widthguard.py` cannot reach — detector warning, decode quarantine + suspended KV
key, boot-preflight re-derivation — and proves the de-quarantine recipe
(repair → reboot → readmitted, key thawed). SECURITY.md §1.8 now cites a scenario
per cell; the untested cells were themselves found by demanding the citations.

**(Superseded trace, kept for the reasoning) Case C — the accepted-then-freeze path (2026-08-25, found by asking, not yet by
test).** A row oversized vs BASE_BUF but loaded by a BACKEND writer rides the
generation chain to every client (no per-row ceiling exists on that path — object
chunking removes it), where it looks editable. A consumer edit touching only small
columns then passes the ingress guard (which measures the PAYLOAD, and says so),
is APPLIED to PostgreSQL, answered `accepted` — and the CDC event for the full row
suspends the table for every reader. Worst ordering: refusal after commitment.
Mitigations: (B) usually saves us — pgoutput elides unchanged TOASTed values, so a
genuinely TOASTed blob edits cleanly around itself; the bad case is INLINE width
(many mid-size columns). Three fixes, by depth: (1) the widest-row check moving
into the producer (already queued) is really about refusing to SEED BAIT — the
snapshot path used to catch this before any client held the row; (2) NEW: a
post-apply width check in the writer's transaction — `octet_length` sum on the
resulting row, over budget → ROLLBACK → verdict `rejected` — makes edge writes
categorically unable to suspend a table (judge vs burst benchmark first);
(3) the `zebridge_enable` width trigger stops backend writers creating the bait
at all. Until (1)+(2) land, generations quietly widened the exposure the snapshot
path's measureWidestRow used to close.

**The oversize row, exercised end-to-end from the browser (2026-08-25).** A 20 KB
mutation against the 16 KB event buffer, driven through `libzb`'s `mutate()`:
MUTATIONS accepted it (PubAck), the listener refused it before PostgreSQL
("20274 bytes exceeds the 16384-byte CDC event buffer... not retrying"), one
dead-letter, verdict `rejected (RowTooLargeToReplicate)`, and the client reverted
the optimistic row — appear, verdict, vanish, outbox empty, PG untouched. Nothing
sneaks into the system; the refused row's brief local visibility is the designed
optimistic window, and the revert is the answer to "is display OK?" — a row that
will never echo must not linger. Getting there surfaced TWO client bugs the
server-side scenarios (rowsize.py covers the guard itself) could never see:
(1) INHERITED from App.tsx — `revertOptimisticWrite`'s "still ours" check compared
`String(stored)` to `String(sent)`, but SQLite stores booleans as 0/1 vs the
payload's true/false, so any table with a boolean column failed the match and the
revert DECLINED, leaving a ghost row until reload; fixed by normalizing the sent
side to storage shape (booleans → 0/1, objects → JSON). (2) Extraction-born — the
revert's change notification carried no event and crashed the app's verb handler;
evless notifications now skip per-event hooks. Lesson for the §10 conformance
fixtures: the verdict→revert path needs golden cases per column TYPE (boolean,
json, null), because coercion bugs hide exactly there.

**Row-too-large becomes a latency downgrade, not a freeze (2026-08-25).** Both
size doors exist and hold today: ingress refuses an oversized mutation with a
verdict before PostgreSQL sees it ("a bad write should cost its sender a verdict,
not cost every reader a table" — size measured from what NATS delivered, never
declared; threshold a deliberate lower bound so no legitimate write is refused),
and egress suspends a table whose row outgrew the event buffer by any other
writer's hand (frozen-and-valid, ex-@panic — the crash-loop reasoning is in
`suspendForRowTooLarge`'s comment). The retirement-era insight: the generation
producer reads POSTGRES, not CDC, and object-store chunking has no per-message
budget — so a row no NATS message can carry still rides a chain. After retirement,
`row_too_large` suspension should mean "CDC frozen, chains still current at
cadence latency": the client keeps chain-seeding suspended tables instead of
treating them as dead, and the 1 MB ceiling stops being a freeze at all. Optional
hardening for non-edge writers stays queued: a width guard trigger installed by
`zebridge_enable` only on tables whose unbounded columns can overflow, judged
against the burst benchmark first (perf: no drift).

**Consequence for retirement day — CDC retention shrinks to ~2 × cadence.** A
chain-seeded client needs CDC only from its manifest's `cutoff_lsn` forward, and the
manifest it read is at most one cadence stale (two, if it caught the pointer just
before a swap). Idle windows need nothing retained (skip = no events in between);
a client offline past retention just re-seeds from artifacts built once for
everyone — a gap stops being an emergency. So the CDC streams' 8d max-age can drop
to ~2 × cadence, and NATS then stores roughly ONE copy of the published data
(per-tenant fulls + small deltas) instead of days of chunks. Preconditions: every
table in the stream has a chain (the snapshot-path retirement itself), and note it
is the NATS window that shrinks — `GC_THRESHOLD_MS` (§7.5, offline writers) is a
PostgreSQL tombstone clock and keeps its own, longer horizon, coupled to the chain
only by depth × cadence.

**DERIVATION LANDED (2026-08-25): the publication IS the generation list.** The
producer's tick now derives its membership each pass — `pg_publication_tables`
minus `zebridge_is_internal_table()` (one predicate, every door), minus keyless
tables (catalog joins, NOT a `::regclass` cast: resolving `format(...)::regclass`
as the reader was refused `permission denied for schema pg_toast`; pg_class ⋈
pg_namespace ⋈ pg_index answers the same question with plain reads), minus
non-routable tables (`isCdcRoutable` ∪ TENANT_RULES — a table no client can follow
gets no chain), minus opt-outs. Tenants come from the DATA via `zebridge_tenants_of`
(SECURITY DEFINER — the reader's own RLS would scope the DISTINCT): dyntenant-
correct BY CONSTRUCTION, proven immediately when the live derive found leftover
tenant `dynten` and chained it with zero configuration, while row-less `globex`
correctly got nothing new. `zebridge_enable` cascades: a published table has a
chain within one cadence, opt-out via `generations => false` writing the ONLY
config that exists — `zebridge_generation_overrides`, an exception list (with
reserved per-table cadence/depth columns). `GENERATIONS_ENABLED=1` is the master
switch (.env.bridge); `GENERATION_RULES` demoted to a RESTRICTION intersected with
the derived set — probes and dev subsets, also enabling the producer by itself.
Side effects handled: `zebridge_user_tenants` added to the internal list (the
roster-leak door derivation would have opened — its schema/snapshot egress doors
remain queued), and `genproducer.py` now owns the only bridge (every enabled
bridge derives the full set and would race a probe's cadence). Verified live:
first derived tick, zero errors, manifests exactly {_default.{users, orders,
counter_public, zebridge_gc_watermark}, acme/globex/dynten.test_types}. ⚠️ Drift
note: the LIVE database's `zebridge_enable` predates the width-guard and
generations steps (template has both) — refresh it from the template before the
next migration relies on either step.

**THE LIVE-BIRTH EXERCISE (2026-08-25) — fresh volumes, a table born in front of a
consumer, both doors, offline and back.** Script: new volumes → system from
templates → consumer up FIRST → migrate `memo` (uid, txt, updated_at) via
`zebridge_enable(writable, version_col => 'updated_at')` → populate → consumer
oversizes (rejected) → consumer away, psql oversized (rejected) + one valid edit →
consumer returns. Everything held: the DDL pipeline carried the newborn's schema
live ("created (first sight)" with zero restarts — updated_at being the DEFAULT
version column meant no SYNC_RULES either), CDC delivered its rows to the watching
client, derivation chained it within one cadence, the consumer's 20 KB edit hit
the INGRESS door (one-column table: the payload itself is the width) → verdict
`rejected` → revert → replica == PG byte-for-byte, the psql edit hit the
TEMPLATE-BORN trigger (`zebridge_width_guard_memo`, first fire in anger, 23514),
the away-time valid edit rode a delta, and the return seeded from the chain
("Seeded memo from generation chain g1, watermark ...+00") with full equality.
Recorded as `scripts/scenarios/livebirth.py` (temp-topology probe + CDC_PUBLIC
subject grant, both restored; owns the only bridge) — 5/5 on its first complete
run, after its own two failures delivered findings (3b) and the parser lesson. FOUR findings, each now fixed or filed:
(1) **envsubst eats any `$word` in the templates** — my `$f$`/`$t$` dollar-quote
tags (and later a comment merely SPELLING the double-dollar delimiter!) broke two
fresh boots by unterminating quoting and silently swallowing every later statement
incl. `zebridge_enable`; law recorded in the template itself: only the plain
double-dollar delimiter survives, and it must never be spelled inside a body,
comments included. Scratch-DB apply with ON_ERROR_STOP is now the proven check.
(2) **The manifest timezone mix**: the producer's window query renders STORED
cutoffs outside the UTC transaction, so one manifest carried `+02` and `+00`
bounds — string-compared bounds, the payload lesson reborn one layer up. Fixed by
pinning the producer's whole SESSION to UTC; memo's chain reset; canonical-`+00`
assertion added to livebirth.py. genproducer.py missed it because its structural
checks compare within one source, never across ticks — coverage lesson noted.
(3) **`zebridge_user_tenants` still reaches clients on a VIRGIN world** — the
internal-list predicate guards the DDL trigger and the producer, but the boot
schema publish remains the unclosed egress door (empty roster today, so no data
leaked; the door itself is confirmed live). Still queued.
(3b) **A table born LIVE bypasses the routability refusal** — boot preflight
refuses a non-routable table before streaming; a table created while the bridge
runs skips that check, publishes into the void, and the bridge FATALs after
retries ("stopping bridge to prevent WAL overflow"). Correct last resort, wrong
first response: first-sight relations need the same no_cdc_subject refusal at
runtime. Found when livebirth.py's temp topology satisfied the bridge but not
CDC_PUBLIC's subject filter — which is also the lesson that T4 has TWO halves:
the topology file AND the stream's subjects.
(4) `zebridge_enable`'s T3 note says "SYNC_RULES + restart" even when the version
column is the global default and no restart is needed — conservative boilerplate;
make it conditional someday.

**THE DYNTENANT CAPSTONE (2026-08-25): a new user, a new tenant, a watching
browser.** nina (existing NATS principal, never used) was mapped to brand-new
tenant `tango` on the live fresh world: runtime streams CDC_TANGO/INIT_TANGO,
conf grants + SIGHUP, `note_t` (tenant-scoped, writable) migrated with the full
cascade (tenant guard fail-closed, RLS, replica identity moved to (tenant_id,
uid)), TENANT_RULES + one bridge restart (the honest T3 residue), mapping row →
`$KV.tenants.nina = tango` instantly. Results, all on film: nina's tab resolved
`tenant tango` live, her gap-check NAMED CDC_TANGO, `note_t` seeded from chain
g2 (full+delta, 4 rows applied over guarded overlap), the CDC_TANGO consumer up
in 6ms, a live psql insert moved her 2→3 — and alice (tenant-less) holds the
same schema with ZERO tango rows: isolation by stream, as designed. It forced
one real CLIENT fix: `cdcStreams`/`initStream`/`cdcStreamForTenant` gated tenant
streams on topology.json's tenant LIST, silently ignoring runtime-born tenants —
now they trust `$KV.tenants` as the runtime truth and special-case only the open
tenant (the list-gate's one legitimate job, avoiding the CDC__DEFAULT ghost).
Two findings + demo-state notes: (1) **JetStream storage reservations**: each
stream's max-bytes counts against the server cap, so runtime tenant provisioning
at up.sh sizes was refused ("insufficient storage resources") — INIT_TANGO landed
at 1G; dyntenant provisioning must budget modestly. (2) The per-principal conf
grant remains the static residue the signing-key endpoint retires — a new tenant
today = one grant block + SIGHUP. Demo state: tango's grants live only in the
RENDERED native conf (a --clean re-render loses them, deliberately — tango is
demo debris, unlike memo which persists via topology.json).

**THE STATIC-RESIDUE ENDGAME (2026-08-25, design filed).** After derivation and
dyntenant, three static surfaces remain, and each now has its named successor —
the winning pattern, six-for-six today: PG functions hold the rules, CDC carries
the news, the broker gets edited by the bridge, and the file keeps only what
never moves.

| static surface today | runtime source (successor) | mechanism |
| --- | --- | --- |
| ~~GENERATION_RULES~~ | the publication | derived per tick — DONE |
| ~~topology.tenants (runtime role)~~ | the data (`zebridge_tenants_of`) + `$KV.tenants` | runtime streams (dyntenant), client trusts KV — DONE |
| ~~`topology.public_tables`~~ | **the catalogue's public rows** (`tenant_col IS NULL`) — `zebridge_public_tables` itself was a projection and is DROPPED | bridge derives untenanted routability from the TABLE, and EDITS `CDC_PUBLIC`'s subject filter itself at runtime when a public table is born — the same move it already makes creating `CDC_<TENANT>`. One migration then does everything: publish, guard, chain, route, stream subject. **Mechanically closes finding (3b)**: a live-born table gains its subject instead of publishing into the void and FATALing. `topology.public_tables` demotes to up.sh's fresh-boot seed, like `tenants` |
| ~~`SYNC_RULES` / `TENANT_RULES` env~~ (was the last hand-worker reconciliation) | **`zebridge_table_rules`**, written by `zebridge_enable` in the SAME transaction as the guards it already installs with those very columns — the env var is a hand-copied duplicate of a fact the catalog holds | the bridge consumes it through the meta-cache + epoch-invalidation machinery it already has (invalidate.py's plumbing): the mutation listener's per-table meta read gains the rule columns, the sweeper and producer read the table they already query, decode-time tenant_col arrives at relation first-sight/DDL. Both T3 MANUAL rows vanish; "verified before migration" dissolves because rules+guards+publication commit atomically and nothing CAN disagree; env demotes to bootstrap/override with a boot-time disagreement warning during transition. Clients unaffected — the descriptor already carries version_column/tombstone_column |
| ~~NATS per-principal conf grants~~ (LANDED 2026-08-25) | **operator/JWT with a scoped signing key** | the permission shape is written ONCE on the signing key (`mutation.{{name()}}.>` etc.); every user JWT inherits it at mint time; a new principal or tenant is an auth-server event, not a conf edit + SIGHUP. The nina/tango grant block was the last manual rehearsal of what the signing key automates |

**The JWT cutover (2026-08-25, evening): the endgame table is EMPTY.** Operator
mode is live on the native stack. `scripts/native/jwt-bootstrap.sh` builds the
whole trust chain from nothing: operator → SYS + ZEBRIDGE (JetStream unlimited)
→ two signing keys — a `service` key (the bridge) and THE `client` scoped key,
which carries the entire per-principal grant block ONCE as a role template
(`mutation.{{name()}}.>`, `cdc.{{tag(tenant)}}.>`, the full `$JS.API` litany,
`$KV.tenants.{{name()}}`). Onboarding a principal is now ONE mint:
`nsc add user -K <client-sk> --tag tenant:<t>` plus the `zebridge_user_tenants`
row — no conf edit, no SIGHUP, ever. The server conf shrank to
`nats-server-jwt.conf`: operator + MEMORY resolver + websocket + jetstream;
every authorization block is gone. Because nsc lowercases tags and templates
substitute literally, tenant stream names carry the tenant AS-IS now
(`CDC_kilo`, never `CDC_KILO`) — reconciler, pruner, snapshot listener, client
and scenarios all changed together. Creds everywhere: the bridge takes
`NATS_CREDS` (nats.zig re-reads the file per reconnect — rotation without
restart), the browser fetches `/creds/<principal>.creds` (public/ symlink) and
authenticates via `credsAuthenticator` — falling back to user/password against
a pre-operator server; `zb.py` honors `NATS_CREDS` in both `connect()` and
`nats_cli`. The account switch orphans the old `$G` JetStream state
(`nats-data.pre-jwt`, kept) — the bridge rebuilt everything at boot: schemas,
tenants backfill, the CDC family, chains. Proof, live: `cdc.kilo.note_t.insert`
stored in `CDC_kilo`; omar publishes `mutation.omar.ping` but subscribing to
`cdc.acme.>` answers `Permissions Violation`; a browser tab with NO password
seeded as kilo and pushed an accepted counter write. (A denied JS API call, per
the harness's own warning, times out unanswered — the scoped key's deny is
silence on that path.) Residue, named: up.sh's NATS boot still speaks the old
world (conf render + nkey creation) — next edit; `nats-server.conf.template`
and check.py's conf-grant drift checks describe a dead mechanism and want
retirement; and the nsc store (`scripts/native/nsc-store`, gitignored: it holds
PRIVATE operator/account keys) is the new authority the mint endpoint will wrap.

**The mint endpoint, LANDED (2026-08-25, night): enrollment end to end.** The
"pump-starter" is real: `zebridge_invites(code PK, principal CHECK
[A-Za-z0-9_-]+, tenant_id, role, expires_at, used_at)` — the naming police as a
CHECK constraint, the code a one-time bearer secret — plus `GET
/enroll?code&user_pubkey` on the bridge's existing HTTP listener. The browser
generates its OWN nkey pair (`nkeys.createUser()` ships in nats-core), sends the
code + public key, and the bridge redeems-and-registers in ONE writer-connection
CTE (used_at is the single-use latch; the `user_tenants` INSERT is the same
event's second projection — the bridge's own CDC then carries it to
`$KV.tenants.<principal>` live, measured) and mints the user JWT in pure
in-memory Zig: `src/jwt_mint.zig`, ~100 lines — nkeys signing the bridge already
had, a local RFC-4648 base32 for the jti, claims matching nsc's byte-shape
(`pub:{} sub:{}` required-empty; the limits fields were dropped to match,
necessity untested). The seed never crosses the wire in either direction: the
client keeps its own, the response is `{jwt, principal}`, and the client
assembles the creds text itself (`credsFileText`). The principal comes back
INSIDE the JWT and libzb + the App header now treat the creds as authoritative
for identity — closing the config-says-bob-JWT-says-omar mismatch class.

Proof, live: an invited browser tab (`?invite=<code>`, no principal, no
password) enrolled as 'pia', tenant kilo resolved from the KV her own enrollment
populated, pushed an accepted counter write (`mutation_ack.pia.…`); the same
JWT's deny held (`Permissions Violation` on `cdc.acme.>`); a replayed code
answers 403. Two debugging lessons paid for it: (1) `grep -o 'U[A-Z0-9]{55}'`
extracts SUBSTRINGS — it sliced a "public key" out of the MIDDLE OF THE SEED
TEXT, minting JWTs whose sub could never match the nonce signer; two
Authorization Violations were blamed on the mint before line-anchored grep
exposed the harness (the Zig mint had been correct from take one). (2) The
enrolled page LOOKED wrong afterward — labeled alice — because the header showed
the URL-default principal while the JWT said pia; the mutation subject
(`mutation_ack.pia.…`) was the witness that couldn't lie. Residue: /enroll
speaks plain HTTP on loopback (the Caddy block terminates TLS in production);
the auth-callout responder — same lookup, same mint, riding `$SYS.REQ.USER.AUTH`
so even the route disappears — is the filed next rung.

**And the demo found a real client bug on its way out: the snapshot-replay LSN
gate.** Every fresh session logged "note_t … 3 rows applied" while the table sat
at 0 — for EVERY snapshot-replayed table, while chain-seeded ones were fine. The
chain of causes: schema descriptors are re-published at every bridge restart
with the CURRENT WAL LSN; `applySchema` initializes the table's `state.lsn` from
that; snapshot rows are applied through `applyEvent`, whose `ev.lsn < state.lsn`
gate then silently dropped every one of them (the descriptor's LSN predates the
restarts) — and the replay counter incremented regardless, so the log claimed
success. Chain seeding survived only because it writes outside `applyEvent`.
Fix: seeding IS the baseline — replay now applies in `seed` mode that bypasses
the gate, and the existing post-replay `state.lsn = desc.lsn` re-anchors the
watermark to the snapshot's own truth. Two general lessons: a counter
incremented beside a call that decides internally is a liar by construction
(count what LANDED); and a schema republish must never advance a DATA watermark.
The closing proof: an invited tab (`?invite=`, nothing else) enrolled as pia,
seeded all three kilo notes BY NAME plus every public table, and pushed an
accepted write — the full OAuth-shaped loop, minus only the real IdP.

**The connection budget (2026-08-26): /enroll must not starve anything.** Exposing
:9090 for more than metrics raised the right question: what bounds the damage? The
accept loop already had the shape (a `std.Io.Group` with GUARANTEED-thread
`concurrent` per connection — `async` may run inline, and a measured zero-byte
client once wedged the whole server that way — plus a receive watchdog and a
16-connection cap that refuses-and-closes: a queue nobody drains is just a slower
outage). What /enroll uniquely burns is not HTTP threads but PG WRITER
connections, so the bounds are layered where each resource lives:

| layer | bound | enforced by |
| --- | --- | --- |
| enroll concurrency | 4, then 503 | bridge (`enroll_in_flight` permit pool, refuse-fast) |
| a hung PG dial | 3s | libpq (`connect_timeout=3` on the enroll conninfo) |
| `bridge_writer` total | **8** | **PG**: `ALTER ROLE … CONNECTION LIMIT` — mutation listener 1 + sweeper 1 + enrolls ≤4 + margin |
| `bridge_reader` total | **10** | **PG** — producer, snapshot worker, boot bursts; the replication stream rides a walsender slot |
| everyone | 100 | PG `max_connections` |

"Match" here means CAP MINUS WHAT WE ALREADY NEED: the bridge may claim at most
18 of the default 100 BY CONSTRUCTION, leaving 82 for operators, psql and the
future app backends — the budget is the headroom accounting, not numeric
equality. The sweeper counts as exactly one because it is an EXTERNAL job
(cron/systemctl): its connection exists only for the seconds a sweep runs, but
the budget reserves its slot so a sweep never races the ceiling. Both role
limits live in the init templates (scratch-verified, live-applied,
`rolconnlimit` = 10/8 confirmed), so a bridge bug that leaked connections in a
loop would hit ITS role ceiling and log refusals on its own side while the
cluster keeps breathing — the rule is in PG, not prose. Smoke, live: 12 parallel
bogus enrolls answered `503 ×8, 403 ×4` while a concurrent `/metrics` scrape
returned 200 mid-burst. Measured steady state at rest: reader 1 backend (the
walsender), writer 1 (the mutation listener). Contract proven by
`scripts/scenarios/connbudget.py` — first run 5/5, including the bite test: PG
itself refused 3 of limit+2 sleeper connections with `too many connections for
role`, so a changed setting (a `-1` limit, a deleted permit pool, an oversized
budget) fails a scenario instead of surviving as stale prose. And the bridge
INTROSPECTS the budget at every boot (preflight's 🔌 line: limits, ceiling,
worst case, headroom) with three warnings: a limit raised to unlimited, a budget
past half the cluster, and the coherence case only the bridge can judge — a
writer limit lowered below its own consumers (listener 1 + sweeper 1 + enroll
permits 4), which would let an enrollment burst starve edge writes. The warning
was PROVEN to fire (limit dropped to 3 → boot warned → restored), and
connbudget.py carries the same coherence check — the scenario for operators, the
boot line for every start, PG for the enforcement: three layers, no prose.

**The exhaustion contract (the counter-measure's other half): what actually
happens at each ceiling.** Refusal everywhere, retry on the CALLER side, and no
internal queue anywhere — deliberately. A queue in front of a saturated resource
adds latency and memory without adding capacity, and the one flow that must
never drop already HAS its durable queue: edge writes live in the client's
outbox until verdict/echo (PROTOCOL §7.1) and in JetStream's MUTATIONS stream
until acked-after-apply — a second buffer inside the bridge would double-queue
the same bytes with worse semantics. Everything else is idempotent and cheap to
retry from where the "is it still worth it" knowledge lives: the caller.

| ceiling hit | the symptom | who retries | what is never lost |
| --- | --- | --- | --- |
| HTTP cap (16) | accept-then-CLOSE: the 17th connection gets no HTTP response at all (coarser than a 503, by design — writing a response would itself cost a slot) | the scraper's own schedule; the browser's fetch error | nothing was in flight |
| enroll permits (4) | explicit `503 busy — retry shortly` | the human re-clicks / the app backs off | the invite row is untouched — a refused attempt consumes nothing |
| writer role limit (8) via enroll | libpq `too many connections for role` inside the 3s dial → `503 backend unavailable` | same | same |
| writer role limit via the mutation listener's reconnect | reconnect refused → its own backoff loop | the bridge retries; meanwhile mutations WAIT in JetStream (unacked → redelivered) and in client outboxes | edge writes — this is the flow the whole design protects |
| writer limit via the sweeper | one cron run fails to connect | the next systemctl tick | tombstones reaped a cycle later; the watermark stays conservative (older = safe) |
| cluster max_connections (100), eaten by OTHERS | the bridge's long-lived conns (walsender, listener) HOLD their slots; only reconnects and per-tick conns fail | bridge backoff / next tick | WAL — the replication slot retains it, the same §2.19 guarantee as any reader outage |

One honest asymmetry, named: the role limits guarantee the bridge cannot starve
the cluster, NOT that the cluster cannot starve the bridge — a CONNECTION LIMIT
is a cap, not a reservation. The mitigation is that the bridge's load-bearing
connections are long-lived (they already hold their slots when the flood
arrives), and everything transient degrades into the retry shapes above.

**The memory audit (2026-08-26): three detectors, three worlds, all green.** The
daemon is long-running, so the leak that matters is the one that GROWS — and the
detectors compose because they see different allocators:

  1. **DebugAllocator exit audit** (Zig's world — mmap-backed, invisible to
     malloc tools): only speaks on paths that terminate, which makes early-exit
     paths its best witness. A bad-nkey boot found exactly one leak this way —
     `catalogue.Load`'s publics/tenants lists, allocated "for the life of the
     process" (a euphemism for never freed). Fixed with `Load.deinit` deferred in
     main (LIFO order puts it after every thread join that reads the aliased
     topology slices) plus frees on the loop's partial-failure branches. The
     repro now exits with ZERO reports — keeping failure exits audit-clean is
     what keeps this detector trustworthy.
  2. **macOS `leaks`** (the malloc zones — libpq's world, where a forgotten
     PQclear is the classic daemon drip): 0 leaks on the live process, every
     sample.
  3. **`leaksoak.py`** (drift over time, the recorded policy): two `leaks`+RSS
     samples around a churn window grinding the REAL verb matrix — three CDC
     verbs public AND tenant-scoped, refused enrolls every round plus successful
     mints every 10th (writer CTE + JWT signing + the user_tenants CDC
     diversion), snapshot-worker requests, mutation-listener round-trips (real
     msgpack envelopes, applied and verdicted — the counter's `last_writer`
     said `c-soak` afterwards), one sweeper pass, /metrics + /status renders.

Three 240s soaks, measured: naive churn +7 MB RSS, 0 leaks; full CDC matrix
**exactly flat** — 2205→2205 malloc nodes, 7360→7360 KB, +1 MB RSS over 113
rounds; complete matrix (mutations + sweeper) 2205→2209 nodes (the sweeper's
own libc bits), KB flat, +6 MB RSS, 0 leaks. The 1.0G "physical footprint" in
the leaks header is the RESERVED ring-slab mapping; resident truth is ~30 MB.
`SOAK_SECONDS` scales the scenario to an hours-long soak when a release wants
the stronger claim.

**The leak-detection boundary, made explicit and closed (2026-08-26).** The two
auto-detectors are TESTING-TIME, not production: `leaks` is invoked by the .py
scenarios (nothing runs it in prod automatically), and the DebugAllocator is a
BUILD-MODE thing (`IS_DEBUG = builtin.mode == .Debug`) — `zig build` defaults to
Debug (gpa + detectLeaks on clean exit), but production is
`-Doptimize=ReleaseFast` → `c_allocator`, NO DebugAllocator, NO exit-time check.
So all the Debug-build soaks proved the Debug build clean; a leak is a
free/PQclear LOGIC fact identical across build modes, so that transfers — but
"policy, never trust" says prove it, not argue it. Done: leaksoak against a
ReleaseFast binary → 0 leaks, +7 MB RSS over 180s. The proof is also MORE
complete than any Debug run, because in Release `c_allocator` routes every Zig
allocation through malloc — so `leaks` sees Zig AND libpq in one pass (visible in
the numbers: malloc footprint ~1.09 GB in Release, where the ring-slab is malloc,
vs ~7 MB in Debug where it is mmap and invisible to `leaks`). Operational
corollary: an operator can run `leaks <prod-pid>` against a live ReleaseFast
bridge for full-coverage leak detection with no rebuild — the one place the
test-time tool doubles as a production tool.

**Chaos & adversarial testing found SIX real robustness bugs (2026-08-26) — all
in the bridge's OWN logic, neither in NATS or libpq, exactly where the weak point
was predicted to be.** Memory was never the problem (0 leaks through every phase);
the two failures were both in error-handling DECISIONS:

  1. **WAL `StreamEnded` conflation → silent CDC death** (chaos.py, PG-loss).
     `pg_terminate_backend` on the walsender surfaces as `error.StreamEnded`,
     which the main loop treated as a GRACEFUL end and `break`ed the replication
     loop forever — process alive, /health green, mutation listener reconnected,
     CDC gone with `pg_reconnects` still 0. A `StreamEnded` is graceful ONLY when
     WE requested the shutdown (`should_stop`); otherwise the server severed it
     and it is reconnectable like any other loss. Fixed in bridge.zig.
  2. **`EndOfStream` retry storm → poison-pill DoS** (adversarial.py). A
     truncated/garbage msgpack payload raised `error.EndOfStream`, which fell
     through to the TRANSIENT-retry branch and was redelivered
     `mutation_max_deliver` (15) times with ack_wait between each — a handful of
     malformed messages stalled the single-consumer queue long enough to block
     legitimate writes behind them. An attacker with one lane could DoS a
     tenant's ingress with truncated bytes. Fixed: a decode failure is PERMANENT
     by nature (same bytes fail forever) → dead-letter on first delivery, except
     OutOfMemory which stays transient. Applied at the decode call site, so any
     error the msgpack lib ever adds is permanent by construction.
  3. **`for … else` sleep → ingress capped at ~10 writes/s** (race.py). The
     mutation pull loop's 100 ms pause was written as a `for … else` meant as
     "sleep on fetch timeout" — but Zig runs the else on every NORMAL completion,
     so the listener slept after EVERY message, throttling the whole ingress
     lane to ~10 msg/s regardless of backlog. Invisible to every single-write
     scenario (their cadence sits under the cap — which is also why their PASS
     verdicts were honest); 24 concurrent writers exposed it in one run: every
     key stalled at round 2 of 5 inside the check window. Fixed in
     mutation_listener.zig — sleep only when the fetch is empty
     (`Config.Nats.mutation_pull_idle_ms`).
  4. **Replication resumed from the WAL HEAD, not the slot → silent permanent
     data loss** (found while validating the Node consumer, 2026-08-26). Both
     `initReplicationStream` and the reconnect path called
     `wal_monitor.getCurrentLSN()` and passed it to `START_REPLICATION`. That
     tells PostgreSQL "start here", so **every change committed while the bridge
     was down, or while its walsender was severed, was skipped and never
     published**. The replication slot was doing its job — retaining the WAL —
     and the bridge stepped over it on every start. Nothing errored; replicas
     simply diverged from Postgres forever. Measured, reproducibly: stop the
     bridge → `DELETE FROM memo WHERE txt='fixtest'` → restart → the delete never
     reached CDC (`cdc.memo.delete` frozen at 193), while PG no longer had the
     row. Fixed by DELETING the parameter: `startStreaming()` now takes no
     position at all, and the `0/0` sentinel ("resume from this slot's
     confirmed_flush_lsn" — not "from the beginning") is written literally into
     the `START_REPLICATION` command in wal_stream.zig, where the protocol lives.
     Verified: the same delete now replays.

     The first attempt at this fix put `0/0` in `Config` and passed it at both
     sites. Wrong, and the review caught it: **`Config` is for tunables an
     operator may legitimately change; this is a protocol invariant with exactly
     one correct value.** Naming it in config invites the very edit that caused
     the bug. The parameter itself was the defect — it let two call sites express
     a wrong position, and both did — so removing it makes the mistake
     unrepresentable rather than merely discouraged. Same lesson as the timestamp
     and publication guards: a rule that lives only in prose is a rule that bites.
     (The boot path's `getCurrentLSN` call went too: once it no longer feeds
     `START_REPLICATION` it was a Postgres round-trip serving one log line, and a
     `current_lsn` in scope beside that call is precisely how it got passed for so
     long. Lag reporting belongs to wal_monitor, which measures it against the
     slot — the number that means something.)

     **Guarded by `downtime.py`** (new, 2026-08-26), which is the answer to "why
     did nothing catch this?": every existing scenario spins a FRESH probe slot,
     and for a new slot the WAL head and `confirmed_flush_lsn` are the same
     position — the two behaviours are literally identical, so the bug was
     invisible by construction. The scenarios that restart or sever a bridge
     (chaos.py) write and check AFTER the event, testing the *after* and never the
     *during*. And the browser demo takes a fresh OPFS database on every load, so
     accumulated drift was erased before anyone could see it. `downtime.py`
     deliberately does the one thing none of them do: reuse ONE slot across two
     bridge lifetimes and commit all three verbs into the gap between them.
     Verified both ways — RED on the pre-fix binary (INSERT, UPDATE and DELETE all
     lost: the window ate everything, not just deletes), GREEN on the fixed one.
     It also covers `kill -9`, because a crash and a clean stop are different paths:
     SIGKILL runs no handler and sends no final status update, so only what
     PostgreSQL already holds survives.

     **Where the resume position actually lives, since the naming invites
     confusion.** Three different quantities wear the word "LSN":
       - `pg_current_wal_lsn()` — the SERVER's WAL head. Only wal_monitor should
         ever read it (lag arithmetic). Using it as a start position was the bug.
       - `metrics.last_ack_lsn` — what the bridge has RECEIVED
         (`metrics.updateLsn(wal_msg.wal_end)`); observability only. Note it is
         RENAMED 2026-08-26 to `last_ack_lsn` in the Snapshot and on `/status`; it
         used to be `current_lsn`, which read exactly like the first one — the same
         conflation that made the bug natural to write.
       - the CLIENT's `lsn` — `_zebridge_sync.global_last_lsn` and per-table
         `state.lsn`, taken from `ev.lsn` inside each CDC payload, driving the seed
         gate. It never reads the bridge's metrics.
     None of these is the durable position. **That one belongs to PostgreSQL**: the
     bridge sends `sendStatusUpdate(batch_pub.getLastConfirmedLsn())` — strictly
     what JetStream has confirmed, never the WAL it merely received — and PG stores
     it as the slot's `confirmed_flush_lsn`. The bridge keeps nothing across a
     restart, which is exactly why `kill -9` is safe.

     The trade this accepts, deliberately: a restart may REPUBLISH events it
     already published (at-least-once instead of the previous at-most-once).
     That is safe by construction — msg ids are LSN-derived, so JetStream dedups
     inside its window and the client applier is idempotent (LWW upsert + LSN
     gate). A duplicate is recoverable; a missing delete is not. This is also the
     bug that made the earlier memo divergence (PG 4 rows, every replica 11)
     read like a client bug: the client was applying faithfully, and the DELETE
     events for those rows did not exist anywhere in CDC.

What the adversarial pass CLEARED (17 hostile mutation shapes): no crash, no
injection (column-name and value both neutralized — allowlist + `$N` params +
`appendIdent`), no hostile data stored, 0 leaks on every refusal path. The
decode-to-SQL surface is sound; both bugs were purely in retry/reconnect
classification — the "synchronous ⇒ correct" assumption's exact blind spot.

Three chaos/robustness scenarios now guard this: `chaos.py` (bad creds boot,
NATS jitter, PG loss, :9090 exhaustion, leaks), `adversarial.py` (hostile
msgpack + /enroll fuzz), `race.py` (concurrent writers, poison interleaved with
legit, leaks under contention). All were RED when they found the bugs — a test
that cannot fail proves nothing.

  5. **One large transaction wedges the bridge PERMANENTLY** (found 2026-08-26 while
     measuring the pgoutput ceiling — by accident, writing a burst as a single `DO`
     block instead of 2000 separate transactions). The WAL loop buffers a whole
     transaction's ring slots and releases them to the publisher only at `.commit`.
     When the tracker fills it logs, discards the in-flight transaction, and
     `return error.TransactionOverflow` — which propagates out of the loop and
     **kills the process**:

         Ring buffer capacity: 32768 slots, transaction row limit: 32767
         Transaction overflow: exceeds 32767 row limit — discarding entire in-flight transaction
         error: TransactionOverflow

     The transaction is still unacked in the slot, so the next boot replays it and
     dies again. Measured: two consecutive restarts, both dead. The bridge was
     **unstartable** until an operator ran `pg_replication_slot_advance` — which
     silently discards those changes, so every replica then diverges from Postgres
     permanently and invisibly. Same end state as finding 4, reached through
     ORDINARY APPLICATION BEHAVIOUR rather than a bridge restart: a bulk import, a
     migration backfill, a `COPY`, a `DELETE FROM big_table` — any single
     transaction over `RING_BUFFER_COUNT` rows.

     ⚠️ Lowering `RING_BUFFER_COUNT` lowers this cliff with it. The default moved
     65536 → 32768 the same day, halving the threshold to ~32.7k rows, which an
     ordinary migration reaches without trying.

     **Why nothing caught it**: every scenario writes in small transactions, and
     speed.py deliberately uses 2000 × 1000 — so the suite drives 2M rows through
     the bridge routinely and never once in one transaction. The guard's own
     comment shows the limit was understood as a deadlock backstop (cap the tracker
     one below the pool so `acquireAndFillSlot` cannot spin on an empty queue); what
     was not considered is what the process should DO when it fires.

     **The cap is a backstop for a SELF-INFLICTED deadlock.** Back-pressure already
     exists and is well built: `acquireAndFillSlot` spins for a free slot with a
     fatal-error check and a 30 s watchdog, so a producer outrunning the publisher
     simply blocks until slots come back. What defeats it is the all-or-nothing
     hold — during one transaction NOTHING reaches the publisher, so its queue is
     empty, no slot can ever return, and the wait would be infinite. The cap fires
     before that. It is not a sizing decision; it prevents a starvation the hold
     itself creates, by killing the process.

     **Fix — release incrementally, ack at commit.** The all-or-nothing
     hold is unnecessary: `proto_version '1'` has no streaming, so PostgreSQL sends
     a transaction's changes only AFTER it commits — everything the loop receives is
     already committed, and there is no abort to roll back. So when the tracker
     fills, release the buffered slots to the publisher, reset the counter, and keep
     going; ack the LSN only at `.commit`, exactly as now. Then:
       * the row limit disappears — a transaction of any size streams through,
         bounded by the ring buffer and its existing back-pressure;
       * the deadlock the cap defends against cannot occur, because releasing gives
         the publisher work, so slots keep returning and the EXISTING back-pressure
         wait behaves normally — nothing new is built; `max_tx_rows`, the overflow
         branch and `error.TransactionOverflow` are all DELETED;
       * a crash mid-transaction publishes a PARTIAL transaction, and the unacked
         LSN makes PostgreSQL replay the whole thing on restart. That is safe by the
         same property finding 4 relies on: msg ids are LSN-derived, so JetStream
         dedups inside its window and the client applier is idempotent (LWW upsert +
         LSN gate). A duplicate is recoverable; a crash loop is not.
     The cost is that consumers can transiently observe a partial transaction, where
     today they observe none of it. Set against a permanently unstartable bridge and
     operator-forced data loss, that is the right trade — and it is the same
     at-least-once bargain the whole pipeline already makes.

     Rejected alternative: quarantine the transaction (suspend its tables, ack past
     it) — that stops the crash loop but KEEPS the silent divergence, which is the
     worse half of the bug. PostgreSQL's `streaming` option (proto v2, PG14+) would
     also solve it and is the "proper" answer, but it means handling STREAM
     START/STOP/ABORT and a protocol bump for a case incremental release already
     covers.

  6. **Schema generation omits FOREIGN KEYS, so PROTOCOL §4 is unimplementable**
     (found 2026-08-26 while asking whether a split transaction could break FK
     ordering client-side). `pg_constraint` appears ZERO times in the whole Zig
     source: the schema builder assembles `pg`/`sqlite` columns, `pk`,
     `pk_columns`, `replica_identity`, the write contract and `lsn`, and never asks
     PostgreSQL about foreign keys. Measured on `$KV.schemas.orders` — no
     `foreign`, `references`, `fkey` or `constraint` key anywhere in the payload.

     Downstream everything then behaves correctly on wrong information: the client
     generates `CREATE TABLE orders ("uid" TEXT NOT NULL PRIMARY KEY, "user_id"
     INTEGER, ...)` — columns and a PK, nothing else — and `foreign_key_list(orders)`
     comes back empty. So `PRAGMA defer_foreign_keys = ON` in the CDC apply path
     guards a constraint that CANNOT EXIST, and §4's deferral rule has nothing to
     defer. `orders` is declared in the migration as the FK-ordering demo
     ("FK-ordering demo: child of users, PROTOCOL.md §4", a real
     `references(:users, on_delete: :delete_all)`), and the demo cannot fire.

     ⚠️ **The consequence for finding 5**: a transaction split across batches cannot
     break FK ordering on a client TODAY — but by accident, not by design. Measured:
     one transaction of 15,000 users + 15,000 orders interleaved, ring 4096 so it
     split across ~8 releases, 30,000/30,000 events published, bridge alive, zero
     drops. That test passes because there is no constraint to violate, so it does
     NOT establish that the ordering is correct. The moment a consumer mirrors FKs
     (a PGlite replica, a hand-written binding following §4) the question becomes
     live, and the metadata to answer it never arrives.

     **Fix**: query `pg_constraint WHERE contype='f'` in the schema builder, emit the
     referenced table and columns, and let the client add `REFERENCES` to its
     generated DDL. Sequencing matters — adding FKs client-side CREATES the
     cross-batch risk that today does not exist, so the metadata and the applier's
     FK hold/retry must land together, and the 30k split test must then be re-run
     WITH constraints present, where it becomes a real test rather than a vacuous
     pass.

Finding 4 is the one that argues loudest for the [[test-escalation]] topic: no
single-fault scenario could see it. Every scenario spins a FRESH probe slot, so
"resume from the head" and "resume from the slot" are identical for them — the
gap only exists for a slot that already has history and a bridge that was away.
It took a second consumer, seeded independently and compared against Postgres
row by row, to make the divergence visible at all.

`race.py`'s long "WIP" convergence failures resolved 2026-08-26 into finding 3
above plus three HARNESS lessons now encoded in the script: (a) `ZB_PSQL` must
point at the host PG (the docker-exec default silently returns empty); (b) a
swallowed publish exception scored as "24/24 keys wrong" — publish failures are
now counted and loud; (c) Nats-Msg-Ids must be namespaced per run — MUTATIONS
dedups inside a 2-minute window, so a rerun with static ids gets clean PubAcks
(`duplicate=true`) while storing NOTHING. After the fix, race.py passes all
three checks, and chaos.py + adversarial.py were RE-RUN against the fixed
binary the same day (policy: the listener's behavior changed, so the recorded
results are stale until re-proven) — both PASS, 0 leaks throughout.

**Refinement (same day): the three PG-side rows are ONE relation.** The disjoint
union of the two positive lists (tenant-scoped in TENANT_RULES, public in
topology + zebridge_public_tables) collapses because `zebridge_enable` already
receives the single distinguishing fact — `tenant_col`. Unify as
`zebridge_catalogue(tbl PK, tenant_col NULL, public_reason, version_col DEFAULT
'updated_at', tombstone_col, tiebreak_col, generations, CHECK ((tenant_col IS
NULL) <> (public_reason IS NULL)))`: NULL tenant_col = public, NOT NULL =
scoped — one nullable column IS the disjoint union. TENANT_RULES, SYNC_RULES,
zebridge_public_tables and zebridge_generation_overrides all become projections
of it; CDC_PUBLIC's subjects derive from the public half; the CHECK absorbs the
publication guard's core law into the schema (an unscoped, unjustified row
cannot exist — stronger than an event trigger refusing it after the fact);
`zebridge_enable` becomes an UPSERT plus the materialization of the row (guards,
RLS, publication) in one transaction. The bridge consumes it via the meta-cache
epoch it already has. The catalogue is the config; the config is a table.

**Built the same evening (2026-08-25): the catalogue row is DONE at boot level.**
`zebridge_catalogue` exists with exactly the refined shape; `zebridge_enable` now
UPSERTs it in the same transaction as the guards (the `generations`/overrides
step became the `catalogue` step, and the two T3 rows collapsed to one — restart
only for a NEW tenant-scoped table's routing). The bridge gained `catalogue.zig`:
one SELECT at boot merges rows into tenant_rules/sync_rules with env winning per
table, graceful on a pre-catalogue database. The producer's derive query LEFT
JOINs the catalogue for tenant_col/version_col and `COALESCE(generations, true)`
retired `zebridge_generation_overrides`. The decisive proof: `note_t` was
stripped from `TENANT_RULES`, the bridge restarted, and a live insert still
routed to `cdc.tango.note_t.insert` — boot line `🗂️ catalogue: 3 row(s),
3 table(s) gained rules`. Backfill surfaced a small finding on the way:
`zebridge_public_tables.tbl` was a regclass, and the dropped livebirth fixture
left a dangling OID that rendered as NULL — the backfill filtered on existence.
(The residue noted here at first — sweeper on env, boot-level only — closed the
same evening; see part 2 below.)

**Part 2, same evening: the residue swept, and the file renamed.** Everything the
env and the topology file still transcribed is gone. `SYNC_RULES`/`TENANT_RULES`
left `.env.bridge` (they still parse, as per-table emergency overrides); the
sweeper reads `zebridge_catalogue.tombstone_col` on its own writer connection —
written in the same transaction as the tombstone trigger it names, so they cannot
disagree. `topology.json` became **`grammar.json`** and lost its `tenants` and
`public_tables` keys: tenants are DATA (`SELECT DISTINCT tenant_id FROM
zebridge_user_tenants`), the public set is the catalogue's `tenant_col IS NULL`
rows, and the bridge now RECONCILES the streams itself at boot — creates any
missing `CDC_<T>`/`INIT_<T>` (1G caps: JetStream max_bytes is a storage
reservation, the INIT_TANGO lesson) and sets `CDC_PUBLIC`'s subject filter
authoritatively to the catalogue's publics plus the open tenant, preserving the
rest of the stored config (updateStream serializes everything — a partial config
would silently reset limits). `up.sh` and `nats-init` create only MUTATIONS,
REQUESTS and the KV buckets now. `zebridge_public_tables` is dropped everywhere:
the publication guard, the audit, `zebridge_enable`'s scoped check and the
gc-watermark registration all read the catalogue. Proof, live with an empty rule
env: `cdc.tango.note_t.insert` in CDC_TANGO, `cdc.memo.update` in CDC_PUBLIC,
boot line `🗂️ catalogue: 8 row(s) … 5 public, 1 tenant(s)` — one tenant because
the DATA says tango is the only mapped tenant, which is the point. Scenarios
moved with it: livebirth and decode_integrity pre-declare their fixture with one
catalogue INSERT and assert the bridge's own boot reconciliation bound the
subject (their temp-topology and stream-edit machinery deleted); check.py's
drift checks compare catalogue against CDC_PUBLIC's bound subjects. What remains
of finding (3b) is only the runtime half: a table born mid-flight still waits
for the next boot to gain its subject — the restart IS the T3 step, for public
tables too now. And the endgame table below is down to one static surface: the
NATS per-principal grants, waiting on the JWT signing key.

**The omar replay (same evening, second pass) — two real findings.** Replaying the
dyntenant browser exercise through the new `tiebreak_col` parameter, with the
counters dropped and reborn cleanly, surfaced two bugs the first pass had masked:

(1) **The positional-rule collapse.** `parseTableRules` SKIPPED empty fields, so
`counter_public:updated_at,,last_writer` — the documented "no tombstone, tiebreak
last_writer" shape, double comma and all — collapsed to `[updated_at,
last_writer]`, and every consumer reads position 1 as the TOMBSTONE. counter_public
had `last_writer` as its tombstone column and no tiebreak at all, silently, since
the counters were born; `.env.bridge`'s comment promised a semantics the parser
never had, and catalogue.zig inherited the collapse faithfully (a catalogue
tiebreak with a NULL tombstone slid into slot 1). Invisible because `last_writer`
stayed NULL — nothing ever *looked* soft-deleted — and tiebreak.py only exercises
the all-three-columns shape. Fixed everywhere position is the meaning: the parser
keeps empty fields as empty strings, catalogue.zig writes an empty placeholder in
slot 1 when only the tiebreak is set, and all five consumers (mutation listener,
event processor, preflight ×2, producer) treat "" as "not configured". Proof: a
fresh omar click now lands `value=1 last_writer=c-4c879720` in Postgres AND in the
replica via the echo — stamped from the envelope's client_id, never from data.

(2) **DROP + recreate leaves ghosts in every seeding source.** Dropping a table
clears Postgres and (live) the client's local table — but the generation chain
manifest, its objects, the snapshot descriptor, the INIT chunks and the CDC
retention all survive under the same table name. A FRESH client seeds from
whichever it hits first and resurrects pre-drop rows: measured, a replayed ghost
row whose update then earned `row_deleted` → revert (the write path handled it
perfectly — the ghost was purely client-side). livebirth.py always knew: its
cleanup deletes the manifest and bookkeeping by hand. The general rule a backend
must follow on DROP (or DROP+recreate): purge `$KV.generations.<t>.<tbl>`,
`$KV.snapshots.<t>.<tbl>`, the `init.snap.<t>.<tbl>.>` chunks and the
`cdc.…<tbl>…` retention, or bump past them. Filed and CLOSED the same evening:
the DDL pipeline now does all four itself. `pruneDroppedTable`
(event_processor.zig) runs on the same event that publishes the schema tombstone:
subject-filtered STREAM.PURGE on the CDC and INIT streams (five INIT shapes,
because only the data chunks carry the table right after the tenant — start,
error, meta and schema put a keyword first, and the first acceptance run left
exactly those behind), manifest-driven object deletes in `gen-<tenant>` (read the
manifest FIRST, purge it LAST — the reverse of the producer's swap order), then
KV-rollup purges of the generations and snapshots keys. Everything is advisory
(log-and-continue: the WAL path never stalls on broker housekeeping) and
boot-scoped like stream reconciliation: open tenant always, boot-known tenants
when the table was tenant-routed — a tenant born after boot keeps its artifacts
until its backend prunes, the dyntenant contract. Two deliberate limits: objects
whose manifest is already gone are invisible to the pruner (the manifest is the
only pointer; the producer's own gen-pruning covers retired objects in normal
operation), and purging CDC retention is intentional data loss for OFFLINE
clients — they reconnect to the tombstone and drop the table anyway. Client side:
`dropLocalTable` (libzb.ts) now also drains `_zebridge_outbox` rows for the
dropped table, LOUDLY — a queued write for a dead table could only ever earn
`row_deleted`, or worse, land on an unrelated table that later reuses the name.

**The road-sweep (2026-08-25, late): the fixture baseline was the last open wound.**
Asked "is anything left open?", the scenario re-sweep answered with three failures
that were all ONE finding wearing different masks: the fresh-volume reset had
wiped not just the fixture TABLES (`users`, `test_types`, `orders` — restored via
`zebridge_enable`, catalogue-era, tiebreak included) but the PRINCIPAL MAPPINGS
(`zebridge_user_tenants` — alice/bob/mary/zb_sweeper restored, propagated live to
`$KV.tenants` with no restart). legacybait failed as "bait not stored" (no
test_types), widthguard's edge case answered `row_deleted` for a row psql could
see (unmapped alice → the writer's RLS sees nothing — fail-closed working exactly
as designed, against the wrong baseline), and genproducer WEDGED for 52 minutes:
its `touch()` updates `min(id)` of an EMPTY `users`, and the depth loop retried
that forever. Three scenario hardenings came out of it: genproducer self-seeds
its probe row (with `updated_at` a minute in the past — a seed at `now()` sits
inside the 5s clamp margin and the idle check reads the margin-echo delta as
skip-if-unchanged broken, measured) and bounds the depth loop so a dead producer
FAILS loudly; tzguard self-provisions `schema_migrations` (probing a missing
table reported "exemption broken" when the guard was fine). One product fix
rode along: preflight's "named in SYNC_RULES ⇒ write intent" heuristic warned on
every outbound-only table once the catalogue filled the sync map for ALL tables —
intent is now a tombstone or tiebreak column, not mere presence. Migrations
followed the era too: the no-PK and exotic fixtures register in
`zebridge_catalogue` (their `zebridge_public_tables` INSERTs would fail any fresh
bootstrap), and 130000 passes `tiebreak_col` through `zebridge_enable`. End
state: envcheck, tzguard, check (zero disagreements), livebirth, decode_integrity,
legacybait, widthguard and genproducer ALL green under the catalogue era, one
slot, no orphans, no debris.

(Also relearned at the shell: zsh does not word-split `$N` — an alias-style
`N="nats -s …"; $N kv purge …` is a silent no-op with stderr swallowed, which
manufactured a false "already clean" mid-investigation. Literal commands only.)

What topology.json keeps: the genuinely static wire grammar — stream prefixes,
subject patterns, KV names, open tenant — which changes only in coordinated
redeploys and therefore never needs re-reading. Deliberately REJECTED: re-reading
topology.json on DB triggers ("more dynamic") — it inverts the coupling, makes a
file the runtime source refreshed by database events, and forces the DBA to touch
two places for one fact. The file shrinks toward being the contract; contracts
don't hot-reload.

Remaining correctness inventory: (1) overlap/regression — settled by the guarded
upsert; (2) deletions — tombstones ride deltas as rows, and the `sweeper retention ≥
k × cadence` check is wired at producer wiring in `bridge.zig` (2026-08-24: stated
at boot, compared against `GC_THRESHOLD_MS` when visible, loud warn when smaller); (3) clock skew — settled by the margin rule; (4)
pointer/object atomicity — settled by the pointer-swap layout (`objstore_race.py`).

⚠️ **The cadence becomes a correctness parameter, not a freshness knob.** Two pieces of
retention hang off it: the delta chain (k × cadence of catch-up depth) and tombstone
retention (the sweeper must not reap a tombstone before every client that could still
apply it as a delta has had the chance — sweeper retention > k × cadence, or a
hard-deleted row silently survives on a stale client). Same class of arithmetic
`descriptorStillFresh()` guards today for CDC gaps; the check needs a home, not a new
idea.

### 1.13 Async publish lanes — the plan (supersedes "pipeline the flush thread")

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

Not yet built; **the measurement it was gated on now exists** (2026-08-24).
`batch_publisher.timedPublish` times every publish→PubAck round trip: summed on
`/metrics` as `bridge_nats_publish_ack_seconds_total` next to
`bridge_nats_publishes_total` (mean = quotient, windows = scrape deltas), and
debug-logged per call. First idle-boot reading: 15 publishes, 19.5 ms total —
**~1.3 ms mean ack wait**, which is in "a few ms × frequent batches = real win"
territory, not sub-millisecond noise. Still to do before building: read the same
number under the README burst load (the ack wait under fsync pressure is the one
that matters), and judge any pipelining change against the burst benchmark by
iters delta, not by intuition about which side "must" be slow.

**Checked against the code (2026-08-24), because the design depends on it:**

- The flush thread drains the ring *indiscriminately* (up to 5000 events / 256 KB /
  500 ms), and only `doPublish` splits the run: KV/schema events publish individually
  inline (flushing pending CDC first, for order), CDC events group **by subject** —
  `cdc.[tenant.]<table>.<operation>` — in first-appearance order. So batching is per
  (table × verb), and yes: a mixed flush run *has* to publish once per distinct
  subject group, serialized, each paying its own PubAck wait. T tables × V verbs
  active in one flush = up to T×V sequential ack waits per cycle. A homogeneous burst
  collapses to one group per flush (one ack per ~256 KB); a mixed workload of many
  small groups is where the ack share compounds.
- "Drain the ring during the ack wait" is not where the win is: the replication
  thread packs into the ring regardless, and the flush thread re-drains the moment
  `flushBatch` returns. The serialized chain is encode(G1) → publish+ack(G1) →
  encode(G2) → … The overlap opportunity is **encode next group / next batch during
  the current ack wait** — and the win per cycle is bounded by min(ack, encode), so
  a 2–5 ms ack behind a 20 ms encode caps around 20 %, not 2×.
- The constraint that rules out the naive version: **publishes must stay strictly
  serialized on one connection.** Group order is first-appearance on purpose (a
  causal INSERT→DELETE across subjects, the §2.1 lesson), and a JetStream stream
  stores in arrival order — concurrent publishes interleave arbitrarily and break
  exactly what `publishCDCSubBatch` was built to guarantee. Slot lifetime is
  compatible (slots free only in `updateConfirmedLsn`, after the whole batch), and
  the retry path already leans on msg_id dedup, so encode-ahead is safe; concurrent
  fan-out is not.
- The emerging lane model (talked through 2026-08-24): batches stay homogeneous per
  subject — one `.batch` message cannot mix verbs, that was §2.1's bug — so a
  (tenant, table) lane's `INSERT, DELETE, INSERT` becomes three messages on their
  runs' subjects, written to the socket in CDC arrival order. Async only means nobody
  waits for the PubAck between them; there is still exactly ONE connection and one
  writer, and socket write order is what the stream stores. Different tenants are
  different streams — no shared order at all. Two tables of one tenant share a
  stream: interleaving their lanes is harmless because nothing guarantees cross-table
  order inside a stream even today (first-appearance grouping already reorders it);
  the invariant that must hold is per-lane internal order, which one ordered writer
  gives for free. The only cross-table casualty is same-transaction multi-table
  causality (FK parent/child), which today's grouping breaks identically — the
  replica has no FK constraints and converges by PK upsert, so this stays a
  documented non-guarantee rather than a regression. Concretely: with
  `order_items.order_id → orders.id`, a flush run holding
  `items(1000 → existing order)`, then `orders(42)`, `items(1001 → 42)` groups as
  lane `order_items` first (first appearance, carrying BOTH item events) and lane
  `orders` second — so item 1001 reaches the stream before its parent order 42.
  SQLite clients shrug (no FK declared, PK upserts, final state converges; the child
  is "orphaned" only mid-replay). The planned Python + local-Postgres consumer is
  where this bites: declared FKs would reject the child. That client must either not
  declare FKs on the replica (the source already enforces them), declare them
  `DEFERRABLE INITIALLY DEFERRED` and apply each batch in one transaction, or apply
  in LSN order across streams (every event carries `lsn`). **Settled 2026-08-24 as a
  consumer rule, now in PROTOCOL.md §4 ("Ordering — what is guaranteed, and the
  foreign-key rule")**: both engines support deferral (PostgreSQL `DEFERRABLE
  INITIALLY DEFERRED`; SQLite the same in DDL, or `PRAGMA defer_foreign_keys` per
  transaction), and the lsn field already carries commit order — no wire change.
**BUILT and verified (2026-08-24), as planned below.** `PublishWindow` in
`nats_publisher.zig` (per-window reply inbox, in-flight list, `drain` fails the whole
window into `flushBatch`'s retry; per-ack latency feeds the same metric); lane-aware
grouping in `publishCDCSubBatch` (per-lane verb-runs, cross-lane coalescing, window
only when every group is a distinct lane). Live mixed-matrix run — `users` (public)
plus `test_types` under `acme` and `globex`, all three verbs, three transactions —
hit all three paths by design: "3 groups with a lane verb-alternation — publishing
sequentially" (the §1.18 case, client ends CORRECT), "3 distinct lanes — async PubAck
window" (one ack wait for three streams' worth of messages), and the sequential
fallback for a mixed-verb lane. Stream counts, PG truth, and the alice client all
agree; zero errors. What remains of the original idea is only the measurement-driven
question of whether MORE overlap (encode-ahead across flush batches) ever pays —
revisit after a burst-load reading of the ack counters.

**Measured under burst (2026-08-24, native PG+NATS on the host, ReleaseFast,
OrbStack stopped):**

- **README burst (2M rows, homogeneous)**: iters ≈ 1.97M for 2M events — the healthy
  ~1 iter/WAL message, no hot-path drift. 3,283 publishes (~609 events each), Σ ack
  2.09 s of a 23.7 s drain → mean ack **0.64 ms**, ack share **~9%** while the
  replication thread ran 85% busy in decode+pack. Verdict on the residual idea:
  encode-ahead pipelining would buy ≤9% under burst — **not worth building**; the
  window's real value is mixed-lane latency, already taken.
- **A/B against the pre-async binary** (async-lane files stashed, rebuilt, rerun):
  56.6k vs 54.0k ev/s, CPU 107% vs 106%, proc_ms 12742 vs 12716, iters identical —
  the async-lane change costs nothing measurable.
- **Mixed window burst** (200 txns × users+acme+globex inserts, 100k rows): all 200
  flush runs took the async window (200× logged), 601 publishes for 3 lanes × 200
  runs, mean ack 0.36 ms — one window drain per run instead of three serialized ack
  waits. Streams: +200 CDC_ACME, +200 CDC_GLOBEX, users on CDC_PUBLIC.
- **Alternation burst** (100 txns of INSERT/DELETE/INSERT on users, ~40k events):
  all 100 runs took the sequential fallback (100× logged), 301 publishes ≈ 3 groups
  per txn, mean ack 0.23 ms.
- **The ~54k ev/s / ~20 µs-per-event figures above were WRONG, and the story of
  finding out is the lesson.** They looked like a 4x regression against the
  README-era ~189k; hours of bisection exonerated, one at a time: OrbStack, host
  starvation, orphan processes, the async-lane diff (A/B), BASE_BUF, the slot, the
  SQL text, metric polling, spawner (bash vs python, parent alive vs dead), QoS
  clamping, E-cores, pipes. The tell that finally cracked it — after the user asked
  whether the test was even measuring the same thing — was a mid-drain `sample` of a
  "107 % CPU" bridge showing it mostly *blocked*, with the call graph pointing at
  `acquireAndFillSlot → Io.Writer → writev`: the hot path was **writing per-event
  debug log lines**. The slow runs' stderr files were **632 MB / 10 million debug
  lines** (5 per event); the fast runs' 12 KB. `LOG_LEVEL=debug` had leaked in from
  the sourced dev env (`.env.bridge` carries it), and `speed.py`'s
  `setdefault("LOG_LEVEL", "info")` *respects* an inherited value — every harness
  wrapper that happened to `export LOG_LEVEL=info` measured fast, every one that
  didn't measured the logger. Fixed: `speed.py` now **forces** info
  (`SPEED_LOG_LEVEL` to override deliberately).

  **Final numbers came only after cleaning TWO more contamination layers** (both
  spotted by the user, not the harness): the native `postgres-data` predated the
  demo-migration redirect, so `users` carried the composite `(name,email)` TEXT
  primary key instead of `id bigserial` — every benchmark insert maintained a text
  btree, capping PostgreSQL at ~190k rows/s regardless of txn shape; and **twelve
  orphaned metric-poll loops from a PREVIOUS session (started Friday, ports
  9095/9099, ~29 CPU-hours consumed)** were still running — invisible to sandboxed
  `pgrep`, they matched "bridge" in Activity Monitor because their command lines
  poll `bridge_*` metrics. Lesson on top of the LOG_LEVEL one: sandboxed kills
  don't reach other invocations' processes — sweep with the sandbox disabled, and
  `down.sh --clean` + fresh migrations beat forensically trusting an old data dir.

  **Corrected native numbers (M2 Pro, ReleaseFast, fresh tuned PG 18.6
  (shared_buffers=2GB, max_wal_size=16GB) + fresh NATS, one bridge, warm-up then
  test)**:

  | run | end-to-end | PG write | drain tail | bridge CPU |
  | --- | --- | --- | --- | --- |
  | canonical 2M INSERT | **236k ev/s** | 2.4 s (~830k rows/s) | 6.1 s | 6.96 s (**3.5 µs/event**, 82 % of one core) |
  | mixed 2M (50 % INS / 30 % UPD / 20 % DEL, id-keyed) | **245k ev/s** | 2.4 s | 5.7 s | 7.03 s (3.5 µs/event) |

  Above the README's 200k+ record, and **mixed verbs cost the same per event as
  inserts** (an earlier 27k mixed figure was the stale composite-PK table: its
  UPDATEs rewrote primary keys and its range-DELETEs seq-scanned). No regression
  exists anywhere — the async-lane work is cost-free (A/B), and the drain-side
  ceiling is now ~330–350k ev/s (2M / ~6 s tail) with PostgreSQL no longer the
  bottleneck at ~830k rows/s ingest.

  Two durable lessons: **a benchmark run at debug measures the logger, not the
  bridge** — check the log file's *size* first when a number looks wrong; and
  `iters` alone cannot catch this class of slowdown (it counts loop trips, not the
  cost inside each) — pair it with CPU-per-event from `bridge_cpu_seconds_total`.

**Where the ceiling actually lives (instrumented 2026-08-24):** a per-second timeline
of the clean 2M burst shows the ring at 0–1 % throughout, Σ PubAck wait 0.61 s of a
6.2 s drain (0.20 ms mean), and the WAL loop *idle in 34 % of its iterations* with
`recv_ms` (waiting on libpq) at 2.5 s against `proc_ms` 1.0 s — the bridge is starved
by **PostgreSQL's walsender/pgoutput**, which delivers ~320k ev/s on this machine.
NATS ingestion is the least-loaded stage by an order of magnitude. WAL retention under
a faster-than-pipeline burst behaves exactly as designed: peaks (251 MB after a 2.4 s
830k rows/s write) and bleeds to 0 within ~6 s; at any sustained rate under ~300k ev/s
it never grows. Output format is already pgoutput binary — no client-side decode lever
remains. The one untried lever if this ceiling ever matters: **partition tables across
several publications/slots** so N walsenders run in parallel — each still decodes the
full WAL but only pays output-callback and send costs for its own tables, so the gain
depends on where pgoutput's time actually goes; measure before believing. The deeper
answer is architectural and already sketched: delta-generation snapshots (§1.12) make
cold-client catch-up independent of walsender throughput entirely — live tailing at
~300k ev/s per slot is then the only walsender-bound path.

**The multi-slot partition lever, tried (2026-08-24):** two dedicated publications
(`pub_users`, `pub_tt` — created ad hoc in SQL; the real flow is the migrations'
`zebridge_enable()`), two bridges at `RING_BUFFER_COUNT=8192` (**19 MB ring each**,
against 156 MB at 65536 — the footprint drops as expected since the ring measured
0–1 % under burst). Result: with 1M narrow `users` rows + 1M wide `test_types` rows
written concurrently, **both configurations are producer-bound and equivalent** —
single bridge: 10.4 s / 191k ev/s / 8.8 s CPU; two bridges: ~13 s (3 s poll
granularity) with each bridge tracking its writer essentially live (4.3 µs/ev narrow,
4.8 µs/ev wide). The single walsender never reached its ~320k ceiling at this load, so
partitioning had nothing to win yet; the regime where it pays is a sustained
multi-table aggregate above ~320k ev/s, which these fixtures cannot produce (only
`users` is narrow, and one table cannot span publications). Two fixture traps cost a
run each, worth remembering: `test_types` DELETEs are tombstones (guards), so
"cleanup by DELETE" leaves every row in place and re-used deterministic uids then
collide (unique-violation storms that read as a dead walsender); and preflight demands
`zebridge_ddl_events` in EVERY publication a bridge attaches to.

**Segregating tables per publication — the DBA recipe and the one engineering gap
(2026-08-24):** the PostgreSQL side is ready today. `zebridge_enable()` already takes
`publication => 'pub_x'` (its default is just `${BRIDGE_CDC_PUBLICATION}` rendered at
init time), it is idempotent (`'already'` when the table is in the publication,
`ON CONFLICT DO NOTHING` on the registry), and any number of migrations may each call
it — that is exactly how the existing migrations work, one call per table. So a DBA can
freely pair publications with slots and run one bridge per pair (`--pub pub_x --slot
slot_x`), rightsizing each ring (`RING_BUFFER_COUNT=8192` → 19 MB was ample under
burst). Per-publication checklist, learned the hard way: `zebridge_ddl_events` must be
added to EVERY publication a bridge attaches to (preflight refuses otherwise — it is
guard-exempt, add it directly), and `zebridge_gc_watermark` belongs in any publication
whose clients flush outboxes. The gap that makes N>1 bridges NOT production-ready yet:
the NATS durables are fixed names shared by every bridge — `bridge_mutations_worker`
on MUTATIONS and the snapshot listener's fixed durable on REQUESTS — so two bridges
work-steal from both queues. For mutations that is accidentally fine (the apply path
is plain SQL, publication-independent); for snapshot requests it is not: the bridge
that wins a request for a table outside its publication cannot serve it, and the
request is consumed. Per-bridge durable names derived from the slot (plus a
publication-aware request filter, or table-partitioned request subjects) is the
missing piece.

**The plan (2026-08-24), settled after the lane discussion — executed above:**

1. **Verb-run grouping.** `publishCDCSubBatch` stops coalescing a subject's events
   across the whole run and instead cuts a group at every subject *change* —
   `INSERT(1), DELETE(2), INSERT(3)` on one table becomes three messages in arrival
   order, not `insert[1,3]` + `delete[2]`. This alone closes §1.18 by construction.
2. **One connection, one writer, an in-flight PubAck window.** The flush thread writes
   each run's message with a per-publish reply inbox (`_INBOX.<prefix>.<n>`) and a
   `Nats-Msg-Id`, without waiting; a wildcard sync subscription on the prefix collects
   PubAcks; after the batch's last run is written, the thread drains the window and
   fails the flush if any ack is missing or is an error — which lands in `flushBatch`'s
   existing retry-with-dedup path.
3. **Sequential within a lane, free across lanes.** Consecutive runs of the SAME lane
   (subject minus the operation token — the table axis) are the only order-coupled
   pairs, and a mid-window failure must never let run N+1 be stored while run N is
   re-published later. v1 keeps this trivially: if a flush batch contains two runs of
   one lane (rare — it needs verb alternation inside one ≤500 ms window), that batch
   publishes fully synchronously, today's path; batches whose runs are all distinct
   lanes — the common case — go through the window. No failure mode reorders a lane.
4. **KV/schema events are an order barrier.** They stay synchronous, and drain the
   window first — a schema must not overtake the CDC events that precede it.
5. **The metric stays honest.** Each in-flight entry records its send time;
   `recordPublishAck` fires per ack, so `bridge_nats_publish_ack_seconds_total` keeps
   meaning wall time per publish, now overlapped instead of serialized.

- The endgame alternative: an **async publish with an in-flight PubAck window**
  (nats.go's `PublishAsync` shape). Writes on one connection stay ordered, so the
  stream's arrival order is preserved while nobody waits per message — that removes
  the ack wait entirely instead of hiding one encode behind it. nats.zig's
  `js.publish` is synchronous request/reply today, so this is an upstream feature
  (or a bridge-side manual reply-inbox scheme) — worth weighing against the
  thread-pipelining before building either.

### 1.14 CLOSED — `web-consumer`'s schema watch no longer uses `kv.watch()`; it is a pull consumer

The leak's mechanism, confirmed in `@nats-io/jetstream` 3.4.0 and reproduced live
(`scratchpad/watchprobe.mjs`, a Node script that hands `getPushConsumer` exactly what
`kv.watch()` does):

- `kv.js`'s `_buildCC` returns a config carrying `filter_subject` and `deliver_policy`.
  `getPushConsumer(stream, cc)` (`jsmstream_api.js:62`) is overloaded on its second
  argument: a string binds an existing consumer, an object that satisfies
  `isOrderedPushConsumerOptions` (`types.js:22` — true for *any* object with
  `filter_subject`/`deliver_policy`/`headers_only`…) goes to `getOrderedPushConsumer`.
  So every KV watch is an **ordered** consumer. The `KV_WATCHER_<nuid>` name `kv.js` sets
  is overwritten with `oc_<nuid>_1` (`jsmstream_api.js:115`), and the 5s
  `idle_heartbeat` `kv.js` asks for is overwritten with 30s (`:104`). The probe confirms
  it: `ordered=true`, name `oc_…_1`, `hb=30s`.
- An ordered consumer heals by replacement: `reset()` (`pushconsumer.js:64`) deletes
  the current consumer and creates `<prefix>_<n+1>`. It fires on two missed heartbeats
  (`IdleHeartbeatMonitor`, `maxOut: 2`, checked every 30s) or on a delivery-sequence gap.
  The delete is denied here — `$JS.API.CONSUMER.DELETE` is withheld from clients on
  purpose, and that stays — so each reset leaves the old consumer to its 5-minute
  inactivity threshold. `reset()` also never clears the monitor's `missed` counter, so
  once heartbeats stop arriving **every** 30s tick resets again: the cadence observed.
- Flow control is on (`flow_control: true`) but alice has no `$JS.FC.>` publish grant,
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

### 1.16 CLOSED — `decode_integrity.py` caught up with the tenant split, and with §2.19

The stale-`INIT`-stream bug, fixed as prescribed (same pattern as `snapshot.py`): the
snapshot path now resolves the real stream (`INIT_PUBLIC` — `decode_fixture` is a public
table, so its tenant is the open tenant), keys the descriptor as `<tenant>.<table>`,
requests on `snapshot.request.<tenant>.<table>`, and filters chunks on
`init.snap.<tenant>.<table>.<id>.>`.

Fixing that exposed a second staleness the Publish Violation had been masking: the CDC
half **also** failed, because since §2.19 the bridge refuses any table that is neither
tenant-scoped nor in **topology.json's** `public_tables` — a check younger than this
scenario. The script registered its fixture in two places (Postgres:
`zebridge_public_tables` + publication; NATS: `CDC_PUBLIC`'s subjects) but not the
third, so its own bridge dropped every event at the source
(`REFUSING 'decode_fixture': not ... in topology.json's public_tables`). Fixed
scenario-side, since the bridge is behaving exactly as §2.19 intends: the script hands
*its own* bridge a temp topology (the repo's file plus the fixture) via
`TOPOLOGY_PATH`, never touching the deployed one. Onboarding a public table takes three
registrations, not two — worth remembering next time a fixture table "publishes fine"
into nothing.

Verified: both acts pass, 400/400 rows byte-for-byte on the CDC path and the snapshot
path (label, jsonb, numeric, enum).

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

### 1.18 CLOSED — lane-aware grouping; same-key verb alternation publishes in order

Found while checking §1.13's grouping claims (2026-08-24), by reading, not yet
reproduced. `publishCDCSubBatch` groups a flush run by subject and emits groups in
**first-appearance** order. Its own comment proves the two-event case: INSERT then
DELETE of one row lands as insert-group before delete-group — correct. But coalescing
puts *every* event of a subject into that subject's one group, so a three-event
alternation on the same key inside one run (≤500 ms / 5000 events) breaks:

- `INSERT(1), DELETE(2), INSERT(3)` → groups `insert[1,3]`, then `delete[2]` — the
  client applies the re-insert *before* the delete and ends with the row gone while
  PostgreSQL has it.
- `DELETE(1), INSERT(2), DELETE(3)` → the mirror image: row survives client-side,
  gone upstream.

The client cannot rescue it: the CDC DELETE path is an unconditional delete-by-PK
(`App.tsx`, `op === 'DELETE'`) — under REPLICA IDENTITY DEFAULT a delete carries no
version to guard on. Reachability is narrow but legal: hard-delete tables with key
reuse inside the flush window (REPLACE-style delete+reinsert idioms twice in quick
succession; client-minted uuid keys rarely recur, `bigserial` never). Soft-delete
tables are immune — their deletes are UPDATEs, one subject.

Fix directions, not yet chosen, and in tension with §1.13: fully order-safe grouping
means breaking a group at every subject *change* (coalesce only consecutive runs),
which turns verb alternation into one publish per flip — exactly the context-switch
cost coalescing exists to avoid — and is the strongest argument for the async
publish-window (§1.13): with no per-publish ack wait, run-level grouping becomes
affordable and the ordering question disappears rather than being patched. A cheaper
interim: within-run last-write-wins per key before grouping (drop all but the final
event per (subject, key) in the run) — correct final state, loses intermediate events
consumers currently see.

**Resolved by §1.13's execution (2026-08-24).** `publishCDCSubBatch` now groups
per-lane verb-runs: a lane (subject minus the operation token) splits its group at
each of its own verb changes, while different lanes still coalesce freely — so
interleaved multi-table traffic keeps today's message counts, and the alternation
above becomes three messages in arrival order. A run containing any lane alternation
publishes fully synchronously (order under retry); runs of distinct lanes go through
the async PubAck window. Verified live with the exact scenario: one transaction
`INSERT(id=987001), DELETE, INSERT` on `users` — the bridge logged "3 groups with a
lane verb-alternation — publishing sequentially" and the connected client finished
with the row PRESENT, where the old coalescing left it deleted.

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

### 2.20 Boot schemas could not change shape across a quick restart, and composite keys never survived boot at all

Found while verifying the root-only `pk` move (§1.7): the new bridge booted, logged
"Boot schema published to KV" for every table — and the KV kept the *previous* boot's
payload. Not a race, not a stale binary (checked: process younger than the binary, new
SQL present in `strings`): the boot publisher's `Nats-Msg-Id` was `schema-boot-<table>`,
identical on every boot, so any restart inside the stream's duplicate window had its
boot schemas **silently deduplicated by JetStream** — acked, logged as published, never
stored. Exactly the failure mode that makes "deploy a schema-shape change, restart the
bridge, client sees nothing" a mystery. The DDL path never had this: its ids carry
`wal_end`. Boot ids now carry the boot LSN (`schema-boot-<table>-<lsn>`), unique per
boot, still idempotent within one.

The same verification exposed a second boot-only gap: `publishBootSchemas`' key query
filtered on `array_length(i.indkey, 1) = 1` and emitted no `pk_columns`, so a
**composite-key table that saw no DDL since bridge start** published `pk: null` and
nothing else — the client would build it with no primary key and every upsert's
`ON CONFLICT` would misfire. The DDL path has carried the full key since the composite
work; boot now runs the same ordered `unnest(indkey)` query and emits the same
`pk`/`pk_columns` (at the root, per §1.7).

Lesson: the payload has **three** producers — `publishBootSchemas` (boot),
`packDdlToSlot` (DDL), and the suspension publishers — and a wire-shape change is not
done until each is checked. The first two must emit identical shapes (`App.tsx` relies
on it); this is the second time boot lagged DDL (`required` had the same history, per
the boot query's own comment).

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
| `zebridge_catalogue` | **THE catalogue — one row per replicated table, the single source the bridge, the sweeper and the generation producer read.** `tenant_col NULL` = public (the CHECK forces a recorded `public_reason`, so an unscoped, unjustified row is unrepresentable), NOT NULL = tenant-scoped; `version_col`/`tombstone_col`/`tiebreak_col` are the LWW columns; `generations` opts a table out of chain building. UPSERTed by `zebridge_enable` in the same transaction as the guards it installs, loaded by the bridge at boot (rule maps, the public set for CDC_PUBLIC's subject reconciliation) and by the producer per tick. Absorbed and replaced `zebridge_public_tables` and `zebridge_generation_overrides` — both were projections of it. `SYNC_RULES`/`TENANT_RULES` env demoted to per-table emergency overrides. |
| `zebridge_gc_watermark` | Tracks the oldest standing tombstone. Read by clients (via CDC) to determine the maximum allowed offline window before their soft-deleted rows are completely swept and discarded. |
| `zebridge_user_tenants` | Maps NATS principals to their corresponding PostgreSQL tenant IDs. Used by RLS policies and triggers to ensure edge writes correspond to the principal's tenant and route deletes correctly. |
| `zebridge_generations` | The delta-generation producer's own memory (§1.13): one row per built generation of a (tenant, table) pair — `gen`, `cutoff_version`, `cutoff_lsn` (`pg_lsn`), `prev_cutoff` (the delta's lower bound; stored, not derived — pruning removes the row it would be derived from), `has_full` (this gen also shipped a `-full` object, the chain's jump-in point), `built_at`, PK `(tenant, tbl, gen)`. Read back on restart instead of the NATS pointer ("the bridge never reads its own output back"); doubles as the audit trail; pruned past chain depth. Internal-listed and unpublished — clients never replicate producer bookkeeping. Carries the read role's **single** write grant: `INSERT`+`DELETE`, never `UPDATE` (append-only by privilege), because the content query must run as the reader and the bookkeeping row must share its transaction. Contract proven by `scripts/scenarios/generations.py`. |
| `zebridge_limits` | One row per INSTANCE (`slot` PK, `publication`, `max_row_bytes`, `updated_at`), registered by each bridge at boot from its own `2^BASE_BUF` via `zebridge_register_limits()` — never maintained by hand; rows whose slot has left `pg_replication_slots` are GC'd on the next boot. ⚠️ NOT read at write time: a table's budget (MIN over the instances whose publication carries it, via the `pg_publication_tables` join) is BAKED as a literal into its `zebridge_width_guard_<tbl>` body — §10b measured the literal free vs +4.81µs/row for a lookup and +22µs/row for the join — and re-derived at every bridge boot and at every `zebridge_enable` (§10l finding 8). The table is the source; the trigger body is the cache. |

#### Client-Side Tables (`zb-client-ts/src/libzb.ts` — the extracted package; web-consumer and the Node consumer both import it)

| Table Name | Role / Description |
| :--- | :--- |
| `_zebridge_outbox` | Holds optimistic edge writes sent by the client that are pending a confirmation/verdict from NATS and the bridge. |
| `_zebridge_sync` | Stores the `global_last_lsn` (PostgreSQL WAL position) successfully applied by the consumer, allowing it to discard already-seen CDC events. (Contains a legacy `global_last_seq` column). |
| `_zebridge_stream_seq` | One durable position per stream (`last_seq`). The gap check compares it against the stream's `first_seq` on reconnect. Advanced per delivered BATCH, not per applied event (§10m D1): an applied event is in the tables, a gated one is provably in the seeded chain, a held one is durably in the inbox — all three account for the message. |
| `_zebridge_inbox` | Durable FK hold (§10h): events whose parent row has not arrived yet (cross-table/cross-stream ordering is not guaranteed) are parked here in the same transaction that failed them, and replayed bulk-first after later batches. Pruned when a seed covers them (`lsn <= watermark`) or the table is dropped — never by age. |
| `_zebridge_generations` | Per-table generation WATERMARK (`tbl PK, watermark, cutoff_lsn`) — the client tracks cutoffs, never gen numbers (a gen is an object-naming detail; the cutoff is what deltas chain on). On reconnect the chain walk applies only deltas whose `cutoff` exceeds the stored watermark. |

<br>

### PostgreSQL Functions (`init.*.sql`)

The functions are divided into read-only configurations, edge-write guard configurations, and composition helpers.

| Function | Usage / Role |
| :--- | :--- |
| **Helpers / Entry Points** | |
| `zebridge_enable()` | The main orchestration entry point: grants, write guards, RLS, the width guard, the `zebridge_catalogue` UPSERT (tenant/version/tombstone/`tiebreak_col`/generations — the declaration itself, atomic with the guards), the publication ADD, and a post-publication width-guard re-bake (finding 8, §10l). Can dry-run to print its plan. Its T3/T4 rows name the only remaining manual steps: one bridge restart, and the NATS grants. ⚠️ **Tombstone gate** (§10k): `writable => true` without `tombstone_col` is REFUSED — chains are the only seed path and a physical DELETE is inexpressible in an upsert-only delta, so hard-deleted rows resurrect on fresh seeds (measured, §10i). Escape hatch `allow_physical_deletes => true` passes with a WARNING row: mechanical refusal + recorded intent, same pattern as the timestamp and publication guards. |
| `zebridge_is_internal_table()` | Checks if a table belongs to ZeBridge (or standard migration tools) to hide it from schema publications and avoid pointless client-side syncing. |
| **Read/CDC Operations** | |
| `zebridge_scope_reads_by_tenant()` | Enables RLS on a table so the reader's `SELECT`s — today that means the generation producer's content queries, which stamp `zb.tenant` per tenant — are scoped to the session's tenant. |
| `zebridge_tenants_of()` | The generation producer's per-tick tenant set for a tenant-scoped table: `SELECT DISTINCT <tenant_col>` from the DATA, UNION the tenants of `zebridge_user_tenants` (`to_regclass`-guarded so the read-only profile, which lacks that table, still works). The union is what produces an EXPLICIT EMPTY manifest for a known tenant with no rows yet — a client can then distinguish "empty" from "no chain built" (§10i). SECURITY DEFINER, because the reader's own RLS would scope the DISTINCT to one tenant — PG functions hold the rules. |
| `zebridge_publication_guard()` | Event trigger function protecting against unscoped `ALTER PUBLICATION ... ADD TABLE`. Rejects publications of tables without row-filters or RLS unless the catalogue marks them public (`tenant_col IS NULL`). |
| `zebridge_timestamp_guard()` | Event trigger function refusing any `CREATE`/`ALTER TABLE` in `public` that introduces a `timestamp without time zone` column — §7.2's wire format and version clamping need absolute instants. `zebridge_is_internal_table` names are exempt (Ecto's `schema_migrations` is naive by design). |
| `zebridge_audit_publications()` | Audits the database to ensure no tables are published without appropriate tenant scoping. |
| `zebridge_ddl_trigger_fn()` | Event trigger running on `ddl_command_end` to capture modified table schemas directly from the catalog and log them as `INSERT`s into `zebridge_ddl_events`. The schema carries the portage payload (§10c): plain non-partial, non-expression secondary `indexes`, and `foreign_keys` filtered to parents that are themselves replicated with the referenced key PK-or-ported-unique — so a replica can enforce what it can actually satisfy. |
| `zebridge_drop_trigger_fn()` | Event trigger running on `sql_drop` to inform clients when tables are permanently deleted by logging the event into `zebridge_ddl_events`. |
| `zebridge_prune_ddl_events()` | TTL function that deletes events older than 2 days from `zebridge_ddl_events` to bound table growth. |
| `zebridge_widest_row()` | Scans a table's data types to evaluate its byte size floor, ensuring it fits inside NATS message ceilings. |
| `zebridge_oversized_defaults()` | Detects column default values that would break the NATS message size budget. |
| **Write/Ingress Operations** | |
| `zebridge_grant_edge_writes()` | Grants `SELECT`, `INSERT`, `UPDATE`, and `DELETE` on a table to the `bridge_writer` role to permit edge mutations. |
| `zebridge_install_write_guards()` | Helper function that sets up triggers for `bump_version`, `soft_delete`, and `guard_tenant` on a table. |
| `zebridge_register_limits(slot, publication, max_row_bytes)` | **SECURITY DEFINER**, and the only writer of `zebridge_limits`. Called by every bridge at boot over its READER connection (the reader holds `EXECUTE` and no table write privilege — which is what lets a read-only deployment register at all). Three steps: GC rows whose `slot` has left `pg_replication_slots` (a retired instance must stop constraining everyone else), upsert this instance's one row with `2^BASE_BUF`, then RE-BAKE every width guard its publication carries via `zebridge_rebudget_width_guard` — a registration that did not update the baked literals would be a lie until the next enable. The bridge then runs at `min(own buffer, effective MIN across instances)`. Replaces the hand-maintained global row that SECURITY.md flagged and that was duly forgotten the first time `BASE_BUF` moved. ⚠️ Must run AFTER the slot exists, or the GC deletes what the upsert writes. |
| `zebridge_rebudget_width_guard(tbl)` | Re-derives one table's budget and `CREATE OR REPLACE`s the guard FUNCTION BODY only — the trigger object is untouched, so no `ACCESS EXCLUSIVE` on the table (measured: ~203ms body swap vs >6s blocking behind a long transaction for DROP/CREATE TRIGGER). The boot re-bake loop and `zebridge_enable`'s post-publication re-bake both call this. |
| `zebridge_install_width_guard()` | Builds and installs the per-table `zebridge_width_guard_<tbl>` trigger: a static body summing the table's unbounded columns (text/unbounded varchar/bytea/json/jsonb/xml/arrays) against a budget BAKED AS A LITERAL — `MIN(max_row_bytes)` over the instances whose publication carries the table (schema-qualified `format('%I.%I',…)::regclass` join through `pg_publication_tables`), default 16384 — raising SQLSTATE 23514 at write time, the same class-23 code the edge-write path treats as a permanent rejection, so both doors (consumer and psql) are guarded by one trigger with zero listener code. No-op on tables without unbounded columns. ⚠️ At `zebridge_enable` time the publication join is EMPTY for the newborn (the guard installs before the publication ADD, by required order), so enable re-bakes AFTER the publication step — finding 8, §10l. The drop trigger reaps `zebridge_width_guard_<tbl>()` with the table. |
| `zebridge_remove_write_guards()` | Clears the write guard triggers, useful when an admin needs to perform physical cleanups on a table that is restricted to soft-deletes. |
| `zebridge_bump_version()` | Write trigger ensuring that any write omitting the version column (e.g., `updated_at`) gets appropriately stamped with the current timestamp. |
| `zebridge_soft_delete()` | Write trigger converting physical `DELETE` statements on tables with a tombstone column into soft-deleting `UPDATE` operations. |
| `zebridge_guard_tenant()` | Write trigger on tenant-routed tables. An absent/empty tenant is resolved through `zebridge_user_tenants` by `zb.principal` — and **fails closed** (raises) when the principal is unset or unmapped, rather than leaving a row unroutable. A tenant containing a NATS subject metacharacter (`.` `*` `>` or space) is rejected outright: it would route to a subject no consumer receives. |
| `zebridge_scope_writes_by_tenant()` | Multi-tenant configuration for edge writes. Sets up RLS and forces the creation of a unique `REPLICA IDENTITY` index (combining PK and tenant) so PostgreSQL routes `DELETE`s properly over CDC. |
| `zebridge_scope_publication_to_one_tenant()` | Single-tenant alternative that uses a PostgreSQL publication row filter (`WHERE tenant = 'acme'`) to pin an entire publication to a specific tenant value. |
| `zebridge_audit_write_guards()` | Audits all tables and reports which ones possess ZeBridge's write guard triggers. |
| `zebridge_audit_sweeper()` | Checks if the internal sweeper principal (`zb_sweeper`) has any unreachable tenants, thereby helping track tenants whose tombstones won't get collected. |

### Event triggers (the objects the functions above are wired to)

| Trigger | Fires on | Function |
| :--- | :--- | :--- |
| `zebridge_ddl_trigger` | `ddl_command_end` | `zebridge_ddl_trigger_fn()` — captures the schema inside the DDL transaction |
| `zebridge_drop_trigger` | `sql_drop` | `zebridge_drop_trigger_fn()` — drops never reach `ddl_command_end` |
| `zebridge_publication_guard_t` | `ddl_command_end`, tag `ALTER PUBLICATION` | `zebridge_publication_guard()` |
| `zebridge_timestamp_guard_t` | `ddl_command_end`, tags `CREATE TABLE`/`CREATE TABLE AS`/`ALTER TABLE` | `zebridge_timestamp_guard()` |

### Table triggers (installed per guarded table by `zebridge_install_write_guards()`)

These are the trigger *objects* the write-guard functions run as — one set per table,
which is why `zebridge_audit_write_guards()` exists to report which tables actually
carry them:

| Trigger | Timing | Behaviour |
| :--- | :--- | :--- |
| `zebridge_bump_version_t` | `BEFORE UPDATE` | stamps the version column when a writer leaves it untouched |
| `zebridge_soft_delete_t` | `BEFORE DELETE` | converts a physical DELETE into a tombstoning UPDATE — **except** when `zb.principal = 'zb_sweeper'`, the carve-out that lets the sweeper actually reap |
| `zebridge_guard_tenant_t` | `BEFORE INSERT OR UPDATE` | `zebridge_guard_tenant()` — see the functions table |

### Roles & policies

| Object | Kind | Role / Description |
| :--- | :--- | :--- |
| `bridge_reader` (`POSTGRES_BRIDGE_USER`) | PG role | REPLICATION + SELECT everywhere. The read path — unable to write anything a client reads, with one deliberate exception: `INSERT`+`DELETE` (no `UPDATE`) on `zebridge_generations`, its own bookkeeping. The generation producer's content queries are scoped by `zb_reader_all` when the connection sets `zb.tenant`; RLS never touches CDC (logical decoding has no query). |
| `bridge_writer` (`POSTGRES_WRITER_USER`) | PG role | Ingress. Created with **no table privileges** — tables open one at a time via `zebridge_grant_edge_writes()`. Writes are tenant-bounded by `zb_tenant_write` reading `zb.principal`. |
| `zb_sweeper` | **not** a PG role | A `zb.principal` GUC value plus `zebridge_user_tenants` rows (one per tenant, or its reaps see nothing under RLS). Set by the bridge's own sweeper (`src/gc.zig`); recognised by `zebridge_soft_delete_t`'s bypass so tombstones can actually be removed. `zebridge_audit_sweeper()` reports tenants it cannot reach. |
| `postgres` (admin) | PG role | Init/render and migrations only. The bridge never reads its credentials (`args.zig` rejects the PG_* fallback by design). |
| `zb_reader_all` | RLS policy | On tenant-read-scoped tables, for `bridge_reader`: everything when `zb.tenant` is unset, that tenant plus the open tenant when set. |
| `zb_tenant_write` | RLS policy | On tenant-write-scoped tables, for `bridge_writer`: the row's tenant must match the one derived from `zb.principal` — fail-closed when underivable. |

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
> **Tested 2026-08-24, and it holds — `scripts/scenarios/dyntenant.py` is the runnable
> proof.** Against a live bridge, with tenant `dynten` deliberately absent from
> `topology.json`: `CDC_DYNTEN`/`INIT_DYNTEN` provisioned at runtime, a
> `zebridge_user_tenants` mapping propagated to `$KV.tenants.<principal>` with no
> restart, and a `test_types` row with `tenant_id='dynten'` published to
> `cdc.dynten.test_types.insert` in the new stream — routed from the row's tenant
> value alone. One contract is load-bearing: **the stream must exist before the first
> write** — a tenant-routed row whose subject matches no stream is the §2.19 blocking
> shape (no PubAck, retries, eventual bridge FATAL). The backend's provisioning step
> is therefore step 1, not housekeeping.

## 10. libzb — one Zig core, every consumer (2026-08-25)

The consumer-code problem, named honestly: App.tsx is ~2700 lines, and the hard 60% is the write side — outbox, verdict state machine, optimistic apply with two revert modes, clamp, echo-as-confirmation. None of it is accidental complexity; all of it would have to be re-derived per language for the example matrix ([Leaf+Python+PG], [Leaf+Go+SQLite], [Leaf+Phoenix+PG], Flutter, Swift, Node/TS — Ruby viable too: Rails 8 pushes SQLite-in-production, `nats-pure` is official). This section is the answer, so it does not get re-invented worse later.

**The split: eater vs speaker.** Every consumer has exactly two roles, and the socket belongs to only one of them.

- **The eater** — the applier core, sans-I/O: bytes in → SQL + instructions out.
  It NEVER touches a socket, not even to "propagate the good news": an outbound write surfaces as an instruction ("publish these bytes on `mutation.<principal>.<table>.<verb>` with this msg-id"), and the host's transport does the sending. Everything hard lives here: the chain walk (manifest → watermark rule → full-or-deltas plan → 404-means-re-read-from-the-full), the version-GUARDED upsert, the LSN gate, the wire-format normalization (§1.13's `' '` vs `'T'` catch — fixed in ONE place forever), the verdict machine, the outbox rules. "The snapshot business" is deliberately INSIDE: it is the most standardized part of the protocol. Driven as a state machine with an effect queue: host feeds bytes (manifest, objects, CDC events, verdicts), core returns effects (fetch X, run these statements in one transaction, publish Y, persist watermark W / lsn L). No daemon-ness: the loop, threads and liveness live in the speaker; the eater is a function you keep calling — which is also why it is testable by fixtures instead of a harness.

- **The speaker** — the pump that owns the socket, reconnects, the read loop.
  Necessarily native to its platform: `@nats-io/nats-core` over WebSockets in the browser, `nats.zig` (TLS verified 2026-08-24) everywhere else. ~50 lines per platform, not ~800.

**One Zig source, two artifacts.**

- `applier.wasm` — the eater compiled `wasm32-freestanding`. Sans-I/O means NO
  WASI: no sockets, clock, or fs imports, so it runs on any runtime including tiny interpreters. Hosts: browser (native), Node (same V8 bytes as the browser — zero deps), Go (`wazero`, pure Go, no cgo), Python/Ruby/Elixir (official wasmtime bindings). ABI: primitive byte-passing over linear memory; the Component Model/WIT is deliberately skipped until it settles — adopting it later changes no logic.
- `libzb` — the eater + `nats.zig` + linked `sqlite3.c`, compiled natively
  (Zig cross-compiles `aarch64-ios`/`aarch64-android` out of the box; iOS forbids JIT, so native beats interpreted wasm there). The library OWNS the C ABI on the order of: `zb_connect(url, creds)`, `zb_query(sql)`, `zb_mutate(table, key, values)`, `zb_on_change(table, cb)`. Swift consumes the header directly, Dart via `dart:ffi`, Kotlin via JNI, Python `ctypes`, Ruby `fiddle`. Being a LIBRARY, not a process, makes the read-loop thread, reconnect policy and FFI memory ownership deliberate API surface — ordinary C-library discipline, designed once.

**The browser carve-out** (the one place the full-Zig client cannot go): wasm hano sockets, so nats.zig's transport physically cannot run there — and does not need to: the TS pump extracted from App.tsx (`zb-client-ts`) is alread built and debugged. Browser = TS speaker + wasm eater. Server-side wasm WITH sockets (WASI preview-2) is parked as bleeding-edge; servers load the native lib via FFI instead, or use the wasm eater with their own native pump.

**The consumer contract fits on an index card.**

- `query` — arbitrary SELECTs, joins, window functions, run DIRECTLY against the
  local SQLite: the replica IS the API, snapshot-consistent (chain-seeded to one
  cutoff, CDC-advanced in lane order, FK integrity via the PROTOCOL §4 deferral
  rule), offline included. Complex dashboards are the user's business, not the
  API's.
- `mutate(table, key, values)` — a 1:1 constructor for the wire message, three
  verbs. **No SQL interpreter exists anywhere in the library**: SQL is only ever
  GENERATED (inbound verbs → statements at the last inch), never PARSED — no
  dialect on the write path, no injection surface. Mirrors the server's trust
  model, where PostgreSQL never receives client SQL either: the whole pipeline
  moves facts about rows.
- `onChange(table, cb)` — the doorbell; the app re-queries.

**The one rule, made mechanical** ("the ones that only live in prose are the ones that bite" — third occurrence of the pattern after the timestamp and publication guards): direct writes to the replica bypass the outbox and diverge silently, so:
❇️  `libzb` keeps its read-write connection PRIVATE and
❇️ hands the app a second connection opened `SQLITE_OPEN_READONLY` (WAL: readers never block the applier).

A stray UPDATE is an error at the call site, not a violated convention. In the browser tier (one sqlocal connection, OPFS sync handles are exclusive) the package instead simply exports no write path except `mutate()`; the raw handle stays for the SQL console, labeled.

**The conformance harness certifies ONE artifact, not six ports.** A sans-I/O core
is its own harness target: fixtures in, emitted SQL and effects out, byte-for-byte
deterministic — no NATS, PG, or browser required. The scenario suite's tricky
cases (clamp, tiebreak, pruned-mid-walk, the timestamp-format edges) become golden
files; the same fixtures certify the wasm build and the native build (they don't
care where the bytes ran). A language binding then only proves its pump: "I fed
the bytes in order and executed what came out." PROTOCOL §7's verdict machine gets
written down as a one-page transition table (~10 transitions) as part of this.

**Dialect choice**: the eater emits SQL TEXT, SQLite dialect first — matching the
schema descriptors' existing two-dialect (`pg`/`sqlite`) philosophy; the PG
dialect lands when the Python/Phoenix consumers force it (rule of three). Neutral
statement-descriptions were considered and rejected: they push rendering into
every binding, the exact cost this design exists to kill.

**Extraction order** (rule of three, twice): `zb-client-ts` is extracted with App.tsx as its FIRST consumer and the Node example as its second — the browser demo becomes the regression test for the extraction. Likewise no universal SDK abstraction before two ports exist against the protocol directly.

**Extraction step 1 LANDED (2026-08-25): `web-consumer/src/libzb.ts`.** The sync core is out of App.tsx. One `ZeBridge` class (~1500 lines with its comments) holding:

- schema watch,
- chain-first seeding with the snapshot fallback,
- CDC apply (batched `defer_foreign_keys`),
- the whole §7 write path (outbox optimistic apply + two revert modes, verdicts, echo-confirm, rtt liveness), and the doorbell events.

App.tsx fell from ~2780 lines to ~490 of pure theater — signals, badges, demo buttons, the SQL console, the /bridge/health poll — wired through the class's public surface: `query`, `mutate` (returns the stamped version; `rawMutation` stays public as the deliberate escape hatch for the malformed-payload demo `onChange`/`onTableEvent`/`onAnyChange`, `onLog`/`onPhase`/`onSuspended`/`onStatus`, and debug getters that rebuilt the `window.zb` console helpers unchanged.
Verified live as its own regression test: fresh load seeded `users` from chain g4 (12 rows) + CDC delivered the 13th, tenant resolved, and a button write round-tripped MUTATION OUT → PubAck → verdict `accepted` → optimistic row visible.

**Extraction step 2 LANDED (2026-08-26): the `zb-client-ts` package.** `zb-client-ts/` at the repo root (source package: `exports` point at `src/index.ts`, no build step), `libzb.ts` moved in via `git mv`, web-consumer consumes it as `"zb-client-ts": "link:../zb-client-ts"` — consumer #1, unchanged but for one import line. Verified live as the regression §10 promised: fresh load through the package → connected as pia (JWT from sessionStorage, tenant kilo) → 8 tables migrated → chain-first seed (memo g18, counter_public g38, watermark g8; snapshot fallback for the chainless) → CDC caught up (831+825) live → button write round-tripped MUTATION OUT (seq 3122) → verdict accepted → row in PG under kilo.

One real lesson, recorded in `vite.config.ts` where it bit: a **linked package's deps resolve inside its own node_modules and are served over `/@fs/` URLs — and vite does not resolve extensionless requests there**. sqlocal spawns its worker with `new Worker(new URL('./worker', import.meta.url))`; under `/node_modules/` vite resolves `worker` → `worker.js`, under `/@fs/` it fell through to the SPA fallback and served **index.html with a 200**. The worker parsed HTML, died silently, and every DB call hung forever — no error in any console, no socket ever dialed, because `connect()`'s first await is `initSyncState()`. Diagnosed by racing `SELECT 1` against a timeout, then fetching both worker URLs and comparing content-types. Fix: `resolve.dedupe` for sqlocal and the shared runtime deps (@nats-io/*, msgpack, uuid) so the linked package uses the consumer's copies. The same class of hazard awaits any host that embeds the library: **the storage adapter owns a worker, and the worker's URL resolution is the host bundler's problem** — an argument for making the storage seam explicit (step 3) instead of letting sqlocal's spawn hide inside the core.

**Extraction step 3 LANDED (2026-08-26): the seams, and consumer #2.** The core no
longer names a driver or a transport. `src/storage.ts` declares the seam (`Exec`,
`transaction`, `deleteDatabaseFile`, a `StorageFactory` the HOST supplies);
`browser-storage.ts` holds the sqlocal default; `node.ts` holds better-sqlite3 +
`@nats-io/transport-node`, exported as `zb-client-ts/node`. `ZeBridgeConfig` gained
`storage` and `connect`; the browser keeps working by defaulting to both.
`examples/04-node-consumer/` is consumer #2, verified live: seeded 8 tables into
better-sqlite3, CDC caught up (832 + 1282), `mutate()` round-tripped to an accepted
verdict, outbox drained, and every table's row count matched Postgres exactly.

Four things the second consumer taught, none of which a single host could have:

- **Node's ESM resolver needs explicit extensions.** Vite tolerated `from './libzb'`;
    Node does not. All internal specifiers now carry `.ts`.
- **Type-only imports must say `import type`.** Node strips types syntactically,
    without analysis, so a value-position type import is a runtime SyntaxError.
    `verbatimModuleSyntax: true` is now on, so tsc catches the whole class.
- **The storage contract was implicit.** The core drives several lanes at once (a
    CDC consumer per stream + the write path) and calls `transaction()` concurrently.
    sqlocal satisfied that invisibly — one worker, one queue — so nobody had to say it.
    better-sqlite3 does not, and interleaved BEGINs dropped whole 100-event batches
    (`cannot start a transaction within a transaction`). The rule is now WRITTEN in
    storage.ts and honoured by a promise chain in node.ts.
- **A host bundler can silently serve HTML as a worker.** (Step 2's lesson; the
    reason the storage adapter belongs to the host, not the core.)

Next: carve the sans-I/O eater boundary inside the package, capturing the tricky
cases as golden fixtures AS THEY ARE WRITTEN (clamp, tiebreak, pruned-mid-walk,
timestamp edges) — the fixtures, not "Node works", are what proves the core is
portable to Zig.

**Rejected on the way here, recorded so it stays rejected**: the reverse architecture (push RAW pgoutput to NATS, decode at the edge / wasmCloud). It breaks the tenant ACL — subject routing REQUIRES decoding (the tenant column lives in the decoded tuple), so raw frames hand every edge decoder all tenants mixed — and pgoutput is stateful (tuples reference relation OIDs from earlier Relation messages: every consumer grows a relcache, late joiners can't decode, retention must preserve schema messages forever). The gain would have been offloading ~3.5µs/event of decode CPU that was never scarce. The salvageable adjacent idea: an optional **SQL lane** — a small worker (wasmCloud fits HERE) consuming the already-tenant-routed streams and republishing rendered SQL phrases per dialect (`cdc-sql.<dialect>.<tenant>.<table>.>`), making ~50-line consumers possible for teams that want them. Behind the bridge, opt-in, ACL intact.

Context this slots into: the leaf topology (bridge↔NATS plain TCP colocated, NATS↔leaves TLS, PG↔bridge SSL for PG-as-a-service; one leaf per tenant whose hub credential carries that tenant's grants; ~20ms leaf latency exercising LWW and the 5s clamp in anger) and the ~20MB bridge footprint (ring buffer is the knob, arenas, no GC). Small tool, sharp contract — the contract is the investment.

## 10b. The row-width budget registers itself (2026-08-26)

`zebridge_limits` was ONE global row and a coupling maintained by hand — SECURITY.md
said so in a ⚠️ line, and it was forgotten the first time `BASE_BUF` moved: buffer
4 KB, table still 16384, so PostgreSQL would accept rows the feed could not carry
and suspend the table on the first CDC touch. Found by reading the boot log after a
sizing change, not by any test.

**Two things were wrong, not one.** The forgetting was the symptom; the design error
was a UNIVERSAL limit. `BASE_BUF` is a per-process setting, so two bridges on one
database can legitimately carry different ceilings, and no single row can be true
for both.

**Shape, chosen by measurement.** The guard fires on every insert/update, so the
budget lookup is hot-path. Timed against the live DB, 2000 iterations each:

| lookup | us/row |
| --- | --- |
| old single global row | 2.19 |
| resolve publication per row (`pg_publication_tables` join + slot liveness) | 22.14 |
| slot-liveness only | 4.35 |
| **per-(table,slot) indexed, liveness GC'd at boot** | **3.01** |

Resolving the publication inside the trigger costs 10x — a 100k-row bulk insert
would pay ~2s of pure guard overhead. So the COLD path resolves and the HOT path
reads: `zebridge_register_limits(slot, publication, bytes)` expands publication →
tables once at boot and writes ONE ROW PER INSTANCE (`slot` PK); the trigger reads
nothing at all — its budget is a baked literal, re-derived at each boot as MIN over
the instances carrying that table.

⚠️ Superseded within the day, twice, and both corrections are the point. The first
shape keyed rows by (tbl, slot) so the trigger could do a cheap indexed lookup; the
review then asked why the table is stored per-table at all when the per-slot fact
is what an instance knows — and measuring answered it: a literal costs NOTHING
per row (2.04 us against 2.88 with no guard) where even an indexed lookup cost
+4.81. Once the hot path reads nothing, the per-table denormalisation had no
purpose left, and the storage collapsed to one row per instance with the
publication join done once at boot.

**Why `slot` and not `publication` as the key** — the question that decided it: PG
cannot map a table to a slot (`pg_create_logical_replication_slot` takes no
publication; the binding is per-session at `START_REPLICATION` and never stored),
so publication is what the guard must join on. But `slot` is what
`pg_replication_slots` KNOWS, which makes retired instances detectable: any bridge's
boot deletes rows whose slot is gone. Keyed by publication, a decommissioned 4 KB
instance would pin the budget low forever with nothing able to tell. So: slot for
identity and liveness, publication for scope. `MIN` because a row must fit the
narrowest instance carrying the table.

**Why a SECURITY DEFINER function rather than a grant.** A read-only deployment has
no writer role, so the bridge must register over the READER connection — and
granting the reader INSERT/UPDATE would have cost the invariant the whole read path
leans on. `zebridge_register_limits` is SECURITY DEFINER with `EXECUTE` granted to
the reader: one code path for both profiles, and `bridge_reader` still holds no
write privilege on any user table (the two internal grants on
`zebridge_generations` predate this and are unchanged).

**A latent bug found on the way**: `init.core.template.sql` granted
`zebridge_catalogue` SELECT to `${POSTGRES_WRITER_USER}` — violating its own header
rule ("nothing here may reference the writer role"), which breaks the read-only
profile on a FRESH cluster. It passed everywhere it was ever run because roles are
cluster-wide and the write half had always been applied first. Moved to
init.write.template.sql.

## 10c. What a PostgreSQL schema carries, and what reaches the replica (2026-08-26)

The client builds its local tables from `$KV.schemas.<table>`, so whatever the
schema generator does not emit simply does not exist on the edge. Taking stock of
what a PG table declares against what actually lands:

| feature | status | consequence of the gap |
| --- | --- | --- |
| column types | ✅ ported | two dialects, `pg` and `sqlite` |
| PRIMARY KEY | ✅ ported | `pk` + `pk_columns` (composite-safe) |
| **NOT NULL** | ✅ ported 2026-08-26 | was computed then dropped; the replica accepted rows PG refused |
| **indexes** | ✅ ported 2026-08-26 | root-level and dialect-neutral; partial/expression/non-btree excluded |
| **FOREIGN KEY** | ✅ ported 2026-08-26 | was finding 6; needed pragma parity + an order-tolerant applier (§10d, §10e) |
| UNIQUE constraints | ✅ ported 2026-08-26 | via the index port — PG backs them with a unique index and SQLite implements them AS one, so enforcement is identical; verified `UNIQUE constraint failed: users.email` on the replica |
| DEFAULT values | ❌ | a client must supply every column on an insert |
| CHECK constraints | ❌ | includes PG enums, which arrive as bare TEXT |

Deliberately NOT ported, and correctly so: sequences and identity columns (the
`DbAllocatedKey` rule refuses server-allocated keys on writable tables — a client
mints its own), and triggers / RLS / partitioning, which are server-side concerns
a replica has no business mirroring. Beyond those, the table above is the whole
list.

**SQLite supports all three of the missing structural ones natively** — `NOT NULL`,
`CREATE INDEX`, and `FOREIGN KEY ... REFERENCES` — so nothing here is blocked by
the target engine. The gap is entirely in what the generator gathers.

**NOT NULL is the sharp one, because the data is ALREADY on the wire.** The `pg`
dialect carries `required` per column and the `sqlite` dialect drops it:

    pg     : {"name": "user_id", "type": "bigint", "required": true}
    sqlite : {"name": "user_id", "type": "INTEGER"}

and the client's generated DDL adds `NOT NULL` only to primary keys, so
`CREATE TABLE users ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT, ...)` —
where `name` is NOT NULL in PostgreSQL. Measured, not theorised: the first
`mutate()` from the Node consumer applied LOCALLY and was then refused upstream
with `null value in column "inserted_at" violates not-null constraint`. The
replica accepted a row PostgreSQL would not. That is the same
"accepted locally, rejected upstream" round trip the ingress width cap exists to
kill (finding 5's sibling), except here the information needed to refuse it
locally was already published and thrown away.

**Ordering, by payoff over cost:**
  1. ~~NOT NULL~~ — DONE 2026-08-26. `required` now travels in both dialects and
     the client emits it. Verified: the replica refuses locally with `NOT NULL
     constraint failed: orders.user_id`, where before the write applied and came
     back a 23502 verdict.
  2. ~~indexes~~ (and UNIQUE with them) — DONE 2026-08-26. Root-level,
     dialect-neutral; only btree, non-partial, non-expression indexes travel,
     since anything excluded costs a scan and never a wrong row. Verified:
     `EXPLAIN QUERY PLAN` reports `SEARCH orders USING INDEX orders_user_id_idx`.
     The client syncs on the identical-schema path too — adding an index upstream
     changes no COLUMN, so that is exactly where its republish lands.
  3. ~~FOREIGN KEY~~ — DONE 2026-08-26, and it needed all of §10d: pragma parity
     across the adapters, a detector that knows SQLite's THREE FK messages, and an
     order-tolerant applier. Porting the constraint made §10e's cross-table
     reordering visible for the first time — it had always been there.
  4. ~~UNIQUE~~ — DONE with the index port. A PG UNIQUE constraint is backed by a
     unique index, SQLite implements UNIQUE AS an index, so shipping the index
     ships the enforcement. Verified end to end.
  5. **Remaining, both low value**: DEFAULT values and CHECK constraints (including
     PG enums, which arrive as bare TEXT). A replica receives COMPLETE rows, so
     defaults matter only for local optimistic inserts, and a CHECK the server
     already enforced cannot be violated by a row the server accepted. Neither is
     a correctness gap for replication; both would only fail a bad local write
     slightly earlier.

**A rule this pair established, worth keeping**: there are TWO schema paths — the
DDL trigger and the boot-time query — and a field added to only one gives it to
tables created while the bridge runs while silently leaving every table present
at boot on the old shape. NOT NULL nearly shipped that way. Both paths, every
time, and the boot path is the easy one to forget because it is not the one you
are testing when you write the feature.

## 10d. What porting FOREIGN KEYs actually requires (2026-08-26)

Measured against SQLite before writing any of it, because the docs' rules bite in
ways that decide the design.

**1. `foreign_keys` is per-connection and OFF by default — and our two adapters
DISAGREE.** better-sqlite3 turns it ON when it opens a database; sqlocal never
sets the pragma, so the browser gets SQLite's default of OFF. Measured: the live
Node replica reports `foreign_keys: 1`, and nothing in sqlocal's dist mentions
the pragma at all. Port FKs today and they would be ENFORCED in Node and IGNORED
in the browser — the same core, the same data, different integrity semantics per
consumer. Invisible right now only because no FK exists to enforce.

That is a storage-seam bug independent of FKs: the adapters must agree on pragmas
that change SEMANTICS (`foreign_keys`), as opposed to ones that only change
performance (`journal_mode`). Fix it in `storage.ts` as part of the contract, not
in each adapter's private setup.

**2. FK resolution is LAZY at DDL, strict at DML.** `CREATE TABLE child(a, b
REFERENCES nosuch(id))` succeeds even though `nosuch` does not exist; the INSERT
then fails with `no such table: main.nosuch`. So schema ARRIVAL ORDER is not a
problem for table creation — the client can keep building tables in whatever
order the KV watch delivers them — but a child that receives CDC before its
parent table exists will fail at apply time.

**3. Two failure messages, neither of which says "FOREIGN KEY constraint
failed".** Measured:
    parent table missing        -> `no such table: main.nosuch`
    parent column not unique    -> `foreign key mismatch - "c4" referencing "parent"`
The applier's `isForeignKeyFailure` matches only /FOREIGN KEY constraint failed/,
so both of those would be classified as permanent and DROPPED rather than held.
The detector has to broaden before FKs travel, or the hold/retry silently becomes
a delete.

**4. The parent key must be the PK or carry a UNIQUE index.** SQLite refuses
`foreign key mismatch` otherwise — the docs' child4 case, reproduced. A plain
index does NOT satisfy it; a UNIQUE one does (reproduced both ways). Since §10c's
index port now ships unique btree indexes, most PG unique constraints arrive
already satisfied — but an FK whose parent key is covered only by a PARTIAL or
EXPRESSION unique index (which we deliberately do not port) would mismatch. So
the FK filter must check that the parent key is the PK or has a PORTED unique
index, not merely that PostgreSQL considered it unique.

**5. Adding an FK upstream forces a client table REBUILD.** SQLite has no
`ALTER TABLE ADD CONSTRAINT`, so unlike an index — created and dropped freely —
an FK change means `rebuildPreservingData`. FK churn is therefore expensive in a
way index churn is not, which argues for porting FKs once at table creation and
treating later constraint changes as the rare, costly case they are.

**Order to build it, given the above:**
  1. pragma parity in the storage seam (`foreign_keys = ON` in both adapters) —
     needed regardless, and it is what makes the browser and Node agree;
  2. broaden the FK-failure detector to the two messages above, so the applier's
     hold/retry actually holds instead of dropping;
  3. emit FKs from BOTH schema paths, filtered to parent keys that are the PK or
     have a ported unique index, and have the client declare them inside
     `CREATE TABLE`;
  4. re-run the 30k interleaved split test WITH constraints present, where it
     stops being a vacuous pass (§10c, finding 6).

⚠️ Enabling enforcement makes the replica STRICTER than it is today: a local
optimistic write that violates an FK starts failing where it used to succeed.
That is the point — it is the same argument as NOT NULL, failing at the call site
instead of round-tripping a verdict — but it is a behaviour change for existing
consumers, not a pure addition.

## 10e. The wire does NOT preserve ordering ACROSS tables (2026-08-26)

Found while making foreign keys real (§10d): a child row can reach a client before
its parent, at ANY transaction size, in an ordinary write. This is a protocol-level
property that was never written down, and it became load-bearing the moment §10c's
FK port gave clients a constraint that can notice it.

**Measured.** 1000 users + 1000 orders inserted INTERLEAVED in one transaction —
2,000 events, comfortably UNDER the 4095 flush quantum, so no transaction split was
involved at all:

    seq 61757 -> cdc.users.insert.batch
    seq 61758 -> cdc.orders.insert.batch
    seq 61759 -> cdc.users.insert.batch
    seq 61760 -> cdc.orders.insert.batch
    seq 61761 -> cdc.orders.insert.batch     <- children
    seq 61762 -> cdc.users.insert.batch      <- their parents, published AFTER

322 events failed their FK on arrival and had to be held. Testing the UNDER-limit
case is what found this: the assumption had been that transaction splitting caused
it, and the un-split case disproved that in one run.

**Mechanism, and it is not a bug in the grouping order.** `publishCDCSubBatch`
groups a flush run's events by subject and emits groups in order of FIRST
APPEARANCE, which is WAL order — that part is correct. The reorder comes from
grouping COLLAPSING the interleave. Take a run that begins mid-pair:

    run N   : ... user[k]
    run N+1 : order[k], user[k+1], order[k+1], user[k+2], ...

In run N+1 the first subject seen is `orders`, so the orders group is emitted
first and carries order[k], order[k+1], order[k+2]... while the users group
emitted after it carries user[k+1], user[k+2]... So order[k] is fine (its parent
was in run N) and EVERY LATER ORDER IN THAT RUN jumps ahead of its parent.

**The trade this exposes.** Preserving cross-table order exactly would mean
grouping only CONSECUTIVE same-subject events, and with interleaved data that
degenerates to batches of one — losing the batching that the `.batch` subject
exists for. Grouping is worth having; the ordering guarantee is the price.

**So the protocol fact is: CDC preserves order WITHIN a table, not ACROSS tables.**
A client must be order-tolerant. For most consumers this has always been
invisible — rows are applied by primary key, LWW, idempotent — which is why it
went unnoticed for so long. It becomes visible exactly when a client holds a
constraint that spans tables, i.e. a foreign key.

⚠️ This belongs in PROTOCOL.md next to §4's deferral rule, which is the
INTRA-batch half of the same problem: `defer_foreign_keys` handles a child
arriving before its parent inside ONE batch, and nothing handled it across
batches until the applier's hold/retry (§10d). The two together are what make FKs
survivable; either alone is not enough.

## 10f. A monotonic event stamp, and why it is not sufficient alone (2026-08-27)

Raised in review: refusing to port an FK whose parent lives in another stream is
too restrictive — a PUBLIC `users` parent with a PRIVATE `salaries` child scoped
to one tenant is exactly the multi-tenant shape this product exists for, not an
edge case. Withdrawn. The alternative raised was PowerSync's: give every event a
monotonic unique id, which is sound here precisely because pgoutput hands us a
total order.

**What PostgreSQL actually guarantees, measured.** Within one replication slot,
logical decoding delivers transactions in COMMIT order and each transaction's
changes in execution order. That is causal order: PostgreSQL enforces a foreign
key at statement time, so a parent always precedes its child in the same
transaction or sits in an earlier-committed one.

**And the sharp edge that makes a stamp necessary rather than cosmetic:** LSNs
are NOT monotonic in delivery order. A transaction that begins earlier and
commits later arrives later carrying a LOWER lsn:

    lsn=8700486880  T2-committed-first    <- delivered FIRST
    lsn=8700486488  T1-first-write        <- lower lsn, delivered SECOND
    lsn=8700487104  T1-second-write

So a client must NEVER sort by lsn to recover order — that would reorder across
commit boundaries and actively break causality. (libzb is safe today by design
rather than luck: `state.lsn` advances only at SEEDING, never per applied event,
so it is a "was this in my snapshot?" watermark. Had it tracked a running
maximum, `T1-first-write` would have been silently dropped. Verified 3/3 rows
landed.)

**A monotonic stamp is cheap and correct here.** The WAL loop is single-threaded
and decodes in commit order, so a counter incremented as each event is packed IS
the global total order — no coordination, no clock, no ambiguity. That gives a
client something it can legitimately sort by, across streams, which `lsn` cannot
provide.

**But sortable is not sufficient.** A client sees only a SUBSET of the global
sequence — its own tenant plus public — so gaps are permanent and expected, and
indistinguishable from "still in flight". Holding stamp 100 from CDC_PUBLIC and
105 from CDC_kilo, it cannot know whether 101-104 belong to a tenant it may not
read or are about to arrive. Ordering what you have does not tell you when it is
safe to apply.

That missing half is exactly what PowerSync's CHECKPOINT is: a barrier saying
"you are consistent as of X", letting the client apply everything up to it
atomically. The full design is therefore stamp + per-scope watermark, and the
watermark is the substantial part — it needs the bridge to know, per client
scope, what it has finished publishing.

**Where that leaves us.** The order-tolerant applier (§10d/§10e) is required
regardless, because it is the only mechanism that handles a gap the client cannot
interpret. A stamp would make holds rarer and give a deterministic apply order
for what has arrived; a checkpoint would make partial transactions unobservable,
which is a STRONGER guarantee than ZeBridge offers today. Neither is a
prerequisite for FKs working — they already do — so this is an improvement to
sequence deliberately, not a hole to plug.

## 10g. A stale snapshot can DESTROY a correct replica (2026-08-27, OPEN)

Found in a clean room while chasing a 3,000-row discrepancy. Not yet fully fixed —
the guard added is partial, and the underlying gap is a design one.

**What happens.** `replaySnapshot` begins with `DELETE FROM <table>` and then
replays the descriptor's chunks. So replaying a STALE descriptor does not merely
fail to help, it destroys data. Measured, from the client's own log:

    Cached snapshot for orders is orphaned (CDC no longer covers its watermark)
      — requesting a fresh one instead
    Request for orders refused (maximum messages per subject exceeded)
      — a snapshot is already pending, waiting for it
    Snapshot metadata ready for orders (LSN 8722029312, 0 rows). Replaying...
    Replay finished for orders (0 rows applied)
    => orders = 0            (it held 5,000 correct rows a moment earlier)

Every step is ordinary. The client judged its cached snapshot orphaned, asked for
a fresh one, the **SNAP_RET throttle refused it** because one was already pending,
and it fell back to that pending descriptor — which had been taken when the table
was EMPTY and carried an LSN (8722029312) BEHIND the client's own position
(8728358024). It rewound, and `DELETE FROM` did the rest.

**This is finding 4's shape on the client side**: resume forward, never rewind. A
snapshot older than data already held is not a baseline, it is a regression.

**Partial fix in place**: `replaySnapshot` now refuses a descriptor when the table
HOLDS ROWS and the descriptor's lsn is behind `globalSyncState.lsn`. An empty
table has nothing to lose and always seeds — which matters, because on a fresh
replica `state.lsn` starts far ahead of any descriptor (see applyEvent's gate), so
a naive lsn comparison would refuse every legitimate first seed.

**Why that is not enough, and what the real gap is.** The client has no per-table
DATA watermark. `global_last_lsn` is global across tables, and `state.lsn` is the
SCHEMA's lsn, not the position its rows are at. So the client cannot reliably
answer the only question that matters here — "is this descriptor older than the
data I hold for THIS table?" — and a second run still emptied `users` with the
guard in place. The fix is to track an applied-position per table and compare
against that; anything less is guessing with a DELETE in hand.

**A SECOND defect, found by the same hunt and now fixed: head-of-line blocking.**
The seeding loop awaited each table's whole request/retry cycle INLINE, so one
table that could not be seeded starved every table after it for
SNAPSHOT_REQUEST_ATTEMPTS x SNAPSHOT_WAIT_MS — 5 x 60s. Measured: `orders`
appeared to be silently failing to seed, and three separate hypotheses were
chased (the ported FK, concurrent bulk-load ordering, an empty chain object)
before the real cause showed: `test_types` sat AHEAD of it in the loop, orphaned
and throttled, and `orders` was simply never reached inside the test's timeout.
Five minutes of head-of-line blocking presenting as data loss.

Seeding is now one promise per table: a table that cannot seed is its own failure
(`this.failed`), never a reason to starve the rest. Verified — `orders` seeds
1,503 rows while `test_types` is still stuck, replica matches PostgreSQL exactly.

⚠️ The lesson for the next hunt: when a table "silently fails", check whether it
was ever REACHED before theorising about why it failed. Three wrong hypotheses
cost more than reading the unfiltered log once would have.

**Also worth pulling on**: the throttle refusing a fresh snapshot and the client
then USING the stale pending one is a bad pairing. A refusal should not silently
degrade into "use whatever is cached" — the safe response to "no fresh snapshot
available" is to keep the current data and stay on CDC, not to rewind.

⚠️ Until the per-table watermark exists, a client that loses CDC coverage while
holding data can still be emptied by a stale descriptor. The window is narrow
(it needs an orphaned cached snapshot plus a throttled request) but it is real,
and the failure is silent and total for that table.

## 10h. The foreign-key port, end to end — findings and wrong turns (2026-08-27)

A consolidated record of the FK work, because the useful part is not the diff: it
is what porting one constraint EXPOSED about the wire, and how many wrong
hypotheses it cost before the real causes surfaced. Details live in §10c–§10g;
this is the thread through them.

### The starting point

`pg_constraint` appeared ZERO times in the whole Zig source. `orders` was declared
in the migration as the FK-ordering demo ("child of users, PROTOCOL.md §4") and
reached every replica as columns and a primary key, nothing else — measured on
`$KV.schemas.orders`: no `foreign`, `references`, `fkey` or `constraint` key
anywhere. So PROTOCOL §4's deferral rule was unimplementable by any consumer, and
libzb's `PRAGMA defer_foreign_keys = ON` guarded a constraint that could not
exist. The demo table was there; the metadata that would make it a demo was not.

### What porting it actually required (all measured, §10d)

  * **`foreign_keys` is per-connection, OFF by default, and our adapters
    DISAGREED** — better-sqlite3 turns it on at open, sqlocal never sets it. Port
    FKs and they would be ENFORCED in Node and IGNORED in the browser: the same
    core, the same data, different integrity per consumer. Now part of the storage
    contract, with the line drawn at PERFORMANCE pragmas (an adapter's business)
    versus SEMANTIC ones (the seam's).
  * **SQLite reports THREE different failures** and only one says what you expect:
    `FOREIGN KEY constraint failed` (missing parent ROW), `no such table:
    main.<parent>` (missing parent TABLE — resolution is lazy at DDL, strict at
    DML), and `foreign key mismatch` (parent key is not the PK and has no UNIQUE
    index). The applier matched only the first, so the other two would have been
    classed permanent and DROPPED — the hold/retry would silently have become a
    delete.
  * **The parent key must be the PK or carry a UNIQUE index**, reproduced both
    ways. The §10c index port ships unique btree indexes, so most PG unique
    constraints arrive already satisfied — but a parent key unique only under a
    PARTIAL or EXPRESSION index is unique to PostgreSQL and unsatisfiable here, so
    the filter checks for an index we ACTUALLY PORT.
  * **No `ALTER TABLE ADD CONSTRAINT` in SQLite**, so an FK change forces a table
    rebuild where an index change is a cheap CREATE/DROP.

### What it EXPOSED: the wire does not order across tables (§10e)

This is the finding that outlived the feature. A child can reach a client before
its parent at ANY transaction size — measured on 2,000 events comfortably under
the flush quantum, so no transaction split was involved:

    seq 61761 -> cdc.orders.insert.batch     (children)
    seq 61762 -> cdc.users.insert.batch      (their parents, published AFTER)

The grouping ORDER is correct (groups emit in first-appearance order, which is WAL
order). What breaks it is grouping COLLAPSING the interleave: a run beginning
mid-pair emits the orders group first, carrying every later order, while the users
group after it carries their parents. Preserving order exactly would mean grouping
only CONSECUTIVE same-subject events, which with interleaved data degenerates to
batches of one — so grouping is worth having and the ordering guarantee is its
price. **CDC preserves order WITHIN a table, not ACROSS tables.** Invisible to any
consumer applying by primary key with LWW; visible precisely when a client holds a
constraint spanning tables.

### And the trap underneath it: LSN is not monotonic in delivery order (§10f)

PostgreSQL gives a strict total order — transactions in COMMIT order, changes
within a transaction in execution order — which is causal order, exactly what a
foreign key needs. But delivery follows COMMIT order, so a transaction that begins
earlier and commits later arrives later carrying a LOWER lsn:

    lsn=8700486880  T2-committed-first    <- delivered FIRST
    lsn=8700486488  T1-first-write        <- lower lsn, delivered SECOND

So a client must NEVER sort by lsn to "restore" order — it would reorder across
commit boundaries and break causality. libzb survives this by design rather than
luck: `state.lsn` advances only at SEEDING, so it is a "was this in my snapshot?"
watermark, not a running maximum. Had it tracked a maximum, `T1-first-write` would
have been silently dropped; verified 3/3 rows landed.

**The monotonic-id idea (PowerSync's), raised in review**: give every event a
stamp the client CAN sort by. Sound here, and cheap — the WAL loop is
single-threaded and decodes in commit order, so a counter incremented as each
event is packed IS the global total order. But **sortable is not sufficient**: a
client sees only a SUBSET of that sequence (its tenant plus public), so gaps are
permanent and indistinguishable from "still in flight". Holding stamp 100 from
CDC_PUBLIC and 105 from CDC_kilo, it cannot know whether 101-104 belong to a
tenant it may not read. The missing half is a per-scope CHECKPOINT — "you are
consistent as of X" — and that is the substantial part. Not built.

Also withdrawn on review: refusing to port an FK whose parent lives in a different
stream. A PUBLIC `users` parent with a PRIVATE `salaries` child scoped to one
tenant is the multi-tenant shape this product exists for, not an edge case. Which
means cross-stream FKs stay unorderable and the order-tolerant applier is required
regardless — a stamp would make holds rarer, never unnecessary.

### The applier, in layers

  1. batch apply inside one transaction with `defer_foreign_keys` — handles a child
     before its parent WITHIN a batch (PROTOCOL §4's rule, and the common case);
  2. on batch failure, ISOLATED replay one event at a time, so one bad row cannot
     discard 99 innocent ones (it used to log and ack anyway: ~600 events were lost
     that way during the Node-consumer work);
  3. missing-parent failures are HELD and retried after each later batch —
     bulk-first in ONE transaction, since per-event retry after every batch is
     O(held x batches) and measured at ~765,000 transactions for 15,000 held,
     which never converged and starved the batches carrying the parents;
  4. holds are DURABLE (`_zebridge_inbox`) — in memory alone they were lost on
     restart while already ACKed, so JetStream would never redeliver them.

Verified: 15,000 users + 15,000 orders interleaved in one transaction, ring 4096
so it split across ~8 releases, FK live — 7,212 held, 7,212 resolved, 0 dropped,
30,000/30,000, zero orphans. And across a kill: 4,280 holds persisted, restored
on restart, drained to 15,000/15,000.

### The wrong turns, recorded because they cost the most

Chasing "`orders` silently fails to seed", THREE hypotheses were pursued and code
was changed on each:

  1. **the ported FK** — plausible, since orders is the only table with one. Wrong.
  2. **concurrent bulk-load ordering** — seeding runs every table in parallel, so a
     child's rows could hit the constraint before its parent's landed. Wrong as the
     cause, though the fix (FK enforcement OFF during the bulk load, re-armed with
     `foreign_key_check` after) is correct and kept: a seed is a bulk load of an
     already-consistent snapshot.
  3. **an empty chain object** — disproved by the object store: `orders-g3-delta`
     was 165 KiB and the manifest well-formed.

The actual cause was in none of them: the seeding loop awaited each table's whole
request/retry cycle INLINE, so `test_types` — orphaned and throttled, sitting
AHEAD of `orders` — starved it for 5 attempts x 60s. `orders` was never REACHED
inside the test's timeout. Five minutes of head-of-line blocking presenting as
data loss.

**The lesson, and it generalises**: when something "silently fails", establish
that it was REACHED before theorising about why it failed. The unfiltered log said
so the whole time; grepping for the expected failure hid it. Three wrong fixes
cost more than reading the log once would have.

### Retired instead of fixed (2026-08-27)

The client's snapshot-on-demand seeding is now GATED OFF (`LEGACY_SNAPSHOT_SEEDING
= false` in libzb.ts — kept readable, not erased: the path earned three findings in
one day and the code is their documentation). Seeding is generations-only, with a
bounded wait (`GENERATION_WAIT_MS`, polled) for a chain that has not been built
yet; a still-chainless table fails LOUDLY, is excluded from CDC, and seeds on the
next connect. The bridge still SERVES snapshots — old consumers and the scenario
suite are untouched; only this client stopped asking.

That retirement DISSOLVES two of the §10g opens for this client (the stale
descriptor cannot rewind a table it never replays; the throttle cannot deadlock a
request never made) — and immediately exposed the retired path's one real virtue:

**"No chain" is ambiguous, and the snapshot path used to disambiguate it.**
Solved live: `test_types` would not seed for omar, and the cause was not the
throttle after all — its 33 rows belong to `acme`/`dynten`, omar is `kilo`, and
the producer derives tenants FROM DATA, so a `kilo.test_types` chain simply never
exists. "No chain for my tenant" means EITHER "producer has not built yet" OR
"the correct state is empty", and the old path answered the second case with a
0-row RLS-scoped dump. Guessing "empty" would silently diverge when the producer
is merely down, so the client currently refuses loudly — correct but overly
strict for the genuinely-empty case. The producer publishing an explicit empty
manifest per known (tenant, table) — or a tenant-scoped "nothing to seed" marker —
is the clean fix; NOT built.

**CLOSED same day — the empty manifest.** `zebridge_tenants_of` now UNIONs the
data-derived tenant set with every tenant the system KNOWS
(`zebridge_user_tenants`), so a known tenant with no rows in a table gets a
0-row full built for it — the explicit "nothing to seed". The client seeds zero
rows, anchors its watermark, and follows CDC; a dyntenant that exists only as
data still gets its chain from the data half of the union, so neither source is
dropped. Guarded with `to_regclass` because `zebridge_user_tenants` is created by
init.write and plpgsql resolves tables at CALL time — an unguarded reference
would pass the apply and then fail every producer tick on a read-only deployment.
No bridge change and no restart: the producer calls the function fresh each tick.
Verified: `kilo.test_types` g1 built on the next cadence, the client seeded it as
0 rows with a watermark, no exclusions, no 90s wait — and 0 rows also confirms
the tenant scoping held (none of acme's rows leaked into kilo's chain).

### Open

  * the monotonic stamp + per-scope checkpoint (§10f) — designed, not built;
  * the per-table DATA watermark (§10g) — still wanted for CDC-side safety even
    with snapshot replay retired;
  * bridge-side retirement (snapshot listener, REQUESTS/SNAP_RET, INIT streams) —
    deliberately untouched so old consumers and the scenario suite keep working;
    remove once no consumer asks.

## 10i. Finding 7: the LSN seed gate LOSES in-flight transactions (2026-08-27)

Found by pressing on the question "is the monotonic stamp actually useful?" — the
answer turned out to be a measured data-loss bug, and the stamp we need turns out
to ALREADY EXIST.

**The experiment.** Session A: `BEGIN; INSERT 'inflight-A'; pg_sleep(45); COMMIT`.
While A is open, an ordinary committed row forces the producer to build new
generations (g2 at 10:44:24, g3 at 10:44:44 — both while A is still open, so both
snapshots exclude it). A commits at ~10:45. PostgreSQL then holds both rows; the
CDC stream carries both events; a FRESH replica seeding from g3 ends with only
`trigger-B`. `inflight-A` is silently gone.

**The mechanism, three layers deep:**

  1. A's row lsn (2/8F3A6C8) is BELOW g3's cutoff_lsn (2/8F3E740) — it wrote
     early, committed late. The client anchors `state.lsn` to the chain cutoff and
     gates `ev.lsn < state.lsn -> drop`, so A's CDC event is discarded as
     "already in the snapshot" when it is precisely NOT in the snapshot.
  2. The DELTA filter shares the defect in a different coordinate: deltas select
     `version > prev cutoff_version`, and `now()` is TRANSACTION-START time — A's
     updated_at (10:44:18) predates every later cutoff, so A is excluded from all
     future deltas too.
  3. Only a future FULL (whole-table scan, sees committed A) rescues fresh
     clients — so with chain depth 6, every client seeding between g3 and the
     next full misses the row. Clients already live before g3 got A normally,
     which makes the divergence maddeningly inconsistent across replicas.

Measured on the wire, the general shape (delivery is COMMIT order, lsn is WRITE
position):

    trigger-B    lsn=8740121656  stream_seq=62128
    inflight-A   lsn=8740120392  stream_seq=62129   <- after B, LOWER lsn

**The answer to the stamp question: we do not need to mint one.** JetStream's
per-stream sequence IS a commit-ordered monotonic id — the bridge publishes in
decode order, decode is commit order (§10f), and the client already persists
`last_seq` per stream. What is missing is not the stamp but its USE at the
cutoff:

  * the producer records, per manifest, the CDC stream's last_seq AT BUILD TIME
    (`cutoff_seq`), alongside the existing cutoff_lsn/version;
  * the client anchors its gate on `msg.seq <= cutoff_seq -> drop` instead of
    the lsn comparison.

Correctness: a transaction visible to the snapshot AND published before capture
has seq <= cutoff_seq (dropped, correct); one invisible to the snapshot commits
later, publishes later, seq > cutoff_seq (applied, correct — this is exactly
inflight-A, 62129); one visible but not yet published at capture has seq >
cutoff_seq (applied AGAIN over its snapshot copy — a duplicate, absorbed by the
idempotent LWW upsert). The failure mode collapses to the duplicate direction,
which the pipeline already absorbs everywhere. The delta filter's exclusion stops
mattering because CDC replay above cutoff_seq carries what deltas miss.

This is PowerSync's op_id lesson landing in our architecture with zero new
machinery: query-based deltas with timestamp cutoffs are inherently racy against
in-flight transactions; stream-derived positions are not. (The user's earlier
"2 x REFRESH_SNAP" instinct was the same point — retention and cutoffs should be
measured in the stream's own coordinates.)

**BUILT and verified same day.** The producer captures the CDC stream's
`last_seq` beside the lsn — BEFORE the snapshot begins, keeping the
overlap-never-gap direction — and ships `cutoff_seq` + `cdc_stream` in the
manifest (omitted if the stream-info call fails, so old clients and degraded
ticks fall back to the lsn gate). The client anchors `state.seedSeq`/`seedStream`
at chain seed and gates `ev.seq <= seedSeq` on the matching stream; optimistic
locals and other streams pass untouched. Re-ran the exact experiment: a
transaction held open across a build (`inflight-A2`), new generation built while
it was open, fresh replica seeded from that chain — **the row ARRIVED**. Under
the lsn gate the same shape was silently lost.

⚠️ Residuals:
  * the chain path should verify CDC still covers its cutoff_seq (the snapshot
    path had `descriptorStillFresh` for exactly this) — a chain whose cutoff_seq
    has aged out of CDC retention is an orphan;
  * `seedSeq` is anchored in memory only — a durable replica that resumes from
    its stored per-stream seq never replays old events, so persistence is not
    needed for correctness today, but re-seeding paths should re-anchor;
  * observed during verification, PRE-EXISTING and documented: a tombstone-less
    table (memo) resurrected a hard-DELETEd row (`trigger-B`) because old chain
    deltas still carry it and the delete event sits below any cutoff — the
    documented weaker guarantee of physical deletes, amplified by chain windows.
    The answer remains "give the table a tombstone column"; no gate can fix it.

## 10j. The cross-STREAM FK test — and the two client defects it exposed (2026-08-27)

§10h's FK verification used `users`+`orders` — both PUBLIC, one stream. The
cross-STREAM shape (tenant-scoped child, public parent: the `salaries` case the
review insisted on keeping) had never actually been exercised. Built live:

    CREATE TABLE salaries (uid uuid PK, user_id bigint REFERENCES users(id),
                           tenant_id text NOT NULL, ...);
    SELECT zebridge_enable('public.salaries', writable => true,
                           tenant_col => 'tenant_id', dry_run => false);

**The approval doors verified themselves on the way:**
  * First attempt used `tenant_col` WITHOUT `writable` — zebridge_enable REFUSED
    it: "would be published unscoped, which zebridge_publication_guard refuses",
    with the three legal options in the error. The migration door works.
  * After the correct enable, the RUNNING bridge still published the schema from
    its boot-time catalogue view (tenant_column None, no FK) — the T3 restart
    law, stated by zebridge_enable's own output. After one restart the schema
    carried tenant_column, writable, AND the ported FK; the client's local DDL
    had `FOREIGN KEY (user_id) REFERENCES users(id)`.

**What worked:** fresh cross-stream seed (750 users via CDC_PUBLIC chain, 750
kilo salaries via gen-kilo chain, zero orphans, FK live); the over-ring 3000-pair
single transaction split across the ring, bridge alive, and the applier's
hold/retry converged 3000 held -> 3000 resolved, zero orphans, across TWO
independent consumers. The core cross-stream FK machinery works.

**What it exposed — two OPEN client defects, found because the durable client
was reconnected repeatedly:**

  1. **CDC_kilo's position is never persisted when every event on it was HELD.**
     `_zebridge_stream_seq` held only CDC_PUBLIC after runs whose kilo traffic
     was 100% FK-held. A stream whose seq is never recorded reads as a GAP on the
     next connect, which triggers a full RE-SEED on every reconnect — turning the
     durable client into a permanently fresh one for that stream.
  2. **A chain-full re-seed DESTROYS rows the chain lacks.** applyPlan's full
     step is `DELETE FROM <table>` + insert the full's rows; the settle pass
     re-seeded salaries from a chain reporting 3000 rows while PG held 4500, and
     the 750 CDC-arrived rows were wiped (replica 3750, then 3000). The replica
     ended MISSING the 750 cx users as well. Same DELETE-then-replay destruction
     class as the retired snapshot path (§10g) — the chain path inherited it.
     The chain-full row count itself (3000 of 4500 present at build time) also
     needs explaining — full-scan vs bookkeeping mismatch, or my reading of an
     interleaved log; NOT yet diagnosed.

RESOLVED 2026-08-27: defect 1 is D1, fixed in §10m; defect 2's data losses
(vanished cx users, 3750-of-4500) were finding 9 (§10m), not the chain path —
the chain-full DELETE destruction itself remains real and is D2's scope.

Both defects need a CLEAN-ROOM diagnosis (this environment has absorbed a day of
purges and slot advances) before fixes: per §10h's own lesson, establish what
actually ran before theorising. The under-ring run's "750 held, 0 resolved at
close" is likely defect 1's shadow (client closed before the public-stream
parents were consumed), not a hold/retry failure.

**Status of the schema portage after this test**: types, PK, NOT NULL, indexes,
UNIQUE (via unique indexes), FOREIGN KEY — all ported and verified, including
cross-stream. Remaining: DEFAULT and CHECK (both low value; a replica receives
complete rows). The PG -> SQLite portage is functionally complete for
replication correctness.

## 10k. Policy decisions and the reconnect-safety plan (2026-08-27)

Decided in review, recorded here so a session break loses nothing:

  1. **Restart policy** — promoted to README ("Restart rules"). The compressed
     law: the catalogue governs it -> migration + restart; data governs it ->
     live. Roadmap: extend `catalog_epoch` refresh (which the mutation listener
     already uses) to the CDC routing map, so mid-flight enables stop needing a
     restart at all.

  2. **Tombstone gate — BUILT.** `zebridge_enable(writable => true)` now REFUSES
     a table without `tombstone_col`: with snapshots retired, chains are the only
     seed path and a physical DELETE is inexpressible in an upsert-only delta, so
     hard-deleted rows resurrect on fresh seeds (measured, §10i). Escape hatch
     `allow_physical_deletes => true` passes with a WARNING row — mechanical
     refusal + recorded intent, same pattern as the timestamp and publication
     guards. Old 11-arg signature dropped; livebirth's tombstone-less fixture now
     opts out explicitly. Residual: psql deletes on READ-ONLY tombstone-less
     tables share the hole; candidate mitigation is a producer-forced FULL
     rebuild on seeing a DELETE event for such a table (caps resurrection at one
     cadence). Not built.

  3. **Cross-tenant FK is NOT forbiddable** (public parent, tenant child is the
     product's own shape) and mobile makes RECONNECTION THE COMMON PATH — which
     promotes §10j's two defects to core correctness work, in this order:
       D1. Persist the stream position when an event is HELD: the inbox row
           already carries the event durably (with its seq and stream), so
           holding IS accounting for it — advance `_zebridge_stream_seq` at hold
           time. Kills the perpetual-gap -> re-seed-on-every-reconnect loop that
           a 100%-held stream currently causes.
       D2. Make re-seeding SCOPED and NON-DESTRUCTIVE: a gap on CDC_kilo must
           re-seed only kilo-routed tables, and no chain-full may DELETE FROM a
           table holding newer CDC-applied data — the per-table data watermark
           (§10g residual) becomes mandatory. Clean-room the two §10j anomalies
           first (the chain-full row-count mismatch and the vanished cx users)
           per §10h's lesson: establish what ran before theorising.
     After D1+D2: strip the legacy snapshot dependencies from the scenario suite
     (inventory in the 2026-08-27 discussion: snapshot.py, stampede.py, wide.py,
     decode_integrity.py, faults.py, plus lighter references in check.py,
     crosstenant.py, envcheck.py, leaksoak.py, objstore_race.py, tls.py,
     tenant_kv.py, endpoint.py) so the bridge-side snapshot code can come out.

## 10l. Finding 8: the newborn width-guard budget, and the workflow consolidated (2026-08-27)

**Finding 8 (fixed, 4d6a030).** Every NEWLY enabled table's width guard baked the
16384 default and ignored all registered instance budgets until the next bridge
restart. Mechanism: `zebridge_enable` installs the guard BEFORE the publication
step (the publication guard's own required ordering), but the baked budget
derives from a join through `pg_publication_tables` — empty for the newborn at
install time — so `COALESCE(..., 16384)` always won. Measured: livebirth's
4096-budget probe STORED a 4096-byte row the guard exists to refuse (case C,
reopened for newborns only). Found by re-running livebirth to verify the
tombstone gate — the fix is a post-publication re-bake inside `zebridge_enable`
(body-only rebudget, no table lock), reported as its own migration step.

**The width-guard workflow, in one place** (the state after §10b, findings 7-8):

  1. the bridge reads env `BASE_BUF` (env ONLY — there is no `--buf` CLI flag;
     the CLI carries --slot/--pub/--port/--top and everything else is env.
     OPEN: whether per-instance sizing deserves a CLI option);
  2. at boot it calls `zebridge_register_limits(slot, publication, 2^BASE_BUF)`
     over the READER connection (SECURITY DEFINER; EXECUTE-only grant) — one row
     per instance in `zebridge_limits` (slot PK), dead-slot GC, and a re-bake of
     every guard its publication carries;
  3. `zebridge_install_width_guard` (at enable) and the boot re-bake both derive
     the budget as MIN over instances whose publication carries the table,
     via the schema-qualified `format('%I.%I', ...)::regclass = tbl` join,
     defaulting 16384 — and `zebridge_enable` re-bakes AFTER the publication step
     (finding 8). The enable re-bake originally compared `pt.tablename = short`
     (name-only, over-matches across schemas) — unified to the regclass form the
     day it was found, in review;
  4. the guard BODY reads nothing at write time: the budget is a baked literal
     (§10b's measurement: literal free, lookup +4.81us/row, join +22us/row).

## 10m. Finding 9: every reconnect dropped every table — and D1 closed (2026-08-27)

The §10k clean room (5 users + 5 kilo salaries, full logs, tiny bridge config)
ran before theorising, per §10h's lesson. It closed D1 and then convicted a
single defect for BOTH §10j anomalies.

**D1 — fixed.** The position persist lived only on the applied-event path;
gate-dropped and held events returned before it. Fix: persist per BATCH, after
the acks — delivery + accounting IS the position (an applied event is in the
tables, a gated one is provably in the seeded chain, a held one is durably in
the inbox). Plus: at consumer setup, a stream whose stored position is 0 gets
the stream's current tail persisted (quiet-stream case; never moves a real
stored position). Verified: the gap->re-seed loop is dead (reconnects report
0 gap lines, 0 re-seeds, both streams in `_zebridge_stream_seq`).

**Finding 9 (the §10j destroyer).** With D1 fixed, reconnects STILL lost data:
one run ended `users=5 salaries=0`, the next `users=0 salaries=0` with the log
explicitly saying "No CDC gap — no seeding needed". The full logs showed why:

    [MIGRATE] users: created (first sight)     <- on EVERY connect
    [ERROR] Applying schema for users failed: FOREIGN KEY constraint failed

`applySchema` decided "first sight" from `syncedTables` — an IN-MEMORY map,
empty in every fresh process — so every reconnect took the first-sight path:
`DROP TABLE` + `CREATE`, wiping the data, while the durable bookkeeping
(watermarks, stream positions) survived and testified everything was fine. The
run that kept its users kept them by ACCIDENT: the salaries FK blocked the
DROP (that's the [ERROR] line). When seeding was skipped — correctly, no gap —
nothing refilled what the drop had just emptied.

This one defect explains all of §10j's unexplained data: the vanished cx users,
the 3750-of-4500 chain-full mystery (the count was taken from a replica that a
reconnect had partially wiped), and §10g's "stale snapshot destruction" had the
same silhouette. The durable client was never durable for DATA — only for its
bookkeeping, which made every symptom look like a seeding bug.

**Fix (two parts, both in libzb.ts):**
  1. Existence comes from the DATABASE: when `syncedTables` misses, read
     `PRAGMA table_info` and reconstruct the existing-table state (columns +
     pk order) from the physical schema. "First sight" now means the table is
     genuinely absent.
  2. `rebuildPreservingData` (the legitimate drop/recreate path) wraps itself
     in `PRAGMA foreign_keys OFF/ON`: with FK ON, SQLite refuses the DROP of a
     referenced parent outright (measured — the [ERROR] line above). The data
     is copied, not changed, so the surgery is FK-safe by construction.

Verified together: fresh seed then two reconnects -> `users=8 salaries=8` all
three runs, `created (first sight)` 9 tables on run 1 and ZERO on reconnects,
0 gaps, and PG agrees (8/8).

**Lesson** (same family as §10h): the in-memory mirror of durable state is a
cache, never the truth. Any decision that destroys data must be grounded in
what is physically in the database.

**Still open — D2, now sharply scoped**: the destruction that remains is real
but narrow. A gap on one stream still re-seeds globally, and a chain-full apply
is still `DELETE FROM` + insert, so a chain older than the replica's CDC-applied
data still destroys the newer rows. Next: per-stream scoping + the per-table
data watermark before any chain-full DELETE (§10g residual, §10k D2).

## 10n. D2: re-seeding is scoped and non-destructive (2026-08-27)

The two halves of §10k's D2, both in libzb.ts, both RED-then-GREEN verified.

**Scoped seeding.** Gap detection already worked per stream; seeding now uses
it. `tablesToSeed` = tables ROUTED to a gapped stream (route = the table's
effective tenant's CDC stream) plus tables with NO generations watermark (never
seeded: a brand-new replica, or a table enabled between two connects — the
latter previously never seeded at all without a coincidental gap). Everything
else resumes from its stored position untouched. Measured: a purge-induced gap
on CDC_kilo alone produced `Seeding 4 table(s) [note_t, counter_tenant,
test_types, salaries]; 5 resume untouched` — zero `Seeded users`, yet users
grew by the row written during the outage, delivered LIVE on the ungapped
CDC_PUBLIC. A mobile client reconnecting with one stale stream no longer
rebuilds its whole replica.

**The chain-full guard (the §10g/§10j destruction, closed).** A chain-full is
`DELETE FROM` + replay, so a chain whose `cutoff_seq` is below the replica's
stored position for that stream would destroy every row applied AFTER the chain
was built — rows the resumed CDC will never re-deliver. Such a chain cannot
close a gap either (the gap sits ABOVE the position it fails to reach).
`applyGenerations` now refuses it before touching the table and returns false;
the existing wait loop polls for the next cadence build, whose cutoff is taken
at the stream tail. Delta-only plans are upserts and need no gate; legacy
manifests without `cutoff_seq` stay ungated (lsn is not comparable — finding 7).
Measured RED: position inflated +100000, salaries watermark dropped (forcing a
full plan) — every poll logged `chain g6 predates this replica (cutoff seq 862
< applied 100861 on CDC_kilo)`, the client gave up on salaries, and the 11 rows
SURVIVED. GREEN: position repaired — `Seeded salaries from generation chain g6
(11 row(s))`, replica == PG.

**Residuals, stated so they are not lost:**
  * A chain can be newer than the stored position yet still short of the
    stream's `first_seq` after aggressive pruning — events between its cutoff
    and `first_seq` are lost with no warning. The chain-orphan check (§10k
    discussion: verify CDC covers the manifest's cutoff_seq at seed time) is
    designed, not built. Retention >= cadence makes it structurally rare.
  * The retired snapshot path's replay still begins with an ungated
    `DELETE FROM` — acceptable only because the path is behind
    `LEGACY_SNAPSHOT_SEEDING = false`; delete it rather than gate it.

With D1 (§10m) + D2, §10k's plan is down to its last item: strip the legacy
snapshot dependencies from the scenario suite, then remove the bridge-side
snapshot code.

## 10o. The scenario suite no longer depends on snapshot-on-demand (2026-08-27)

The §10k plan's last precondition, done. DELETED (their purpose WAS the retired
path): snapshot.py (request/replay/verify), stampede.py (request hammering),
wide.py (width discipline via the snapshot working set — the width guard itself
is covered by widthguard.py and livebirth.py), faults.py (snapshot-listener
fault regressions; the listener leaves with the path). EDITED: check.py (§4
init.snap.* grant-symmetry check retired in place), crosstenant.py (probe D),
tenant_kv.py (§4 audience/contents), leaksoak.py (the every-30th snapshot
request op), decode_integrity.py (now CDC-only; the COPY-binary decode half
leaves with pg_copy_binary.zig), envcheck.py (SNAP_RET_SECONDS out of the known
set). Comment-only mentions in tls.py, objstore_race.py, endpoint.py, speed.py,
generations.py, zb.py stay — they are history or generations terminology.

Nothing in scripts/scenarios references snapshot_request, init.snap.*, or the
$KV.snapshots bucket any more. The bridge-side snapshot code (listener, worker,
pg_copy_binary.zig, the INIT_* publish path, SNAP_RET_SECONDS and friends) is
now unreferenced by the suite and can come out; .env/.conf cleanup rides with
that removal.

## 10p. Snapshot-on-demand is DELETED, and finding 10 (2026-08-27)

The cleanup §10o cleared the way for. Removed in one pass, verified live:

**Bridge**: `snapshot_listener.zig`, `pg_copy_binary.zig`, `streaming_encoder.zig`
(used only by those two) deleted; the listener thread, its allocator, the
snapshot half of the max_payload budget log, and `Config.Snapshot` excised;
`reconcileCdcStreams` now creates only `CDC_<TENANT>` (the whole INIT family is
gone — with `init.schema` already retired, INIT carried nothing but snapshot
chunks). Boot log now reads `Streams: CDC, MUTATIONS`.

**Topology/grammar**: `streams.init`, `streams.requests`, `init_streams`, every
`snapshot_*` subject, `subjects.init_prefix`, and `kv.snapshots` are out of
grammar.json and topology.zig alike. ⚠️ Two regressions the cleanup itself
caused, both caught before commit: the line-filter swallowed the
`t.subject_cdc_prefix` parse assignment (undefined memory — unit test segfault,
then a live `integer overflow` panic in reconcile from a stale binary), and it
corrupted the parse test's embedded JSON. Lesson repeated: after a mechanical
line-filter, run BOTH `zig build` and `zig build test`, and reinstall the exe
before restarting anything.

**NATS**: REQUESTS stream, KV_snapshots, and every INIT_* stream deleted from
the live server; nats-init (compose + up.sh), both server confs (218 grant
lines), and jwt-bootstrap.sh no longer create or grant any of it.
`SNAP_RET_SECONDS` is gone from .env.admin.

**Client**: the gated legacy path is deleted, not gated — waitForDescriptor,
descriptorStillFresh, replaySnapshot, initStream, the request/retry loop, and
the LEGACY_SNAPSHOT_SEEDING switch itself. Phase name 'snapshot' kept (UI
compat); it means "seeded".

**Finding 10 (client, fixed here — found by the post-cleanup verification).**
The seed gate's lsn fallback compared `ev.lsn < state.lsn`, but `state.lsn` is
advanced by every SCHEMA event — and a bridge restart republishes schemas at
the WAL head. So the composition restart x reconnect dropped every event the
new bridge replayed (older lsn than the fresh schema): measured — two rows
consumed and position-accounted, absent from the tables, unrecoverable without
a re-seed since the position had moved past them. Fix: a dedicated
`state.seedLsn`, set ONLY by applyGenerations at the manifest cutoff; schema
lsns never gate data. RED->GREEN: the same composition (pair inserted offline,
bridge restarted, client reconnected) now applies the replayed events — the
salary FK-held and resolved when its parent arrived. The pre-fix casualties
were healed by dropping the two tables' watermarks: scoped seeding re-pulled
chains g7/g9 and the replica ended identical to PG.

Residual: `zb-client-ts` still tolerates manifests without `cutoff_seq` via the
seedLsn fallback; the producer always ships cutoff_seq, so that fallback can go
once no pre-cutoff_seq chain can exist. Docs (README/PROTOCOL/SECURITY) rewrite
follows in its own commit.

## 10q. LWW clock sensitivity, the PowerSync sequencer, and the HLC candidate (2026-08-27)

Discussion record, no code change yet.

PowerSync does not resolve conflicts: writes go client -> the developer's own
backend API (a CRUD upload queue) and the backend applies them however it
likes — in practice, arrival order, so last-arrival-wins. Downstream is an
op-log with server-assigned sequences and checkpoints; the client rebases its
optimistic pending edits on each checkpoint (server wins). DELETEs need no
tombstone column because they travel as explicit REMOVE ops in the log; a
client offline past bucket compaction re-syncs the bucket wholesale. The
schema demands nothing, but the write path is yours to build — the intrusion
we put in columns (version/tombstone/tiebreak), they put in a backend you
must design correctly per app.

Arrival time vs client time is a question about offline: arrival order
resolves "which edit was later" by CONNECTIVITY (a Monday offline edit synced
Wednesday loses to Tuesday's online edit); clock skew is seconds while
reconnection delay is days, so client-stamped time better approximates
intent for offline-first. Inside the skew window no clock is "right" — that
is the tiebreak column's job, deterministic rather than an arrival race.

Existing mitigation: `version_future_tolerance` (5s) caps fast-clock
dominance. The open hole is the SLOW clock losing its own edits.

**BUILT 2026-08-27 (§10s): core.hlcVersion + normalizeVersion, floor fed
from CDC events' version column and chain cutoff_version.** Measured live: a
row stamped 30s in the future arrived via CDC and the client's next version
landed one microsecond above it while its wall clock sat 22s below. The
original sketch:**
`version = max(wall_clock, newest_version_seen_via_CDC + 1us)`. A device that
has seen the current row can never stamp below it; no schema, protocol, or
bridge change. Bridge arrival time stays interesting only as an audit column,
never as the comparator.

## 10r. The full test campaign after the snapshot removal (2026-08-27)

Run on the native stack (JWT NATS, host PG), main bridge at BASE_BUF=12,
cadence 20s. Two battery rounds plus a Node client pass.

**PASS (24)**: render, dyntenant, generations, mutate, writable, tiebreak,
poison, invalidate, burst, offline, replies, guards, tzguard, sweeper, reaps,
connbudget, sizing, race, downtime, credentials-preflight half of keys,
endpoint, livebirth, legacybait, decode_integrity. The tricky-point coverage
held: WAL replay across kill -9 (downtime), the ingress throttle fix (race),
newborn width budgets (livebirth), legacy oversized rows (legacybait), CDC
decode integrity (now CDC-only), the generations SQL contract, the write path
end to end (mutate/tiebreak/poison/invalidate/replies/offline).

**Environmental, not code** — the harness lessons:
  * zb.py's psql defaults to `docker exec` — the native stack needs
    `ZB_PSQL="psql <url>"` exported, or every scenario fails on the OrbStack
    socket while looking like product breakage.
  * python-consumer/.venv lacked `nkeys`; with NATS_CREDS exported, nats-py
    takes its JWT path and imports it. Installed (uv pip).
  * sizing's test 1 failed only inside the battery: its bridge contended for
    the walsender the just-killed main bridge still held, and stalled before
    the payload check. Solo on a probe slot: all green, including
    EventBufferExceedsMaxPayload firing (direct repro confirmed the guard).
  * envcheck's port findings are the native-vs-compose declaration mismatch,
    pre-existing.

**Still red, one class**: objgrants (edits the password conf, not the JWT one),
credentials (password principals), clamp and keys' write half (no verdict, no
CDC echo for the probe principal) — the user/password-era scenarios that
never crossed the JWT migration, joining crosstenant/tenant_kv/widthguard on
the known rework list. Not snapshot-cleanup fallout: their failures are
Authorization Violation / missing verdicts, and keys' preflight half passes.

**Scenario fixes made**: generations.py's fixture now uses gen 900001+ and its
cleanup no longer DELETEs the live producer's real bookkeeping (it was
deleting `WHERE tbl='users'` wholesale); objgrants re-anchored on CDC_ACME
(INIT_ACME left with §10p); burst.py now documents that it deliberately
leaves its 2,000,000 rows and must never run in a shared battery.

**The burst pollution incident, and the two-sided cleanup lesson.** burst.py's
2M rows turned every fresh seed into a 2M-row chain apply (Node runs timed
out; that is how it was noticed). Recovery: stop bridge -> DELETE the rows ->
`pg_replication_slot_advance` past the delete WAL (streaming 2M deletes would
re-flood) -> purge CDC_PUBLIC -> remove the users chain objects + manifest.
⚠️ That last step was HALF a cleanup: the producer's memory is
`zebridge_generations` in PG ("the bridge never reads its own output back"),
so it kept building deltas on top of objects that no longer existed — a chain
whose manifest 404s on fetch. The client handled it correctly (loud fail, no
destruction — the §10n behaviour), and clearing the PG rows made the next
tick rebuild g1 from scratch. Cleanup of a chain must clear BOTH sides or
neither.

**Node client pass (zb-client-ts over better-sqlite3)**: fresh seed (9 tables,
users 13 rows from the fresh g1), reconnect with zero gaps, zero re-seeds and
zero `created (first sight)` (findings 9/10 stay dead), offline cross-stream
FK pair held then resolved on parent arrival — replica == PG (14/16).
**Browser pass (web-consumer over ws://8080, JWT enrollment)**: invite
`webby@kilo` redeemed end to end — GET /enroll minted the JWT (client-side
nkey pair, seed never crossed the wire), used_at stamped, zebridge_user_tenants
row + live $KV.tenants entry; the page connected, seeded all 9 tables with
counts identical to PG, rendered the three suspended fixtures as banners, and
went live: a psql INSERT appeared in the browser within seconds (users 14->15,
INS badge), and the UI's test_types INSERT round-tripped browser -> MUTATIONS
-> writer -> PG -> CDC echo, with the replica seeing exactly its own kilo row
of PG's five (tenant scoping live). ⚠️ Operational gotcha found on the way:
the mint arms only when BOTH `ZB_SIGNING_SEED` (the client-scoped signing key
seed, in scripts/native/nsc-store/.../keys/A/...) AND `ZB_ACCOUNT_PUB` (the
ZEBRIDGE account public key) are set — with only the seed it stays off
SILENTLY (no warn line; the warn only covers the missing-writer case).

## 10s. The carve: a sans-I/O core with conformance fixtures — and WASM dropped (2026-08-27)

**The decision, recorded.** Ordering for the client's future: (1) carve the
pure decision core out of zb-client-ts, (2) encode every measured finding as a
language-neutral fixture, (3) only then port to Zig — the port implements the
fixtures, not the living TS. Rationale: findings 9 and 10 were client-core
bugs found THIS WEEK; a second implementation of a moving target fixes every
bug twice forever, while a port against a fixture spec is caught by the shared
suite. **WASM is dropped from the roadmap**: its job was "run the core outside
a browser", and that is now covered twice — zb-client-ts runs as-is in every
JS runtime (Windows server included), and the native libzb (C ABI) will cover
every non-JS host via FFI. The only WASM left is sqlocal's SQLite inside the
browser, which is the storage adapter's private business.

**Carve increment 1 — DONE.** `zb-client-ts/src/core.ts`: pure, imports
nothing, calls nothing — `seedGateDrops` (findings 7+10 as one rule),
`planFromManifest` + `fullPredatesReplica` (the chain walk and D2's
destruction guard), `streamHasGap` + `scopeSeeding` (the per-stream gap rule,
never-seeded criterion, D2 scoping), `advancePosition` (D1's accounting),
`foreignKeyFailureKind` (the three measured SQLite messages), `pgTsToWire`,
`lsnToNumber`. libzb.ts now delegates at every one of those sites; the I/O
shells (NATS pump, storage adapter) stay where they were.

**The fixtures ARE the spec**: `zb-client-ts/fixtures/core-fixtures.json` —
36 cases, each measured finding by name (the in-flight transaction below the
lsn watermark, the schema-lsn event that must never gate data, the D2 RED
cutoff 862 vs stored 100861, the kilo-scoped re-seed, the firstSeq-1
boundary, the strict-< lsn edge). `src/core.test.ts` is the TS runner
(`pnpm test`, node:test over strip-types); a Zig core writes its own thin
runner over the SAME JSON and is correct exactly when it passes. (The suite
already earned its keep once: its first run caught a hand-mis-hexed lsn in a
fixture — the expected values must be derived, never typed from memory.)

**Increment 2a — the apply SQL builders, DONE (same day).** `planKeyChange`
(the changed-PK delete, with the measured both-keys-live rationale),
`planUpsert` (the CDC upsert), `planDelete` (null on a partial composite key —
deleting on it would match more rows than PG did), `chainUpsertSql` (the
version-guarded chain upsert) and `chainRowParams` (JSON + wire-timestamp
binding) — applyEvent and applyPlan now execute exactly what the core builds,
and 16 new fixtures pin the SQL byte-for-byte (52 total). One deliberate
behaviour ALIGNMENT rode along: a table whose every column is in the key now
gets `ON CONFLICT .. DO NOTHING` on the CDC path too — previously a plain
INSERT that would THROW on redelivery, violating §7.1's idempotency promise;
the chain path always had the DO NOTHING, and the two now match.

**Increment 2b — the schema migration planner, DONE (same day).** Everything
applySchema DECIDES is now pure and fixture-pinned: `columnDdl` (inline pk /
`required` NOT NULL), `fkClausesFor` (malformed FKs dropped, not guessed),
`createTableSteps`, `rebuildSteps` (finding 9's legitimate drop/recreate as an
exact step list — the shell keeps only the FK-pragma wrap and the error-driven
ALTER->rebuild fallback), `diffColumns` (rename-aware: a hinted rename is
neither added nor removed, an unhinted one degrades to add+remove — §1.2 as a
named fixture), `fkTextDiffers`, `viewSteps` (the plumbing-column exclusion),
and `indexSyncPlan`. 24 new fixtures (76 total). Verified LIVE through the DDL
trigger: `ALTER TABLE memo ADD COLUMN` then `DROP COLUMN` in PG — the replica
migrated in place both ways, 6 rows preserved throughout, zero gaps, zero
re-creates. The shell's applySchema is now orchestration only: fetch physical
state, execute steps, log, book-keep.

**The §10q HLC — BUILT (same day).** `normalizeVersion` (canonical 6-digit
micros — PG trims trailing zeros, and mixed widths break both string
comparison and the micro arithmetic), `maxVersion`, `hlcVersion`
(= nextVersion over max(own last, observed floor)). The floor is fed from
every arriving CDC row's version column (never our own optimistic stamps) and
from each chain's cutoff_version at seed time; the schema payload's
version_column now lands in TableState. A slow-clock device stamps one
microsecond above the newest version it has seen; arrival time never becomes
the comparator. 8 fixtures; measured live with a +30s future row against a
wall clock 22s behind it.

**Increment 2c — the mutate() envelope, DONE (same day).** `nextVersion`
(the monotonic stamp as a pure rule — the clock stays in the shell, and this
is exactly where the §10q HLC candidate will land), `subjectSafeToken` (the
msg_id rides as mutation_ack subject tokens — unescaped, one write's ack
would fan out as a wildcard), `mutationSubject`, `mutationMsgId` (the version
stays IN the id: a second edit is a different write, a retry is not),
`mutationKeyId`, `mutationPayload` (DELETE ships no data), `optimisticEvent`
(lsn pinned at MAX_SAFE_INTEGER, optimistic-flagged), and `buildMutation` —
the whole envelope in one call, the first thing a port implements. 7 new
fixtures (83 total). Verified live: a Node INSERT round-tripped
mutate -> MUTATIONS -> writer -> PG -> CDC echo (outbox drained to 0), and
the DELETE came back as the tombstoning UPDATE the soft-delete trigger makes
of it. **The transport seam — DONE (same day), and the carve is COMPLETE.**
`transport.ts` is the second wall next to storage.ts: a `Transport` interface
(connect / credsAuthenticator / headers / jetstream / jetstreamManager / kv /
objectStore / deliverPolicy) with the @nats-io wrap as `natsTransport`, a
structural `TransportConnection` (close/status/subscribe/rtt) replacing the
NatsConnection type, and the NATS wire constants (DELIVER_POLICY) spelled once
— they are protocol tokens, identical in nats.zig. libzb now contains ZERO
@nats-io imports; `config.connect` still overrides just the dial (the Node
TCP adapter), `config.transport` replaces the whole wire layer — which is also
what a serverless mock test injects. Verified live through the seam: full
read path (dial, KV, object store, consumers, schema watch — reconnect clean,
replica == PG) and full write path (headers, publish, verdict sub — zero
errors, outbox drained, row in PG).

**The port is unblocked**: a Zig libzb implements core.ts against the 91
fixtures, maps Transport to nats.zig and Storage to a SQLite driver, and the
shells are all that remains to write.

**Finding 11, small, found by 2c's live write**: better-sqlite3 refuses JS
booleans and `undefined` at bind time, so on Node the optimistic apply and
every boolean-carrying CDC echo errored locally while the same rows applied
fine in the browser (sqlocal coerces implicitly). Binding is SEMANTICS, so the
rule joined the storage contract (storage.ts): boolean -> 0/1, undefined ->
NULL, in every adapter; node.ts now coerces. Verified: the same write that
errored three times runs clean, the local replica shows the row and then its
tombstone.

## 11 Restart Rules

PROMOTED to README ("Restart rules", operator-facing) 2026-08-27 — README carries
the current-state copy; this section keeps the history.

The restart rules, as they stand today

|  Change  |  What's needed.  |
|----------| -----------------|
| New table (public or tenant-scoped)  | one zebridge_enable(...) migration + one bridge restart — boot reads the catalogue and, for public tables, reconciles CDC_PUBLIC's subject filter itself. No env edit, no nats stream edit. |
| Changed rule on an existing table (version/tombstone/tiebreak/tenant_col)  | catalogue row updates via zebridge_enable re-run + bridge restart (boot-level read; the sweeper likewise re-reads on its own restart) |
| New tenant | zero bridge restarts: backend creates CDC_<T>/INIT_<T>, inserts the zebridge_user_tenants row (propagates live to $KV.tenants) — plus the NATS grant block + SIGHUP (reload, not restart) until the JWT signing key |
| New user on an existing tenant    | conf grant + SIGHUP only  |
| DROP TABLE   | nothing for the bridge — but purge the four ghost sources (chain, descriptor, INIT chunks, CDC retention) or resh clients resurrect rows |
| Generations on/off, tenant set growth    | nothing — the producer reads the catalogue and the data per tick |
