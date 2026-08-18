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

Where the invariant holds today — 3 of 10:

| pattern | identity-first |
| --- | --- |
| `cdc.<tenant>.<table>.<op>` (when TENANT_RULES is set) | ✅ tenant |
| `mutation.<principal>.<table>.<operation>` | ✅ principal |
| `mutation_ack.<principal>.<msg_id>` | ✅ principal |
| `cdc.<table>.<op>` (unscoped) | ❌ no identity at all |
| `mutation_error.<table>` | ❌ keyed by table |
| `init.snap.<table>.<snapshot_id>.<chunk>` | ❌ keyed by table |
| `snapshot.request.<table>` | ❌ no identity |
| `init.schema` | ❌ no identity |
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
- **Anything not behind an identity token** — seven of the ten patterns above.

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
