# Working plan — snapshots via `COPY ... (FORMAT binary)`

Carries the session state a fresh session would otherwise lose. Section letters are
historical, not a running order — see **Start here** below.

---

## 🔜 Start here next session (written 2026-08-15)

**Environment as left.** The Postgres volume was recreated, then `Emitter.Scenario`
steps 2 → 3 → 4 → 5 were run, so `users` exists with `PRIMARY KEY (name, email)` and
**12 515 rows** (padded for the pagination test — the scenario itself leaves 15).
`test_types` exists and is empty; `users_no_pk` and `exotic_types` were never created
(steps 1, 7, 8 not run). Two hand-built fixtures are also present, both in `my_pub`,
both granted to `bridge_reader`:

- `public.ck` — 25 000 rows, `PRIMARY KEY (tenant, name)`, 3 000 names repeated per
  tenant so chunk boundaries land inside a run of equal `tenant`; every name contains
  a `'`.
- `public.sk` — 15 002 rows, `PRIMARY KEY (id)` spanning **-1 to 15000**, which is what
  pins the removed `"id" > 0` first-chunk sentinel.

`DROP TABLE public.ck, public.sk;` when they stop being useful, and
`DELETE FROM public.users WHERE name LIKE 'n-%';` to put `users` back to its
scenario size. Bring the stack up with
`NATS_BRIDGE_NKEY_SEED="SU..." docker compose -f docker-compose.full.yml --env-file .env.admin
up -d postgres-primary nats-config-gen nats-server nats-init bridge-init`, then run the
bridge from the host with `.env.bridge` (it needs DATABASE_READER_URL; there is no PG_* fallback
any more).

**Also landed 2026-08-15, after §E:** the oversized-row `@panic` is gone. A row that
does not fit the per-event buffer now suspends its table (`reason: "row_too_large"`)
instead of killing the process — panic was a poison pill, since the row precedes any
later ACK, so a supervised restart re-read it and panicked again with every other table
stopped. The bridge also now reads `max_payload` from the NATS server's INFO line and
warns at boot if `BASE_BUF` cannot fit inside it. README has the sizing formula.

**Read path: complete.** Snapshots (binary COPY, composite keys), CDC (type-guarded,
TOAST-correct), schemas, suspensions, gap detection and client re-seed have all run end
to end, and a fresh web-consumer bootstrapped from empty on 2026-08-15. What is left on
the read side is polish (§F), not mechanism.

**Write path: security scaffolding in place, no mutation has ever been applied.** The
ingress role, its separate connection, the poison-pill guard and the version-column
report all exist; the subject is still parsed from the payload, there is no reply, and
`mutation_listener` still expects the deleted `_hlc`/`_deleted` columns. See §D0.

**Work, in order:**

1. ~~**Delete the CSV path**~~ — ✅ **DONE 2026-08-15.** `src/pg_copy_csv.zig` (883 lines,
   13 tests) deleted, along with `getTablePrimaryKey`, `PkMetadata`, the dead import in
   `snapshot_listener.zig`, the test-discovery import in `bridge.zig`, and the stale
   comment in `streaming_encoder.zig`. Test count 222 → 196; nothing else moved. The
   gate, for the record: The rule was
   "delete with evidence, not on a green log line", and the evidence now exists: a fresh
   web-consumer bootstrapped from an empty SQLite, watched the schemas KV, detected the
   gap, requested snapshots, and replayed a **binary** snapshot into local tables —
   `📦 Published chunk 0 (250 rows, 45545 bytes)` on the bridge, `Replay finished for
   test_types` on the client, rows queryable afterwards. The empty-table case (`users`,
   `0 batches, 0 rows`) was handled correctly on both sides in the same run.

   What to delete: `src/pg_copy_csv.zig` (and its 13 tests), the unused import at
   `snapshot_listener.zig:17`, the test-discovery import at `bridge.zig:37`, the stale
   reference in `streaming_encoder.zig:31`, and — dead since pagination started resolving
   the key from `getTableColumns` — `getTablePrimaryKey` and `PkMetadata`.

2. **Client item D — DONE.** LSN/sequence persistence (`_zebridge_sync`), a JetStream
   consumer with `DeliverPolicy.StartSequence`, gap detection against
   `stream.state.first_seq`, truncate-then-apply snapshot replay. Verified in the same
   run. This was the gate on item 1 and on TEST_SCENARIOS **B**.
3. **The scenario migrations moved** from `emitter/priv/spec/` to
   `emitter/priv/repo/migrations/` (2026-08-15, untracked at the new location — commit
   them). `Emitter.Scenario` steps 1–8 run again from there.

4. ~~**Snapshot memory unbounded; chunks capped in rows, not bytes**~~ — ✅ **DONE
   2026-08-16.** The fetch is now bounded server-side, so residency no longer depends on
   the table.

   **Ask the widest row first.** One scan per snapshot, inside the REPEATABLE READ
   transaction: `SELECT max(<size expr>), avg(<size expr>) FROM t`. A row at or above the
   message budget **suspends the table** (`row_too_large`, the same verdict and wire word
   CDC reaches) before a byte moves — no COPY runs at all. Below it, `max < budget`
   guarantees every chunk contains at least one row, which is what removes the zero-row
   deadlock by construction rather than by a guard.

   The scan is affordable because `octet_length` on `text`/`bytea` reads the length out of
   the TOAST pointer. Measured on 200 rows of 256 KiB blobs: **2 shared buffers**, against
   **430** for an expression that must actually read the value.

   **Chunks are bounded by a running sum.** `COPY (SELECT <cols> FROM (SELECT <cols>,
   sum(<size expr>) OVER (ORDER BY <pk>) AS zb_running … LIMIT <n>) s WHERE zb_running <=
   <budget>)`. Explicit column list, never `SELECT *` — the subquery carries `zb_running`,
   and binary COPY matches columns positionally. The encode buffer is now sized *to* the
   budget, so encoder overflow and over-budget are one event.

   ⚠️ **The running sum broke `isFinal`, and that had to be fixed properly.** Once chunks
   are trimmed by bytes, a short result no longer means "the table ended" — observed live
   as `limit=7506 fetched=4670 encoded=4670`, which looked final and truncated a
   25 000-row table at 8 389. `isFinal` is now `mayBeFinal`, answering only the cheap half
   (a full chunk, or one the encoder trimmed, is certainly not last), and the caller
   confirms with one indexed lookahead past the cursor.

   Also fixed here: `NoSpaceLeft` is a shrink signal rather than a fatal error; rows are
   decoded **once** instead of per shrink attempt; and `PQgetCopyData == -1` is no longer
   read as success — a server error mid-COPY reports itself only from `PQgetResult`, and
   without that check a truncated COPY was indistinguishable from a short final chunk.

   **Verified:** narrow table (25 000 rows) 6 chunks, ordering identical to PostgreSQL's
   `ORDER BY`; wide table (2 000 × 256 KiB = 500 MB) 667 chunks with **RSS 436,896 →
   442,432 KB, +5.4 MB** — flat, independent of table size; a 2 MiB row rejected by the
   pre-scan with **zero chunks published** and a suspension on the schemas KV; and a COPY
   killed mid-snapshot leaving no orphans (below). 298 tests.

**Untested, noted 2026-08-16: encryption in transit.** Neither leg has ever been run
with TLS.

- **Postgres.** Should be the easy one now: `sslmode` rides in `DATABASE_READER_URL`'s query
  string (`?sslmode=verify-full&sslrootcert=…`), libpq does the work, and `connInfo`
  passes the URL through untouched. Worth an actual run against a TLS-enabled server
  before claiming it — in particular that `verify-full` still works once `connInfo`
  appends its keepalives, and on the **replication** connection, which appends
  `replication=database` as well.
- **NATS: a shipping constraint of v1.0, not a bug (settled 2026-08-16).** TLS was
  attempted against the vendored pure-Zig client and not achieved — adding it looks like
  substantial work in the client itself, where nkey auth was already a local patch. So
  the deployment removes the need instead: **v1.0 requires the bridge and nats-server on
  the same host, talking over loopback**, where there is no link to encrypt.

  State it as a requirement in the release notes rather than leaving it to be discovered.
  It is sound — loopback is not a network an attacker sits on — but it is a constraint an
  operator may refuse, and the answer to that is v1.1 below, not a redesign of v1.0.

### v1.0: migrate to nats-io/nats.zig (evaluated 2026-08-16, deferred)

**Not v0.14.** The current release ships on the vendored client, which works; this buys TLS
and therefore a remote NATS, which is a capability, not a fix. Kept costed here so picking
it up is a decision rather than a fresh investigation.

**Versioning, for the record:** the minor tracks the **PostgreSQL floor**. `v0.14` = PG 14+,
today's release. `v0.16` = PG 16+, the standby work below (logical decoding on a standby did
not exist before 16). `v1.0` = this — a NATS client with TLS, which is what lifts the
colocation constraint.

`https://github.com/nats-io/nats.zig` — official org, Apache-2.0, 218★, last push
2026-05-21, tag `v0.1.0`. **Pre-1.0 and says so** ("The API may change before 1.0"),
which is the one real risk. Everything the bridge depends on is there, checked against
the source rather than the README:

| what the bridge needs | nats.zig |
| --- | --- |
| Zig 0.16, `std.Io` | yes — "`std.Io`-idiomatic", requires `std.Io.Threaded` as host runtime, which is what `init.io` already is |
| **TLS** | server-authenticated: `tls_required`, `tls_ca_file`, `tls://` scheme, `tls_handshake_first`. mTLS is *not* implemented |
| nkey seed auth | `nkey_seed` / `nkey_seed_file` — native, so the local patch disappears |
| pull consumer tuning | `src/jetstream/types.zig`: `durable_name`, `deliver_policy`, `ack_policy`, `ack_wait`, `max_deliver`, `filter_subject` — every field this bridge sets |
| dedup on publish | `Nats-Msg-Id` headers, PubAck with `seq`/`stream` |
| KV | `createKeyValue`, `put`/`get`/`watch` — a real API instead of publishing to `$KV.<bucket>.<key>` by hand |
| `max_payload` | `Client.max_payload`, cached from server INFO — public, instead of reaching into `js.connection.?.server_max_payload` |

And it does not have the bug that panicked the snapshot listener. `src/jetstream/pull.zig`
consumes status frames inside the library:

```zig
if (msg.status()) |code| {
    msg.deinit();
    switch (code) { 404, 408, 409 => break, 100 => continue, else => break }
}
```

so a control frame can never reach caller code and be acked into a null `ReplyTo()`.

**Migration surface is smaller than it looks.** Five files import `nats`
(`nats_publisher`, `batch_publisher`, `schema_publisher`, `snapshot_listener`,
`mutation_listener`) and between them make ~15 distinct calls: `JS.CONNECT/PUBLISH/
DISCONNECT/PURGE`, `Consumer.START/STOP/CONSUME/ACK/NACK/REUSE/PUBLISH`, `Core.CONNECT/
SUB/PUB/DISCONNECT`. `build.zig` names the module `"nats"` via `addModule`, so switching
to `b.dependency("nats", …)` leaves every `@import("nats")` untouched. Against that:
**5 443 lines of vendored client leave the tree**, along with the Linux-syscall porting
notes in §A1 and the nkey patch.

The work is in semantics, not call count: blocking `CONSUME` loops become `io.async`/
futures, and `Endpoint.parseUrl` gains the `tls://` scheme it currently refuses. Do it as
its own branch with the scenario probes as the gate — `faults.py` and `stampede.py`
already pin the two behaviours most likely to regress.

### v0.16: reads from a **physical streaming standby** (scoped 2026-08-16)

> **Standby replica, never a logical replica.** The whole design rests on the standby
> replaying the primary's WAL byte for byte, so both hosts share one LSN address space and
> one timeline. A logical subscriber is a separate cluster with its own WAL and an
> unrelated LSN space; pointing this at one makes `ev.lsn <= state.lsn` compare two
> different number lines and produces duplicates or silent drops. "Replica" covers both
> in casual speech — only the physical streaming standby is meant anywhere in this section.

**Independent of the nats.zig item above** — different leg. Postgres reaches the bridge
over libpq, which has TLS regardless of what the NATS client can do, so this lands whether
or not that migration ever happens.

**Named for its floor, and usable without a standby.** `v0.16` raises the PostgreSQL
minimum to 16 because that is where logical decoding on a standby begins. The *release* is
still usable against a lone primary, or against logical replicas — those deployments simply
point `DATABASE_READER_URL` somewhere else and leave the new switch off. What actually changes per
deployment is two things: a genuinely different reader URL, and a flag along the lines of
`--conc true`.

### …and it is what unblocks concurrent snapshots

Worth stating plainly, because it is a stronger argument for the standby than read
offloading alone.

Snapshots are served **one at a time** (thread 7 calls `generateIncrementalSnapshot`
inline). That is not an oversight — it is the only thing keeping N concurrent `COPY`s off
the primary, which is the same load the `REQUESTS` stampede window exists to prevent. So
concurrency and primary-protection are in direct conflict *as long as snapshots read the
primary*.

Move the reads to a standby and the conflict dissolves: protecting the primary was the
entire reason to serialise, so a standby makes concurrency nearly free. Each snapshot
already opens its **own** PG and NATS connections (`snapshot_listener.zig:1358`, `:1385`,
both torn down per request), so workers would not contend on anything shared — the pieces
that scale with N are the encode buffer (~1 MB each) and the refusal registry (already safe
for multiple writers).

That also fixes the queue-expiry hole below, from the other end: a shorter queue is a queue
whose requests do not age out.

**So the switch is one mechanism with two effects** — `--conc true` means "snapshots may run
in parallel", and it is only safe to turn on when the reads are pointed at a standby. Keep
them as one decision rather than two knobs that can be set inconsistently.

### The queue-expiry hole this exposes (v0.14, live today)

Requests wait **in the `REQUESTS` stream**, not in the bridge — the consumer is a *pull*
consumer (`$JS.API.CONSUMER.MSG.NEXT`, `batch = 1`, `no_wait = true`, no `deliver_subject`),
and during a snapshot it does not poll at all, because the loop is blocked inside the COPY.

So a queued request ages against the stream's `max_age` with nothing attending to it. Once
its wait exceeds that window — the wait being the **sum of the work ahead of it**, not one
long snapshot — the broker drops it unread. The client gets no chunks, no error, and nothing
to retry against until the window clears.

Mitigated 2026-08-16 rather than fixed: every snapshot logs its duration, the bridge reads
the stream's real `max_age` from the server (`SNAP_RET_SECONDS` is nats-init's, so asking is
the only way to know it), and it warns when a snapshot exceeds half the window. The
invariant nobody had written down:

```
SNAP_RET  >  worst-case snapshot  ×  tables that may request at once
```

Finding it required fixing the vendored `StreamConfig`: `max_age`, `max_msgs`, `max_bytes`
and `max_msgs_per_subject` were `i32`, and a nanosecond duration in an `i32` caps at **2.1
seconds** — so reading any real stream's retention was impossible.

Shape: **ingress → primary, reads → standby.** It moves the two heavy reads off the
primary — the long `COPY` of a snapshot, and logical decoding itself, which runs on the
walsender of whichever host owns the slot.

**One standby or two?** One is the design. Two is worth knowing about, because the two
reads want *opposite* settings from the same host:

| | snapshot reads | CDC reads |
| --- | --- | --- |
| needs a replication slot | **no** — just a consistent read + an LSN | yes |
| minimum PostgreSQL | any (14+, as today) | **16** — logical decoding on standby |
| slot invalidation on recovery conflict | not applicable | permanent, forces a full re-seed |
| wants `max_standby_streaming_delay` | **large** — a long `COPY` must survive | small — replay should keep up |
| effect of lagging | eats CDC retention (see invariant below) | is the lag |

The conflict is concrete: a large `max_standby_streaming_delay` makes the standby *pause
replay* while a snapshot runs, so on a single shared standby **a long snapshot stalls CDC**
from that same host. `hot_standby_feedback=on` avoids the conflict at the source instead of
delaying replay, which is what lets one standby serve both — at the cost of bloat and xid
retention on the primary.

So: one standby with `hot_standby_feedback=on`, or two standbys where the snapshot one is a
plain read replica with no slot, no PG16 requirement, and permission to lag freely.

**Config: nothing new.** `DATABASE_READER_URL` = standby, `DATABASE_WRITER_URL` = primary. That
is the whole configuration change, and it is the payoff of the 2026-08-16 URL split —
before it, the read path was assembled from `PG_HOST`/`PG_USER` with no seam to point at
a second host.

**The read path is already read-only**, verified rather than assumed:

- the publication is *verified, never created* — no `CREATE PUBLICATION` on a read-only host
- slot creation uses `pg_create_logical_replication_slot()`, permitted on a standby from
  PG16, and already degrades to "ask your DBA" when refused
- `"DROP TABLE"` in `event_processor.zig:672` is a DDL **command-tag comparison** read out
  of `zebridge_ddl_events`, not SQL the bridge executes
- `dropSlot` exists but is never called

**Code: five `pg_current_wal_lsn()` calls**, each of which errors outright with
`recovery is in progress` on a standby:

| site | purpose | if unfixed |
| --- | --- | --- |
| `snapshot_listener.zig:1167` | snapshot watermark | **load-bearing** — the LSN `web-consumer/src/App.tsx:350` compares every CDC event against |
| `event_processor.zig:959` | one watermark for the boot-schema pass | has a "stamp 0" fallback, so it degrades *quietly* |
| `wal_monitor.zig:120` | current LSN | monitoring |
| `wal_monitor.zig:171,174` | `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` | monitoring |

One helper fixes all five: resolve `pg_is_in_recovery()` once per connection, then use
`pg_last_wal_replay_lsn()` or `pg_current_wal_lsn()`. ~20 lines plus threading.

**LSNs are a property of the WAL stream, not of the machine.** A physical standby replays
the primary's WAL byte for byte and shares its LSN address space and timeline, so a
watermark taken as the standby's replay LSN is directly comparable with CDC LSNs from the
primary — it is simply an earlier point on the same line. This is what keeps
`if (ev.lsn <= state.lsn)` correct across two hosts.

**The retention invariant grows a term, and violating it fails silently.** A snapshot from
a standby lagging by *L* carries a watermark *L* older than the primary's position, and the
client then needs every CDC event with `lsn > watermark` still present in the stream. So
TEST_SCENARIOS' `CDC_RET > SNAP_RET + apply time` becomes:

```
CDC_RET > SNAP_RET + apply time + replica lag
```

Break it and nothing raises: the client applies what is in the stream, the `first_seq` gap
check passes, and the table quietly ends up missing a window of changes. There is no way
for a client to tell "event expired" from "event never existed".

**Which makes one new metric load-bearing, not optional.** On a standby,
`pg_wal_lsn_diff(replay_lsn, restart_lsn)` measures how far the *slot* trails the
*standby's own replay* — it says nothing about how far the standby trails the primary. So
today's `lag_bytes` would stay green while the standby falls arbitrarily far behind and the
invariant above breaks. Add standby-vs-primary lag before, not after.

**Operational requirements, and the risk that is actually accepted:**

- **PG16+** — logical decoding on a standby did not exist before it. Raises the product's
  floor from PG14 (where `pgoutput` binary starts) for anyone using this topology.
- `max_standby_streaming_delay` — **30s by default**, measured on the current server. A
  snapshot `COPY` running longer than that is cancelled by recovery conflict
  (`canceling statement due to conflict with recovery`); the listener publishes
  `generation_failed` and the client retries into the same wall. Needs a much larger value
  on the snapshot standby, or `hot_standby_feedback=on`.
- `hot_standby_feedback=on` pushes bloat back onto the primary, undoing part of what the
  split moved.
- **Slot invalidation on recovery conflict is permanent**, and forces a full re-seed for
  every client. This is the real cost of CDC-from-standby, and the reason snapshots-only is
  the safer first step.

**If the motivation is surviving loss of the primary rather than offloading it**, PG17's
`sync_replication_slots` + `synchronized_standby_slots` are the better tool — both present
on the current server (18.4). The slot stays on the primary and is synced to standbys: no
standby decoding, no invalidation-on-conflict.

**Staging — snapshots first, and it is the cheaper half by a long way.** An earlier draft
framed this as the *more* awkward option because it needs a third URL
(`DATABASE_SNAPSHOT_URL`, defaulting to `DATABASE_READER_URL`) where the full split needs none.
That weighed the wrong thing. A snapshot standby:

- needs **no replication slot**, so the invalidation risk does not exist
- needs **no PG16** — logical decoding on standby is a CDC requirement, not a read one
- may lag freely, bounded only by CDC retention
- removes the single heaviest read from the primary

against one extra env var and the `pg_is_in_recovery()` LSN branch, which is needed either
way. CDC-from-standby is the bigger commitment: a slot on a host whose slots can be
invalidated by recovery conflict, and a PG floor of 16.

Do snapshots first. Decide CDC separately, with the standby-lag metric already in place.

### Two flexibility claims, verified 2026-08-16

**1. "A topology rename is a restart." — It is now. Fixed 2026-08-16.**

grammar.json is read at **startup** (`src/topology.zig`), not baked in by `build.zig`.
`TOPOLOGY_PATH` overrides the location; a missing file or key stops the bridge before any
thread exists, naming the key (`MissingKey at "subjects"."snapshot_meta_pattern"`), so the
guarantee the compile-time version gave moved rather than disappeared. Verified: renaming
`streams.cdc` and `subjects.cdc_prefix` and restarting — **no rebuild** — produced
`Streams: CDC_RENAMED …` and `CDC subject pattern: changes.<table>.<operation>`.

The one new piece is `topology.render`: `std.fmt.allocPrint` needs a comptime format
string, so the `{[table]s}` patterns needed a runtime substituter. It checks both
directions — an unknown placeholder *and* an unused argument are errors — because a wrong
subject is silent: the message lands under a name nobody subscribes to, or under none at
all if no stream captures it.

*What follows is the finding that prompted the change.*

**1a. Why it had to move — No: it was a rebuild.** `build.zig:62`
`@embedFile("grammar.json")` feeds `addOptions`, so every stream, subject and bucket name
is *baked into the binary*. Tested: renaming `streams.cdc` and running `zig build` with no
`.zig` file touched does pick the change up (the build system tracks the embed correctly),
but the **already-built binary keeps the old name** — it logged
`Streams: CDC_RENAMED …` while grammar.json on disk said `CDC`. The log line said
"(from grammar.json)", which implied runtime reading; it now says "compiled in … rebuild
to change".

The asymmetry is the friction point: `nats-init` reads grammar.json at *container run
time* with `jq` (docker-compose.full.yml:161-164), so `docker compose up` applies a rename
to the server immediately, while the bridge needs a rebuild and redeploy. A rename is
therefore three coordinated moves — rebuild the bridge, re-run nats-init, update clients —
not one restart.

This is the price of the comptime design, and it was bought deliberately: a missing key
fails the *build*, which is what stopped the bridge and nats-init drifting apart. Making it
runtime would trade that guarantee for a startup error and would cost the comptime string
concatenation (`mutations_subject_wildcard = prefix ++ ".>"`). Worth revisiting only if
renames become frequent, which they should not.

**2. "A DBA adds a table; the bridge has no constraint on it." — True, and now true at
runtime too (fixed 2026-08-16 and 2026-08-17).**

**Hit in a live run**, which is how the last piece was found: the bridge was started, *then*
a migration created `users`/`test_types` and added them to the publication. The DDL path
published their schemas, so a client built local tables — and every snapshot request came
back `table_not_monitored`, because `monitored_tables` was the boot-time list. The client
had a table it could never seed, and nothing said "restart the bridge".

The boot list is now a **cache, not truth**: when it says a table is unknown, the snapshot
listener asks `pg_publication_tables` before refusing (`isTableInPublication`). One query,
only on the path that would otherwise reject the request. Verified by reproducing the
sequence exactly — bridge up with 2 tables, `ALTER PUBLICATION … ADD TABLE users` while
running, snapshot served without a restart.

`preflight.reportTable` runs from the DDL path, so a table created while the bridge is
running gets the same ✍️/⚠️ lines it would have got at boot. Verified live: `CREATE TABLE
zb_naive (id bigint PRIMARY KEY, updated_at timestamp)` against a running bridge produced
`✍️ 'zb_naive': edge-writable on 'updated_at'` followed by the naive-timestamp and
nullable warnings. One extra catalog query per `CREATE TABLE`, on a short-lived
connection, and every failure is swallowed with a debug line — advice must not cost a DDL
event.

The client-facing half of that warning is now `PROTOCOL.md` §7.2, "The column's *type*
decides whether last-write-wins is sound": the operator sees the log, the client author
needs the consequence.

What follows still stands. No table name is hardcoded anywhere. However
`monitored_tables` is read **once at boot** from `pg_publication_tables`
(`bridge.zig:464`), and it is consulted at runtime by the snapshot listener
(`snapshot_listener.zig:1050`, `isTableMonitored`).

So after `ALTER PUBLICATION … ADD TABLE` against a running bridge:

- **CDC flows** — pgoutput sends the new relation, and the decoder works from
  `RelationMessage.relation_id`, not from a name list;
- **snapshots are refused** — `table_not_monitored`, until restart;
- **no boot schema was published** for it (`publishBootSchemas` takes `monitored_tables`),
  so unless a `CREATE TABLE` DDL event fires — which `ALTER PUBLICATION` on an *existing*
  table does not — clients receive change events for a table they have no schema for and
  cannot seed.

That mixed state is the real gap, and it is worse than a clean "not supported": the bridge
looks like it is working. Restarting the instance resolves it. Either make the refusal
symmetric (drop CDC for tables absent from `monitored_tables`) or re-read the publication
when a DDL event arrives — the second is the better behaviour and the more work.

Prerequisites the DBA owns either way: the table needs a primary key (otherwise preflight
refuses it — `refused_tables`), `bridge_reader` needs `SELECT` for the snapshot `COPY`, and
edge writes need `SELECT zebridge_grant_edge_writes('public.<table>')`.

**3. The slot is per-instance and architecture-neutral — correct.** It is that instance's
pointer into the WAL, nothing more. Two consequences worth stating: a slot whose bridge has
stopped **pins WAL forever** and will fill the primary's disk (`max_slot_wal_keep_size` is
the guard, set to 10GB in compose), and `max_replication_slots` caps how many instances can
exist at once.

### Client retry — DONE 2026-08-16

`PROTOCOL.md` §6 said a client MUST bound its wait and re-request; `web-consumer` did
`await watchP` with no timeout, so the contract and the reference implementation
contradicted each other. Both halves are now fixed in `web-consumer/src/App.tsx`:

- **`js.publish`, not `nc.publish`.** A core publish is fire-and-forget — when the
  one-per-table window is occupied the broker drops the message and says nothing, so the
  client could not tell "queued" from "discarded". `stampede.py` demonstrates exactly this:
  3 JetStream publishes → `accepted=1 rejected=2` with a visible 503, while 3 core
  publishes vanish silently.
- **`waitForDescriptor(kv, table, 60s)`** returning null on timeout instead of hanging, up
  to 5 attempts, with the watch torn down in a `finally` — an abandoned KV watch leaks a
  subscription per attempt, and this is the retry path.

A 503 is logged as *"a snapshot is already pending, waiting for it"*, not as a failure:
another client's request produces a descriptor that serves everyone.

Verified: `tsc --noEmit` clean, `npm run build` clean, and the broker behaviour the fix
relies on re-confirmed by `stampede.py`. **Not** verified in a browser against a live
bridge — that needs a manual run.

### The pattern behind every bug found on 2026-08-16

All four were **a size cap measuring the wrong quantity**, and all four were silent:

| where | the cap counted | what it should have counted | symptom |
| --- | --- | --- | --- |
| snapshot chunks | rows (10 000) | encoded bytes vs `max_payload` | connection closed, snapshot lost |
| CDC batches | `table_len + operation_len + subject_len` — the metadata | `data_len` too — the row | 5 000 events in one 55 MB message; **50 000 events counted as published, zero delivered** |
| `StreamConfig.max_age` | an `i32` | `i64` — it is nanoseconds | any retention over 2.1 s unreadable |
| `cdc_events_published_total` | events *packed into a slot* | events NATS *acknowledged* | dashboard green throughout total data loss |

Worth a rule: **whenever a limit is enforced against a budget, the quantity accumulated
must be the same quantity the budget is denominated in** — and the arithmetic must live in
a named function, not inline in a loop, or nothing can test it. `wireSize`, `shrinkTake`
and `rowsPerChunk` all exist because of this.

Two startup checks now catch the configured version of the same class before any
allocation: slab-vs-memory (cgroup-aware) and `BASE_BUF`-vs-`max_payload`. Both refuse to
start rather than warn.

**Small, independent, pick up any time** (§F): array quoting (`{"solo"}` vs `{solo}` —
systematic, blocks a byte-equality golden test), snapshot failures that abort without
publishing to `init.snap.error.<table>`, golden-value test per PG major (note
`pgoutput`'s `binary` needs PG 14+, but `COPY ... FORMAT binary` goes back to 7.4).

`pg_copy_csv.zig` is now **cleared for deletion** — the gate (a consumer reconstructing
a table from a binary snapshot) was met live on 2026-08-15; see item 1 above. `getTablePrimaryKey` and
`PkMetadata` in `snapshot_listener.zig` are now dead (pagination resolves the key from
`getTableColumns`); they go out with CSV.

---

# A. Landed this session — prerequisites, do not redo

## A1. The bridge is now portable (was Linux-only)

The binary could not run natively on macOS. `Conn.newInbox` issued a raw Linux
`getrandom` syscall; Darwin has no such syscall number, so the kernel killed the
process with **SIGSYS** (exit 140) immediately after `Connecting to NATS...`.

**This was not a bad vendoring decision.** Zig 0.16 gutted `std.posix` — `socket`,
`connect`, `fcntl`, `clock_gettime`, `nanosleep` are all gone (networking moved to
`std.Io`). Upstream g41797/nats 0.0.3 uses `posix.fcntl`, which simply does not
compile on 0.16, so the vendor patch reached for `std.os.linux` to make it build.
The only misstep was choosing raw syscalls over libc, which silently made the whole
binary Linux-only — invisible until the first native run.

Fix applied at **16 sites**: `std.os.linux.*` → `std.c.*`, which switches on
`native_os` and costs nothing since `link_libc = true` already.

- `src/nats_vendor/Conn.zig` — `newInbox`, plus `timespec`/`clock_gettime`
- `src/nats_vendor/Client.zig` — `fcntl` ×2, `socket`, `connect`
- `src/utils.zig`, `batch_publisher.zig`, `event_processor.zig`,
  `snapshot_listener.zig`, `wal_stream.zig`, `mailbox_vendor/mailbox.zig`

Entropy needed a comptime split — **no single libc entry point covers both targets**:

```zig
switch (builtin.os.tag) {
    .linux => _ = std.c.getrandom(buf.ptr, buf.len, 0),  // musl has this, not arc4random_buf
    else   => std.c.arc4random_buf(buf.ptr, buf.len),    // Darwin has this, not getrandom
}
```

## A2. NATS credentials were parsed away and dropped

`nats_publisher.zig` built `nats://user:pass@host:4222`, then split on `@` to find
the host and **discarded the credentials** — never passing them to `JS.CONNECT`. The
server requires authorization, so the client hung forever with no error surfaced.
Fixed in **both** `connect()` and `reconnect()` (they call `CONNECT` separately).

This is the same defect commit `141a00e` fixed in `snapshot_listener.zig`; it was
never applied to the main publisher. Also stopped `nats_url` being logged, since it
embeds the password and those logs ship to Loki.

Still open, minor: `bridge.zig:111` hardcodes `:4222` (no `NATS_PORT`), and
`PublisherConfig.url` carries a hardcoded default.

## A3. Local run setup

`.env.local` (new) holds host-facing values. Two traps, both encoded there:

- **A host Postgres shadows the container.** It binds `[::1]:5432` and
  `127.0.0.1:5432`; those specific bindings beat Docker's wildcard `*:5432`, so
  `localhost` reaches the *host* server (no `bridge_reader` → confusing
  "role does not exist"). Compose now publishes `${PG_PUBLISH_PORT:-5432}`; use
  `PG_PUBLISH_PORT=55432`, and `127.0.0.1` rather than `localhost`.
- `docker-compose.full.yml` also needed: postgres:18 wants the volume at
  `/var/lib/postgresql`, **not** `/var/lib/postgresql/data`; and the `nats:latest`
  image is distroless (no `sh`, no `wget`) so its `CMD-SHELL` healthcheck could
  never pass — removed, `nats-init` polls readiness itself.

Bring the stack up with:

```bash
PG_PUBLISH_PORT=55432 docker compose -f docker-compose.full.yml --env-file .env.prod \
  up -d postgres-primary nats-config-gen nats-server nats-init bridge-init
set -a && source .env.local && set +a
./zig-out/bin/bridge --slot my_slot --pub my_pub --port 9090
```

**Verified working end to end**: slot created, publication verified, JetStream
connected, WAL consumed, events batched and published, graceful shutdown clean.

---

# B. Open threads — resolve before COPY BINARY

## B1. DELETEs vanish on default replica identity  ← highest priority

Silent data loss. The event is *counted* but never reaches JetStream.

| table | `relreplident` | DELETE lands in NATS? |
| --- | --- | --- |
| `test_types` | `f` (full) | yes — `cdc.test_types.delete` |
| `users` | `d` (default, PK only) | **no** — subject never created |

Reproduction (stack up, bridge running):

```sql
INSERT INTO users (name,email) VALUES ('carol','c@example.com');
DELETE FROM users WHERE name='carol';
```

`/status` shows `cdc_events_published` incrementing by 2, but
`nats stream subjects CDC` never grows a `cdc.users.delete.batch` subject. No
warning in the log at info level.

This matters because **PK-only is the Postgres default** — `REPLICA IDENTITY FULL`
is opt-in, and only `test_types` has it. So the bridge silently loses deletes for
any normally-configured table.

Where to look: with default replica identity, pgoutput's DELETE carries only the
key column and the rest are null. Trace `packMutationToSlot` /
`acquireAndFillSlot` in `event_processor.zig` and the batching in
`batch_publisher.zig` — the gap is between the published counter and the actual
JetStream publish. Suspect an empty-column or all-null path dropping the event
after it has been counted.

## B2. Retest the vendored NATS library on its own terms

Validating a networking library only through this app is too weak — especially
after swapping its syscall layer. **`src/nats_vendor/` dropped every upstream test
file**, so there is currently nothing to run.

ndrean cloned upstream to **`/Users/nevendrean/code/zig/nats`**, which already has:

- `src/*_tests.zig` — `root_tests`, `core_tests`, `net_tests`, `parse_tests`,
  `jetstream_tests`, `consumer_tests`, `subscriber_tests`, `misc_tests`,
  `integration_tests`
- build steps: `zig build test`, `zig build integration-test`, `zig build tls`
- `int-test/` — `docker-compose.yml`, per-auth-mode configs
  (`nats-default/token/userpass/nkey.conf`), TLS certs, and
  `run-integration-tests.sh`, which starts a server per auth mode

### Crucial: the clone is Zig 0.15, and does not build on 0.16

`zig build test` in the clone fails outright under 0.16:

```
vendor/zul/src/arc.zig  → invalid builtin '@Type'   (zul itself is 0.15-only)
src/Core.zig            → std.Thread.Mutex removed   (→ std.Io.Mutex)
src/JetStream.zig       → std.Thread.Mutex removed
src/net_tests.zig       → std.net removed
```

CI confirms it targets `zig 0.15.2` (`ci.yml:15`), and `build.zig.zon` declares
`minimum_zig_version = "0.15.0"`.

**So `src/nats_vendor/` is the only 0.16-ported copy of this library in existence.**
Running the clone's suite under 0.15 would validate *upstream* logic, not the ported
code ZeBridge actually ships — which is the code whose syscall layer just changed.
That makes it the wrong test.

### Therefore: port the tests into the vendored copy, not the reverse

Sizing is favourable — **8 of 9 test files have zero 0.16-breaking dependencies**,
because they exercise the library's API rather than std internals:

| file | lines | 0.16 blockers |
| --- | --- | --- |
| `net_tests.zig` | 521 | **5** (`std.net`) |
| `integration_tests.zig` | 404 | 0 |
| `consumer_tests.zig` | 340 | 0 |
| `core_tests.zig` | 288 | 0 |
| `jetstream_tests.zig` | 175 | 0 |
| `misc_tests.zig` | 110 | 0 |
| `parse_tests.zig` | 102 | 0 |
| `subscriber_tests.zig` | 71 | 0 |
| `root_tests.zig` | 26 | 0 |

### DONE — the suite now runs, and passes

Copied the 8 clean test files into `src/nats_vendor/`, plus `int-test/` (configs,
certs, `run-integration-tests.sh`, and upstream's `docker/` compose under
`int-test/upstream-docker/`). Two new build steps:

```bash
zig build test-nats              # 33 unit tests, needs a no-auth server on 4222
zig build test-nats-integration  # needs a live server
```

`src/nats_vendor/root_tests.zig` was adapted: it drops `nkeys.zig` (nkey auth was
never vendored) and `net_tests.zig` (still needs a `std.net` port).

**Result: 33/33 passing**, verified on **both nats 2.11.17 and 2.14.4** — so the
NATS server version is *not* a friction point. The tests hardcode port 4222 with no
credentials, so run them against a no-auth server:

```bash
docker stop nats-server                                   # free 4222
docker run -d --rm --name nats-test -p 4222:4222 -p 8222:8222 nats:2.11 -js -m 8222
zig build test-nats
```

Upstream pins are inconsistent — `docker/docker-compose.yml` uses **2.11**,
`int-test/docker-compose.yml` uses `latest`, CI uses **2.12.3**. Since both ends of
that range pass, treat any future failure as our port, not the server.

### API drift the tests exposed (fixed)

Bringing the tests in surfaced exactly the undocumented divergence of B3:

- Every `CONNECT` / `START` / `SUBSCRIBE` gained an `io: std.Io` parameter in the
  vendor patch. 26 test call sites updated to pass `std.testing.io`.
- `Conn.newInbox()` lost its error union (`![36]u8` → `[36]u8`), so `try` had to go.
- **`Subscriber.zig:88` and `:90` still had `try Conn.newInbox()`** — a genuine
  compile error in the vendored *library*, invisible until now because Zig analyses
  lazily and the bridge never reaches `Subscriber.subscribe`. Fixed.

That last one is the argument for keeping these tests: it was broken code shipping
in the tree that no amount of app-level testing would have found.

**Expect compile failures — and treat them as the point.** The vendor patch changed
`protocol.zig` (128 lines), `Core.zig` (59), `JetStream.zig` (42), `Consumer.zig`
(35), `Subscriber.zig` (23). Any test that no longer compiles is API drift the
vendoring introduced, which is exactly the undocumented divergence in B3.

Also bring `int-test/` across: `docker-compose.yml`, the per-auth configs
(`nats-default/token/userpass/nkey.conf`), the TLS certs, and
`run-integration-tests.sh`.

**Versions (checked):** container `nats:latest` = **2.14.4**, host homebrew
`nats-server` = **2.14.4**, upstream CI pins **v2.12.3** (`ci.yml:20`) and `mac.yml`
just uses `brew install nats-server` (latest). So the server versions are close to
CI and not a likely friction point. The `2.11` sighting is a stale commented-out pin
at ZeBridge's `docker-compose.yml:37`. The real version friction is **Zig 0.15 vs
0.16**, not NATS.

## B3. Vendor divergence is undocumented

`src/nats_vendor/` differs from upstream 0.0.3 by roughly 1000 lines across 9 files
(`Client.zig` 513, `Conn.zig` 224, `protocol.zig` 128, …) with no record of which
changes are 0.16 ports, which are bug fixes, and which are accidents. `Client.zig`
also still carries its **own second `@cImport`** of `<netdb.h>`, which is exactly
the type-namespace problem the main tree just removed via `addTranslateC`.

---

# C. COPY BINARY — ✅ DONE (2026-08-14), verified live

## Why

Snapshots use `COPY ... (FORMAT csv, HEADER true)`, so Postgres renders every value
to text and the bridge parses it back. The CDC path already runs
`START_REPLICATION ... binary 'true'` (`wal_stream.zig:72`), so
`pgoutput.decodeBinColumnData(allocator, type_id, bytes)` is an existing binary
decoder covering ~42 OIDs — NUMERIC, JSONB's version byte, arrays, enums-as-text.

Binary COPY emits **the same per-value encodings that decoder already accepts**.
This is framing plus a type lookup, not a new parser.

## The one real design problem

CSV's `HEADER true` made the stream self-describing — names travelled with the data,
so parser and payload could not disagree. Binary COPY's header is 19 fixed bytes: no
names, no types, no column count. **Layout must arrive out-of-band and be exactly
consistent with what that COPY emitted.**

Rejected sources:

- `schema_publisher.zig` reads `information_schema` and stores `data_type` as a text
  name (`"timestamp with time zone"`). The decoder switches on numeric OIDs, so this
  needs a hand-maintained name→OID table. Brittle. (That path stays — human-readable
  names are the right thing to *publish* to consumers.)
- `pgoutput.RelationMessage` carries real OIDs, but `relation_map` is only populated
  once a RELATION message arrives — i.e. after the first change to that table since
  the bridge connected. A snapshot can be requested before that. Racy.

**Decision: query `pg_attribute` on the snapshot connection, inside the existing
`BEGIN ISOLATION LEVEL REPEATABLE READ`.** One lookup before the chunk loop covers
every chunk, since the schema cannot move under REPEATABLE READ, and no
`ALTER TABLE` can slip between lookup and COPY.

```sql
SELECT a.attname, a.atttypid
FROM pg_attribute a
WHERE a.attrelid = '"public"."users"'::regclass
  AND a.attnum > 0 AND NOT a.attisdropped
ORDER BY a.attnum;
```

Names and OIDs in `SELECT *` order, one round trip. `getTablePrimaryKey` already
uses this exact shape (`::regclass` + `pg_attribute`) on the same connection.

## Wire format

| Part | Bytes | Notes |
| --- | --- | --- |
| Signature | 11 | `PGCOPY\n\377\r\n\0` — reject if mismatched |
| Flags | 4 | int32; bit 16 = OIDs included (expect 0) |
| Header extension length | 4 | int32; skip that many bytes, normally 0 |
| Per tuple | 2 | int16 field count, or `-1` for trailer |
| Per field | 4 + n | int32 length, `-1` means NULL (no bytes follow) |

1. **Each chunk is a separate COPY** — pagination issues a fresh
   `COPY (SELECT ... WHERE pk > $last LIMIT n)` per chunk, so a 19-byte header is
   parsed per chunk, not once per stream.
2. **The per-tuple field count is the only integrity check** without names. If it
   disagrees with the catalog column count that is a **hard error**, never a
   warning — decoding on would silently misassign values to columns.

## Steps

1. **`src/pg_copy_binary.zig`** — header/trailer/tuple framing over `PQgetCopyData`,
   delegating each field to `pgoutput.decodeBinColumnData`. Model the read loop on
   `pg_copy_csv.zig:121` (`executeCopy`), which already handles `PGRES_COPY_OUT` and
   `PQfreemem`. NULL is length `-1`, distinct from a zero-length value.
2. **`getTableColumns`** in `snapshot_listener.zig` — the catalog query above,
   returning `[]struct { name: []const u8, oid: u32 }`. Called once after `BEGIN`.
3. **Swap the generator** — replace `CopyCsvParser` in
   `generateIncrementalSnapshotZig` (the live path). The encoder is unchanged: it
   already takes decoded values plus column names.
4. **Ownership** — rows come from the chunk arena, so do *not* repeat `CsvRow`'s
   per-row `allocator` field. The arena is the only owner; no per-row `deinit`.
5. **Verify against CSV** — snapshot the same table both ways and diff decoded
   values, especially NUMERIC, JSONB, `TIMESTAMPTZ`, `TEXT[]`, and the `INT[][]`
   matrix column in `test_types`, where text and binary most easily differ.

## Rollout

Keep `pg_copy_csv.zig` and its 13 tests until binary is proven end to end. The CSV
path is also still referenced by `generateIncrementalSnapshot` (the older generator),
so it cannot be deleted in the same change regardless. Remove CSV only after binary
completes a full bootstrap of `test_types` and `users` with a consumer
reconstructing both.

## Watch out

- `decodeBinColumnData` decodes based on the OID it is *told*. A wrong mapping
  produces plausible garbage rather than an error — which is why the catalog lookup
  must be transactionally consistent, not cached across snapshots.
- Postgres sends JSON as text even in binary mode, and JSONB as a version byte plus
  text. Already handled (`pgoutput.zig:229`).
- Composite and range types are not in the OID switch. Unknown OIDs must fail loudly
  at snapshot time rather than silently emitting bytes-as-text.

---

---

# E. CDC type guard — ✅ DONE (2026-08-15), verified live

## The gap

`decodeBinColumnData` ends in `else => { log.warn("Unknown type OID"); return .{ .text
= raw_bytes }; }`. That is **correct for an enum** — Postgres sends enum labels as text
in binary — and **silent corruption for anything else**. Observed live: an `hstore`
column shipped its binary wire form, `\0\0\0\1\0\0\0\1k\0\0\0\1v`, as a string value.

The snapshot path is now guarded: `getTableColumns` fetches `pg_type.typtype`, and
`decodeTuple` refuses any unknown OID that is not `'e'` (§C, verified).

**CDC is still open.** It decodes with OIDs from the `RELATION` message, which carries
no `typtype`, so it hits the unguarded fallback.

## Why not a catalog lookup on RELATION

That was the first idea and it is worse: `RELATION` arrives on the replication hot path,
and a blocking `pg_type` query there is exactly what we avoided elsewhere.

**Better seam: the DDL event already runs inside Postgres.** The event trigger has full
catalog access, so the type facts are free at the point they are produced. Add them to
`schema_def` and the bridge never queries at runtime.

## Design (as built)

1. **`init.{core,write}.template.sql`** — add `oid` and `typtype` per column to the `schema_def`
   JSON the event trigger builds.

   Ship **facts, not policy**. Do *not* emit a `supported: true/false` flag: Postgres
   has no idea which OIDs the Zig decoder implements, and encoding that judgement in SQL
   puts the capability list in two places that will drift. The bridge has `isKnownOid`;
   it only needs `typtype` to classify the unknowns.

2. **Keep it off the client payload.** `schema_def` is bridge-facing; the KV value is
   client-facing, and they are already different shapes. Consume `oid`/`typtype` when
   building the KV payload and do not forward them — a client has no use for an OID.

3. **Two populators, mirroring `refused_tables`.**
   - `event_processor.packDdlToSlot` — everything that changes while the bridge runs.
   - `publishBootSchemas` — tables that have had no DDL since startup. It already runs a
     per-table catalog query; add `typtype` to it.

4. **An OID → typtype cache**, consulted by the CDC decode path.

   **It never needs invalidating.** A type's OID lives as long as the type does; drop
   and recreate it and you get a *new* OID arriving on a *new* DDL event, so a stale
   entry describes something that can no longer appear on the wire.

5. **Fail closed.** An unknown OID with *no* cache entry must refuse, never fall back to
   text. The current fallback is the open version of exactly this, and it is what
   shipped hstore bytes as a string.

6. **Failure action: suspend the table.** Reuses `refused_tables`. The reason is now a
   `Reason` enum whose `@tagName` *is* the wire string, so the log line, the KV
   suspension payload and the code cannot drift into three spellings; each carries a
   `fixHint`. The suspension is published **in place of** the offending mutation, which
   keeps stream ordering and tells the client the exact LSN where its data stops being
   complete. Only the first event pays for this — after that `refused.shouldDrop`
   filters the table before the decoder sees it.

## Verified live

- `enum_only(id, feeling mood)` → `cdc.enum_only.insert` with `feeling: "happy"`: an
  enum passes through as its label.
- `exotic(id, feeling mood, attrs hstore)` → no CDC event; instead
  `$KV.schemas.exotic` =
  `{"table":"exotic","suspended":true,"reason":"unsupported_column_type","lsn":…}`.
- `ALTER TABLE exotic DROP COLUMN attrs` → `✅ 'exotic' is no longer refused
  (unsupported_column_type resolved)` and a live schema republished, no restart.
- TOAST: a 5 000-char `EXTERNAL` column, updated *around* → payload keys are `id, n`
  only; updated *directly* → the full value; inserted as SQL NULL → `"big": null`. All
  three distinguishable, which was the whole point.

## What the check found — and it did change the fix

`parseTupleData` **read** the format byte and threw it away: `'t'` and `'b'` took
identical branches, and `'n'` and `'u'` both became `null`. Two independent bugs sat
underneath the type guard:

1. **Text-format columns were decoded as binary.** Postgres falls back to text per
   column even under `binary 'true'` whenever a type has no `typsend`. Those bytes went
   through `decodeBinColumnData` anyway.
2. **Unchanged TOAST was delivered as NULL.** `'u'` means "this UPDATE did not touch a
   TOASTed value, so no bytes were sent". Collapsed to `null`, every client applying the
   event **erased a large column PostgreSQL never modified**. Live for any value past
   the TOAST threshold — the bigger the column, the more certain the loss.

So `TupleData.cols` is now `[]ColumnValue`, a union of `null` / `unchanged` / `text` /
`binary`, and the distinction lives in the type rather than in a comment.

- `.text` → passed through verbatim. Needs no OID knowledge, so it cannot be corrupted
  by lacking any.
- `.unchanged` → encoded by **omitting the column from the payload**, which is exactly
  what Postgres is saying. The reference client already applies only the keys present
  (`INSERT … ON CONFLICT DO UPDATE SET` over `Object.keys(data)`), so it was fixed by
  the bridge telling the truth. Documented in PROTOCOL.md §4.
- `.binary` → the type guard below.

---

# F. Smaller items left from the COPY BINARY work

- **Array quoting.** The bridge writes `{"x","y,z"}` where Postgres writes `{x,"y,z"}` —
  both valid array literals, parsing identically, but not byte-equal. Blocks a golden
  test that asserts byte equality with text `COPY`.
- **Snapshot failures are silent to clients.** `UnsupportedColumnType`,
  `MalformedPrimaryKey` and `UnsafeStringLiterals` all abort server-side without
  publishing to `init.snap.error.<table>`. Each deserves an `error_type`
  (`PROTOCOL.md` §6).
- **Fold the two catalog queries.** `getTableColumns` is now the only source of both
  layout and key, so `getTablePrimaryKey` and `PkMetadata` are dead code. They go out
  with CSV rather than in a separate change.
- **Golden-value test per PG major.** `pgoutput`'s `binary` option needs PG 14+, so the
  bridge will not start on older servers — but `COPY ... FORMAT binary` works back to
  7.4, so the decoder alone can be exercised much further back.
- ~~**Do not prune CSV yet.**~~ ✅ The gate was "binary completes a full bootstrap with a
  consumer reconstructing the table", and it was met on 2026-08-15 — a fresh
  web-consumer seeded `test_types` from a binary snapshot and handled the empty `users`
  correctly in the same run. Deleted with evidence, as intended, not on a green log line.

## Next up, in order

1. **§E above** — the CDC type guard.
2. **Client item D** — LSN persistence, JetStream consumer with `ByStartSequence`,
   snapshot application. Unblocks TEST_SCENARIOS groups B, C3, C4, D2, lets F6 end in a
   re-seed instead of a caveat, and opens the gate on deleting CSV.

---

---

# G. Composite keyset pagination — ✅ DONE (2026-08-15), verified live

The reason COPY BINARY went first: `ColumnMeta.pk_ord` already carried the whole key on
typed values, so this was a change to pagination alone.

## What changed

- **`pg_copy_binary.keysetPredicate` / `orderByClause`** — build
  `("a","b") > ('7'::integer, 'carol'::text)` and `ORDER BY "a","b"` from the catalog
  rows. A single-column key degenerates to `("a") > (…)`, which Postgres reads as a
  plain scalar comparison, so there is **one** code path rather than a special case.
- **`Streamer` captures N cursor values**, not one: `last_pk` is now
  `?[]const []const u8`, filled from `pk_idx` in key order. A NULL in a key column is
  `error.NullPrimaryKeyValue` — a PK column is NOT NULL, so a null there means the
  catalog layout is not the one this COPY emitted, the same class of fault as a field
  count mismatch.
- **`resolvePrimaryKey`** orders the key from `pk_ord` and rejects ordinals that are not
  exactly `1..n` (`error.MalformedPrimaryKey`), which would otherwise leave a hole in
  the cursor.
- **`ColumnMeta.type_text`** now carries `format_type(...)` for every column, and every
  cursor literal is cast to it. Inside a row constructor an untyped literal is
  `unknown`; naming the type makes the comparison the same one `ORDER BY` uses, on
  domains and enums as much as on `int`.
- **Preflight's `snapshots_unimplemented` finding is gone** — a composite key is now an
  ordinary table shape, not a warning.

## Two bugs fixed on the way

- **The first chunk used a sentinel.** `"id" > 0` for a numeric key silently dropped any
  row with a zero or negative key, and `''` was not a real lower bound for text either.
  The first chunk now takes the whole table (`1=1`) and only later chunks carry a
  cursor.
- **Cursor literals were interpolated unescaped.** A key value containing `'` produced a
  syntax error at best. Quoting now doubles `'`, `bytea` goes out as `'\x…'` (raw bytes
  would have carried a NUL that truncates the statement at the libpq boundary), and the
  snapshot refuses to start if `standard_conforming_strings` is off — read from
  `PQparameterStatus`, so it costs no round trip.

## Verification

Unit tests cover predicate shape, key order vs column order, quote escaping and bytea
hex. Live, on the fixture described in TEST_SCENARIOS §G4 — 25 000 rows over
`(tenant, name)` with 3 000 names per tenant, so both chunk boundaries fall inside a run
of equal `tenant`:

```
📸 'ck': paginating on a 2-column primary key
📦 chunk 0 (10000 rows) … 📦 chunk 1 (10000 rows) … 📦 chunk 2 (5000 rows)
```

Chunk 0 ends at `(3, u'-00999)` and chunk 1 resumes at `(3, u'-01000)` — paging on
`tenant` alone would have skipped the remaining 2 000 rows of tenant 3. Decoded:
25 000 rows, 25 000 distinct keys, globally ascending. The single-column fixture
(`sk`, ids -1..15000) returns 15 002 rows including `-1` and `0`.

Then on the real scenario table: `users` after step 5 (`PRIMARY KEY (name, email)`,
both `varchar(255)`), padded to 12 515 rows so a boundary falls inside a run of equal
names. The emitted key sequence was **identical to PostgreSQL's own
`ORDER BY name, email`** — which also pins the collation question, since `User-*` sorts
after `n-*` under the database's collation but before it in byte order.

---

# D. The headline feature — bi-directional write path (NATS → ZeBridge → Postgres)

> Ordered last deliberately: sections E and F are the open work on the read path, and
> this is the next *feature*. Section letters are historical, not a running order —
> see "Next up, in order" at the end of §F.

The headline feature. Design settled 2026-08-10; **partially built and audited
2026-08-15** — see the checklist below, which is the running order.

## D0. Checklist (2026-08-15)

Written after auditing `mutation_listener.zig` and comparing against PowerSync and
Electric. Client-facing rules live in `PROTOCOL.md` §7; this is the bridge side.

**Security — first, because "it works" and "it is correct" currently diverge silently**

- [x] **subject grammar fixed 2026-08-15**: `mutation.<principal>.<table>.<operation>`,
      in `grammar.json` → `build.zig` → `Config.Nats.mutation_subject_pattern` plus the
      token positions. Nothing parses it yet; the contract is pinned so the switch from
      nkey to JWT/operator becomes a deployment change rather than a client migration.
      Principal first because a per-user grant is then one wildcard (`mutation.<id>.>`)
      and adding a table reissues no credentials
- [x] **principal, table and operation from the subject**, never the payload — NATS
      authorizes subjects, so a payload-derived table means broker permissions constrain
      nothing, and a payload-derived identity is worth nothing at all
- [x] reject subjects that do not have exactly `mutation_token_count` tokens, before
      parsing any of them
- [x] identifiers validated against the catalog, never interpolated (today a table named
      `x" ; DROP …` closes the quote)
- [x] `zebridge_ddl_events` explicitly excluded — writable today, which lets a client
      forge a schema for every other client
- [x] **`bridge_writer` role — DONE 2026-08-16.** Created by `init.{core,write}.template.sql` with
      `NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS` and **zero table
      privileges**; `zebridge_grant_edge_writes(regclass)` opens one table at a time and
      refuses `zebridge_ddl_events` by name. The bridge connects through it on a separate
      connection (`DATABASE_WRITER_URL` or `POSTGRES_WRITER_USER/_PASSWORD`); no writer
      configured means the mutation listener does not start, rather than falling back to
      the read role. Verified: `permission denied` before a grant, `INSERT 0 1` after,
      `bridge_reader` unchanged at SELECT + REPLICATION.

**✅ The identity model is now enforced (verified live 2026-08-17).**

§7.1's guarantee — the principal is trustworthy because NATS authorises subjects — had no
backing until now: the web client connected with **the bridge's own nkey** and the server
carried no `permissions` block, so one identity did everything. Both are fixed in
`nats-server.conf.template`: the bridge keeps its nkey with `>`/`>`, and each client
principal is a user/password entry allow-listed to its own `mutation.<principal>.>`.

Verified against a live server, not just parsed — as `alice`: writing `mutation.bob.…`,
forging `cdc.>`, overwriting `$KV.schemas.>`, consuming `MUTATIONS`, and purging or
deleting `INIT` are all refused; her own writes, KV reads, durable pull + ack on `INIT`,
and JetStream-published snapshot requests all succeed.

Two things this does **not** buy, recorded so they are not assumed:

- **A browser password is enforced, not secret.** It authenticates the bundle, not the
  person; anyone with devtools can write as `alice` from curl. Short-lived per-session
  JWTs are the endpoint — same permission shape, so nothing above changes.
- **JS API denials surface as client timeouts, not permission errors.** The server drops
  the denied publish and the caller waits out its own deadline. Budget for that when
  debugging a too-narrow allow-list.

⚠️ **Gap this exposed:** `mutation_error_pattern` is `mutation_error.{[table]s}` — no
principal token. A client can only hear its own rejection by subscribing to every
principal's rejections for that table, and the dead-letter body quotes the payload that
failed, so alice reads bob's rejected rows. Fix is
`mutation_error.{[principal]s}.{[table]s}`, after which the client's subscribe narrows to
`mutation_error.alice.>`. Not done: it changes the wire contract, so it belongs with the
reply-channel work rather than being slipped in here.

**Row-level authorization — the audit stopped at "can write any table" before reaching
"can write any row". Without this, a client permitted to write a table can write *every
row of it*.**

- [x] `SET LOCAL zebridge.principal = '<subject token>'` before each statement;
      policies compare `current_setting('zebridge.principal')` against an ordinary
      column. One database role, any number of principals — a PG role per mobile user is
      unworkable
- [x] `bridge_writer` must **not own** the tables and must not have `BYPASSRLS`: owners
      are exempt from RLS unless `FORCE ROW LEVEL SECURITY` is set, and that exemption
      fails **open** ✅ Verified: `bridge_writer` has
      `rolbypassrls = f`, and the tables are owned by `postgres` — an owner bypasses RLS
      on its own tables, so this is what makes the row policies bind the write path at all.
- [x] the principal is the application's **internal user id**, immutable for the life of
      the account (it lands in the credential, every subject, the policy's column, and
      any queued mutation simultaneously)
- [x] batching interaction: `SET LOCAL` is per transaction, so a batch mixing principals
      re-issues it per statement. Safe **only** with the individual-retry fallback below,
      since one row's constraint or RLS failure aborts the whole transaction

**Correctness**

- [x] **poison-pill guard — DONE 2026-08-16.** `max_deliver = 5` on the ingress
      consumer, plus `isPermanent(err)`: a payload that cannot decode is dead-lettered to
      `mutation_error.<table>` and **ACKed**, while a transient failure still NAKs.
      Verified against the same reproduction: 24 redeliveries in 15 s → **1**, with
      `Outstanding Acks: 0`. ⚠️ The dead letter is currently **logged, not published** —
      the listener's handle is pull-only, so the publish lands with the reply channel.
- [x] **orphaned snapshot chunks purged on abort — DONE 2026-08-16.** A snapshot that
      dies after publishing chunk 0 left its chunks in `INIT` under an id no descriptor
      references. Invisible to clients — the descriptor is written only after COMMIT — but
      not harmless: `INIT` is 8 GiB with **discard = old**, so a wide table failing
      repeatedly evicted *other* tables' snapshot chunks and schemas. They are now purged
      by subject, and `init.snap.error.<table>` carries the `snapshot_id`. Needed a
      `PURGE_FILTER` on the vendored client, which only had whole-stream purge. Verified:
      COPY killed mid-run → 670 subjects down to 2, KV descriptor still pointing at the
      previous *successful* snapshot.
- [x] **dead-letter *publish* — DONE 2026-08-16.** It logged and stopped, on the note
      that "the consumer connection is pull-only". That was wrong: `nats.Consumer.PUBLISH`
      exists — the snapshot listener already answers `init.snap.error.<table>` the same
      way — and `mutation_error.>` is one of the MUTATIONS stream's subjects, so the
      message is stored rather than dropped for want of a stream. Still best-effort: a
      failed publish logs and returns, because refusing to ACK would restore the infinite
      loop the dead-letter path exists to end.
- [x] reply on the reply-to subject: `accepted` / `stale` / `row_deleted` / error.
      Both PowerSync's blocking FIFO queue and Electric's `awaitTxId` need it; without
      it a client cannot dequeue, and `row_deleted` has nowhere to go
- [ ] **clamp future version values**, and return the clamped value. ⚠️ Still open, and
      PROTOCOL.md §7.2 already promises it — a skewed clock writes a version nobody can
      beat, and every later write is silently rejected as stale
- [x] keep the `IS NULL OR` guard — a NULL stored version otherwise rejects every write

**Version column (LWW)**

- [x] **`SYNC_RULES` + `SYNC_VERSION_COLUMN` — DONE 2026-08-16.** Same grammar as
      `TRANSITION_RULES`, same parser (`parseTableRules`). An optional per-table
      override: the primary source is the table's `zebridge_catalogue` row, loaded at
      boot. `-arrival` (writable with last-arrival-wins) is **not** implemented yet.
- [x] **preflight reporting — DONE 2026-08-16.** Checks the *named* column, never
      searches; lists the table's orderable columns as candidates with the exact
      `SYNC_RULES=` line to set. Warns on naive-vs-tz, second precision and nullability
      without refusing any of them — Ecto's `timestamps()` produces naive columns, so
      refusing would exclude the reference schema. `classifyVersionColumn` is pure and
      unit-tested on all four verdicts.
- [x] **refuse `created%` / `inserted%` — DONE 2026-08-16**, however configured.
- [x] case-sensitive and always quoted — `updatedAt` is not `updatedat`
- [x] publish `"sync": {version, tombstone, writable}` in the schema KV so clients
      discover it instead of hardcoding

**Two triggers per edge-writable table**, generated by the bridge's init SQL — needed
because **apps write to PostgreSQL directly**, which is the entire premise of CDC:

- [ ] `BEFORE UPDATE` — stamp the version column when a writer did not. Raw SQL, cron
      jobs and data fixes do not maintain it; the failure is silent and asymmetric (the
      row changes, the version does not, so a stale phone overwrites a fresh backend write)
- [ ] `BEFORE DELETE` — convert the delete into `UPDATE … SET deleted_at = now()` and
      `RETURN NULL`. Without it a backend `DELETE` is physical, no tombstone exists, and
      an offline client's queued edit resurrects the row immediately — not after
      `GC_THRESHOLD_MS`, but at once. See "consequences for direct apps" below.

**Deletes and GC**

- [x] `deleted_at` when the table has one; hard delete otherwise (stated weaker guarantee)
- [x] `GC_THRESHOLD_MS` = the stated maximum offline window, not a tuning knob
- [ ] **publish the GC watermark to KV** after each sweep; clients compare their oldest
      queued version against it **before flushing the outbox**
- [ ] keep GC's own deletes off the wire — a sweep of a million tombstones is otherwise
      a million CDC events about rows every client already knows are dead

**Throughput — last, and only what is free**

> Nothing here is started. Ordering still holds: correctness first.

- [ ] batch N mutations per transaction (measured 8.6× locally)
- [ ] **individual-retry fallback**: on batch failure, replay the batch one statement at
      a time so a single row's constraint or RLS violation costs latency, not other
      clients' writes. Required before batching mixed principals is safe, and it is the
      same machinery the dead-letter path needs
- [x] ~~`synchronous_commit = off`~~ — **decided against**, not outstanding: batching already amortises the fsync, and
      with a reply channel the bridge would be acking writes a crash can still lose
- [x] no libpq pipeline mode, no multi-row bucketing — **a decision, deliberately kept**

### Consequences for apps that write to PostgreSQL directly

The `BEFORE DELETE` trigger changes semantics for every writer, not just the edge. These
must be in the docs, because `<table>_live` only fixes the first one:

| what changes | why | remedy |
| --- | --- | --- |
| `SELECT` returns tombstones | the row is still there | query `<table>_live`, or add `WHERE deleted_at IS NULL` |
| **UNIQUE constraints still hold against deleted rows** | the tombstone occupies the key, so re-creating a "deleted" user fails | partial index: `CREATE UNIQUE INDEX … WHERE deleted_at IS NULL` |
| **foreign keys stay valid** | children keep pointing at a deleted parent; `ON DELETE CASCADE` never fires | cascade in the trigger, or accept it |
| `DELETE … RETURNING` returns nothing | the trigger returns NULL | use the UPDATE form |
| **`DELETE` reports 0 rows affected** | ⚠️ `Repo.delete!/1` raises `Ecto.StaleEntryError`; ActiveRecord and others check the same count | app must delete via an update, or use `<table>_live` with a rule |

The last row is the one that will bite first — your own emitter uses `Repo.delete!`.

**Trust split: NATS is the policy engine, zebridge is a dumb dispatcher, Postgres
holds the business rules.**

## Command model — Postgres function dispatch

The bridge never builds SQL from client input. It calls:

```sql
SELECT app.<action>($1::text, $2::jsonb)   -- $1 = principal, $2 = client payload
```

The DBA owns logic, atomicity and validation in SQL, so new actions ship without
rebuilding the bridge. Rejected: a bridge-side SQL template allowlist, and a generic
CRUD envelope (pushes all authorization to the edge — widest blast radius).

## Authorization — nats-server does it, zebridge does none

Subject layout is **`cmd.<principal>.<action>`**, e.g. `cmd.alice.transfer_funds`.

This shape is forced: **NATS authorizes subjects, not payloads.** Both the principal
and the action must be subject tokens or broker permissions have no purchase on
them, and the story collapses back to trusting the payload.

```
user alice: { publish: ["cmd.alice.transfer_funds", "cmd.alice.close_account"] }
```

alice cannot publish as bob, nor invoke `delete_account` at all — enforced before
zebridge sees a byte. The payload carries parameters only, never identity. Reading
the principal off the subject is not the bridge doing authorization; it is reading a
field the broker already vouched for.

Rejected: client-minted signed tokens. A token the edge client signs itself proves
nothing — it chose the claims. That would need a distinct issuer plus an Ed25519
public key in the bridge, which only earns its cost if one NATS connection
multiplexes many end users (a shared gateway). Revisit only if that becomes true.

## Two things the bridge must still do, despite being "blind"

1. **Validate the action as a SQL identifier** (`^[a-z][a-z0-9_]{0,62}$`). A function
   name cannot be parameterized, so this is the one place client-derived text reaches
   SQL text. Check it regardless of NATS permissions.
2. **Run as a `bridge_writer` role with `EXECUTE` on schema `app` and *zero* table
   privileges.** This is the backstop that makes "blindly works" safe: even a bug in
   subject parsing cannot reach a table directly. The existing `bridge_reader`
   (SELECT + REPLICATION) stays read-only — the write path uses a separate
   connection under a separate role.

## Delivery — JetStream durable + reply

Commands land on a durable CMD stream with a `Nats-Msg-Id` for dedup; the bridge
replies on the NATS reply-to subject with ok/error, then ACKs. Survives bridge
restarts, gives the edge at-least-once with idempotency, and mirrors the ACK
discipline the read path already uses.

## Memory

A command is short-lived and off the WAL hot path, so it wants an **arena per
command**, reset between dispatches — the same shape as a snapshot chunk, not the
pre-allocated event slab. The reply is written and gone before the next command
starts.

## Testing

The Elixir app in `consumer/` already writes directly to Postgres to generate CDC.
Switching that producer side to publish commands instead gives a clean A/B against
the same consumer — one path swapped, everything else identical.

## Deployment note

External clients reach nats-server over **wss**; nats-server terminates TLS for them.
zebridge ↔ nats-server stays plaintext because the vendored Zig client has no TLS
(`Conn.zig` hardcodes `"tls_required": false`). That is safe **only while bridge and
nats-server are co-located**. Splitting them across hosts silently makes it a
vulnerability — the same caveat as `PG_SSLMODE` in A3.
