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
`NATS_NKEY_SEED="SU..." docker compose -f docker-compose.full.yml --env-file .env.admin
up -d postgres-primary nats-config-gen nats-server nats-init bridge-init`, then run the
bridge from the host with `.env.bridge` (it needs DATABASE_URL; there is no PG_* fallback
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

4. 🔴 **Snapshot chunks are capped in rows, not bytes — any table with rows averaging
   more than ~104 bytes cannot be snapshotted at all.** Found 2026-08-16 by
   `scripts/scenarios/snapshot.py`. `Snapshot.chunk_size` is a compile-time `10_000`
   and `streamToEncoder` publishes whatever those rows encode to, with no reference to
   the server's `max_payload`. Measured on `test_types` (12 columns): 9,000 rows =
   996,789 bytes and publishes fine; 10,000 rows ≈ 1.11 MB and the server closes the
   connection — `error.Timeout`, then `WriteAllHUPError` on all five retries, because
   the payload never changes and the retry can only re-kill the connection. The snapshot
   is lost, no `init.snap.error.<table>` is published, and the CDC publisher on the same
   process reconnects in a loop.

   `Config.Nats` already documents this hazard for the CDC path and `bridge.zig:403`
   already reads `nats_publisher.serverMaxPayload(js)` at boot; the snapshot path is
   simply the one that never got the guard.

   The fix is a byte budget, but it touches the wire contract, so decide first: the
   encoder writes its array header with the row count **up front**, so a chunk cannot be
   truncated mid-encode — it has to be re-encoded as a prefix of the rows already
   fetched (cheap, no re-query). That makes `num_rows < chunk_size` stop meaning "last
   chunk", which is what `X-Snapshot-Final-Chunk` is derived from today and what both
   consumers read. So `streamToEncoder` must report *fetched* and *encoded* row counts
   separately, and final becomes `fetched < chunk_size and encoded == fetched`.

**Untested, noted 2026-08-16: encryption in transit.** Neither leg has ever been run
with TLS.

- **Postgres.** Should be the easy one now: `sslmode` rides in `DATABASE_URL`'s query
  string (`?sslmode=verify-full&sslrootcert=…`), libpq does the work, and `connInfo`
  passes the URL through untouched. Worth an actual run against a TLS-enabled server
  before claiming it — in particular that `verify-full` still works once `connInfo`
  appends its keepalives, and on the **replication** connection, which appends
  `replication=database` as well.
- **NATS: a deployment constraint, arrived at the honest way (2026-08-16).** TLS was
  attempted against the vendored pure-Zig client and not achieved — adding it looks like
  substantial work in the client itself, where nkey auth was already a local patch. So
  the deployment removes the need instead: **the bridge and nats-server are colocated on
  one VPS and talk over loopback**, and there is no link to encrypt.

  Worth being clear that this is a mitigation, not a preference — it is sound (loopback
  is not a network an attacker sits on), but it constrains the topology, and the
  constraint should be re-examined if the client ever gains TLS. `Endpoint.parseUrl` accepts only `nats://` and rejects `tls://` with
  `MissingScheme`, which matches the deployment rather than limiting it: a URL promising
  transport security the client cannot provide is worse than a refusal.

  What this rules out, and should be stated wherever the topology is described: a
  *remote* NATS. Clients still reach the server from anywhere — that is the websocket
  port and the broker's own concern — but the bridge→NATS hop is an intra-host one by
  design. Revisit only if the client gains TLS.

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

1. **`init.sql.template`** — add `oid` and `typtype` per column to the `schema_def`
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
      in `topology.json` → `build.zig` → `Config.Nats.mutation_subject_pattern` plus the
      token positions. Nothing parses it yet; the contract is pinned so the switch from
      nkey to JWT/operator becomes a deployment change rather than a client migration.
      Principal first because a per-user grant is then one wildcard (`mutation.<id>.>`)
      and adding a table reissues no credentials
- [ ] **principal, table and operation from the subject**, never the payload — NATS
      authorizes subjects, so a payload-derived table means broker permissions constrain
      nothing, and a payload-derived identity is worth nothing at all
- [ ] reject subjects that do not have exactly `mutation_token_count` tokens, before
      parsing any of them
- [ ] identifiers validated against the catalog, never interpolated (today a table named
      `x" ; DROP …` closes the quote)
- [ ] `zebridge_ddl_events` explicitly excluded — writable today, which lets a client
      forge a schema for every other client
- [x] **`bridge_writer` role — DONE 2026-08-16.** Created by `init.sql.template` with
      `NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS` and **zero table
      privileges**; `zebridge_grant_edge_writes(regclass)` opens one table at a time and
      refuses `zebridge_ddl_events` by name. The bridge connects through it on a separate
      connection (`DATABASE_WRITER_URL` or `POSTGRES_WRITER_USER/_PASSWORD`); no writer
      configured means the mutation listener does not start, rather than falling back to
      the read role. Verified: `permission denied` before a grant, `INSERT 0 1` after,
      `bridge_reader` unchanged at SELECT + REPLICATION.

**Row-level authorization — the audit stopped at "can write any table" before reaching
"can write any row". Without this, a client permitted to write a table can write *every
row of it*.**

- [ ] `SET LOCAL zebridge.principal = '<subject token>'` before each statement;
      policies compare `current_setting('zebridge.principal')` against an ordinary
      column. One database role, any number of principals — a PG role per mobile user is
      unworkable
- [ ] `bridge_writer` must **not own** the tables and must not have `BYPASSRLS`: owners
      are exempt from RLS unless `FORCE ROW LEVEL SECURITY` is set, and that exemption
      fails **open**
- [ ] the principal is the application's **internal user id**, immutable for the life of
      the account (it lands in the credential, every subject, the policy's column, and
      any queued mutation simultaneously)
- [ ] batching interaction: `SET LOCAL` is per transaction, so a batch mixing principals
      re-issues it per statement. Safe **only** with the individual-retry fallback below,
      since one row's constraint or RLS failure aborts the whole transaction

**Correctness**

- [x] **poison-pill guard — DONE 2026-08-16.** `max_deliver = 5` on the ingress
      consumer, plus `isPermanent(err)`: a payload that cannot decode is dead-lettered to
      `mutation_error.<table>` and **ACKed**, while a transient failure still NAKs.
      Verified against the same reproduction: 24 redeliveries in 15 s → **1**, with
      `Outstanding Acks: 0`. ⚠️ The dead letter is currently **logged, not published** —
      the listener's handle is pull-only, so the publish lands with the reply channel.
- [ ] reply on the reply-to subject: `accepted` / `stale` / `row_deleted` / error.
      Both PowerSync's blocking FIFO queue and Electric's `awaitTxId` need it; without
      it a client cannot dequeue, and `row_deleted` has nowhere to go
- [ ] clamp future version values, and return the clamped value
- [ ] keep the `IS NULL OR` guard — a NULL stored version otherwise rejects every write

**Version column (LWW)**

- [x] **`SYNC_RULES` + `SYNC_VERSION_COLUMN` — DONE 2026-08-16.** Same grammar as
      `TRANSITION_RULES`, same parser (`parseTableRules`). `-arrival` (writable with
      last-arrival-wins) is **not** implemented yet.
- [x] **preflight reporting — DONE 2026-08-16.** Checks the *named* column, never
      searches; lists the table's orderable columns as candidates with the exact
      `SYNC_RULES=` line to set. Warns on naive-vs-tz, second precision and nullability
      without refusing any of them — Ecto's `timestamps()` produces naive columns, so
      refusing would exclude the reference schema. `classifyVersionColumn` is pure and
      unit-tested on all four verdicts.
- [x] **refuse `created%` / `inserted%` — DONE 2026-08-16**, however configured.
- [ ] case-sensitive and always quoted — `updatedAt` is not `updatedat`
- [ ] publish `"sync": {version, tombstone, writable}` in the schema KV so clients
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

- [ ] `deleted_at` when the table has one; hard delete otherwise (stated weaker guarantee)
- [ ] `GC_THRESHOLD_MS` = the stated maximum offline window, not a tuning knob
- [ ] publish the GC watermark to KV after each sweep; clients compare their oldest
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
- [ ] ~~`synchronous_commit = off`~~ — dropped: batching already amortises the fsync, and
      with a reply channel the bridge would be acking writes a crash can still lose
- [ ] no libpq pipeline mode, no multi-row bucketing for now

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
