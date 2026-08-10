# Working plan — snapshots via `COPY ... (FORMAT binary)`

Carries the session state a fresh session would otherwise lose. Three parts:
**A.** what already landed and must not be re-derived, **B.** open threads blocking
COPY BINARY, **C.** the COPY BINARY plan itself (still not started).

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

# C. COPY BINARY — the actual plan (not started)

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

# D. After COPY BINARY — bi-directional write path (NATS → ZeBridge → Postgres)

The headline feature. Design settled 2026-08-10; not started.

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
