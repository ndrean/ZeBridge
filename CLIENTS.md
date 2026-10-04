# The two clients — what each does, and where they differ

Two client libraries speak the protocol.

| | libzb | zb-client-ts |
| --- | --- | --- |
| language | Zig core + shell, C ABI | TypeScript core + shell |
| hosts | through its bindings: Python (`zb-python`), Kotlin/Java (`zb-android`), Dart/Flutter (`zb-dart`); any other language through the C ABI directly | browser, Node, React Native |
| local engine | SQLite (a file), PostgreSQL (`dbUrl`; seeds through COPY), or DuckDB (`engine: "duckdb"`; libduckdb opened at run time; seeds through the appender; the micro-VM worker's analytical replica, a file DuckDB itself opens once libzb closes it) | SQLite (sqlocal, better-sqlite3), PGlite |
| large answers | `reply` puts an answer past `results.inline_max_bytes` in `res-<tenant>` and sends an envelope; `request` resolves it | the same, in `serve`'s reply and in `request` |
| being a service | `zb_client_serve` + the `requests` in `poll` + `zb_client_reply`: the host's loop answers | `serve({tenants, handlers, queue})`: async handlers, the library subscribes and replies |
| cooperative documents | `mergeRegisters` through `zb_call` (COOPERATIVE_EDITING.md) | `mergeRegisters`, imported |
| on-demand answers | `zb_client_request` + `zb_client_ingest` | `request` + `ingest` |
| reply inbox | `_INBOX.<principal>`, from the `inbox_prefix` connection option — replies, KV watchers, object reads and pull consumers all land there, inside the principal's `_INBOX.<principal>.>` grant | the same, through nats.js's `inboxPrefix` |
| loop | host-driven: `sync`, `poll`, `flush` | self-driven: `connect()` runs it |
| tables followed | **one rule, both clients (`core.tableSet`, fixtures `tableSet`)**: `tables` is a list, or `"*"` for every published table; `ondemandTables` are held for their schema only, never seeded or tailed, rows through `ingest`; a name in both is on-demand; absent both, nothing is followed — and the TS client says so in its log | the same |
| tenants followed | every membership in `$KV.tenants.<principal>` (a set): one chain per tenant into one table, one CDC stream per tenant, watermarks per (table, tenant); `zb_client_join`/`zb_client_leave` at runtime; a join the credentials cannot read is refused, and a stream that becomes unreadable is set aside alone and named in the poll report (`unreadable`) | the FIRST membership only, with a warning when there are more (parity queued) |

## The first ten lines

The same shape in every language: one options object, the same keys, passed to a
constructor. The first run on a device needs only the bridge's URL and the invite your
backend handed it; later runs need nothing but the tables. Enrollment, the identity file,
the NATS URL and JWT renewal are the library's.

TypeScript (browser, Node, React Native):

```ts
import { ZeBridge } from 'zb-client-ts';
const zb = new ZeBridge({ bridgeUrl: 'https://zb.example.com', invite: code, tables: ['orders'] });
await zb.connect();
zb.onChange('orders', refresh_ui_callback);
const open = await zb.query('SELECT * FROM orders WHERE status = ?', 'open');
await zb.mutate('orders', 'UPDATE', { id: 7 }, { status: 'done' });
```

Python:

```python
from zebridge import ZeBridge
with ZeBridge(bridge_url="https://zb.example.com", invite=code, tables=["orders"], on_change=refresh) as zb:
    open_ = zb.query("SELECT * FROM orders WHERE status = ?", "open")
    zb.mutate("orders", "UPDATE", {"id": 7}, {"status": "done"})
```

Kotlin (Android):

```kotlin
val zb = ZeBridge(mapOf("bridgeUrl" to "https://zb.example.com", "invite" to code, "tables" to listOf("orders")), context) { refresh_ui_callback() }
val open = zb.query("SELECT * FROM orders WHERE status = ?", "open")
zb.mutate("orders", "UPDATE", mapOf("id" to 7), mapOf("status" to "done"))
```

Dart (Flutter):

```dart
final zb = await ZeBridgeWorker.spawn({'bridgeUrl': 'https://zb.example.com', 'invite': code, 'tables': ['orders']});
zb.reports.listen((r) => refresh_ui_callback());
final open = await zb.query('SELECT * FROM orders WHERE status = ?', ['open']);
await zb.mutate('orders', 'UPDATE', {'id': 7}, {'status': 'done'});
```

Any other language, through the C ABI — five calls, JSON in and out:

```c
uint64_t h = zb_client_connect("{\"bridgeUrl\":\"https://zb.example.com\",\"invite\":\"…\",\"tables\":[\"orders\"]}");
zb_free(zb_client_sync(h));                 // seed and catch up
for (;;) zb_free(zb_client_poll(h, 1000));  // the loop: tail, verdicts, JWT renewal
char *rows = zb_client_query(h, "SELECT * FROM orders", "[]");  /* … */ 
zb_free(rows);
zb_client_close(h);
```

**The two table lists**:

- `tables` are seeded and followed live;
- `ondemandTables` get their schema only, and rows arrive when this client asks a service (`request`) and keeps the answer (`ingest`).

## Writes: what the library does for you

An app calls `mutate` and reads rows back with `query`. Everything between is the
library's, in both clients, and it is what PROTOCOL §7 requires of any client:

1. **Optimistic apply and outbox, in one transaction.** The row changes locally at once,
   and the write is queued in `_zebridge_outbox` in the same transaction, with the row's
   state before it. A crash cannot keep one without the other.
2. **Sent in the order it was written**, with a version stamp and a `msg_id`, so a retry
   is the same write, not a second one. Before sending, the queue is checked against the
   GC watermark: a write older than it is refused locally and its row restored.
3. **A verdict per write** (`mutation_ack.<principal>.<msg_id>`), and the library acts on it:

   | verdict | the library |
   | --- | --- |
   | `accepted` | drops the entry; the CDC echo confirms the row |
   | `stale` | drops the entry; rebases the write onto the winning row when their columns do not overlap, else surfaces it |
   | `row_deleted` | drops the entry and the local row |
   | `rejected` | drops the entry and restores the row's state before the write |
   | `failed` (`rate_limited`) | keeps the entry, holds the queue for `retry_after_ms` |
   | no verdict yet | keeps the entry and sends it again on the next flush; missed verdicts are read back from the `VERDICTS` stream |

4. **State only arrives through CDC.** A verdict is never data: the row the app sees is
   what PostgreSQL emitted, apart from the optimistic row still waiting for its echo.

Last-write-wins is decided by PostgreSQL on the table's version column (and its tiebreak
column, on equal versions). The version, tombstone and tiebreak columns are the table's
`zebridge_catalogue` row, and every client learns them from the schema it receives.

⚠️ A client written from scratch, not on libzb or zb-client-ts, must do all four. Without
them it does not get a weaker guarantee, it loses writes: a write sent while offline and
not queued is gone, and one with no verdict looks the same whether it was applied or
refused.

## Bindings: the rule

A binding is the thin layer that gives one language libzb's C ABI. It may contain **only**:

1. **the declarations** of the C functions (types, freeing the returned strings);
2. **its language's thread model** — libzb drives a client from one thread: a worker thread (Python, Kotlin), an isolate (Dart), a loop the host already has;
3. **error mapping** — libzb's words (`{"error"}`, `zb_last_error`) into the language's exception;
4. **a default storage location** — the app-private directory where the platform has one.

**Behavior never goes in a binding.** A retry, a default value, a parsing rule, a renewal policy: libzb, where every binding gets it at once. Enrollment and renewal were built that way, and they cost the Kotlin and Dart bindings almost nothing. A binding with logic is a second implementation, and a second implementation drifts — the three Flutter examples' copies had, before `zb-dart` replaced them.

Each binding pins the ABI version it was written for (`ZB_ABI` / `zbAbi`), and `libzb/python/abi_check.py` fails when any pin disagrees with libzb.
The bindings today: `zb-python` (~300 lines), `zb-dart` (~600, the isolate worker included), `zb-android` (~430, JNI included). A new one — Swift, Go, Rust, Ruby — starts from the closest of them.

## The configuration keys, side by side

One vocabulary: the same key means the same thing in libzb's `opts_json` and in zb-client-ts's `new ZeBridge(opts)`, with the same default. An app passes what is about the app; what differs between platforms (storage, zstd, the NATS dial, crypto) the library decides — libzb by being native, zb-client-ts by its platform entry, which the bundler picks from package.json `exports` (`node`, `browser`, `react-native`).

| key | libzb (`opts_json`) | zb-client-ts (`ZeBridgeConfig`) |
| --- | --- | --- |
| `bridgeUrl` | ✅ where `invite` is redeemed (`/enroll`) and the JWT renewed (`/renew`); kept in the identity | ✅ the same, and where the grammar hash is fetched when `grammarHash` is unset |
| `invite` | ✅ first run only: with `bridgeUrl` and no stored identity, enrolls this device | ✅ the same |
| `identityPath` | ✅ where the identity is kept (JSON, mode 0600); default `<dbPath>.identity`, else `zebridge.identity` | ✅ the same file on Node; a localStorage key in the browser; the app's SQLite store on React Native |
| `natsUrl` | ✅ optional once enrolled: the identity carries it | ✅ the same (the identity's `nats_ws_url` in the browser and on React Native) |
| `creds` | ✅ the .creds TEXT (nats.zig patch 19); wins over the identity | ✅ |
| `credsPath` | ✅ a .creds file | ✅ where there is a filesystem (Node) |
| `principal` | ✅ optional: the creds or the identity name it | ✅ the same (also the inbox prefix on both) |
| `password` | — (creds only) | ✅ dev shape, user/password |
| `grammarHash` | ✅ refuses to open on a mismatch | ✅ |
| `tables`, `ondemandTables` | ✅ list or `"*"` | ✅ the same rule |
| `dbPath` | ✅ SQLite or DuckDB file | ✅ a file (Node, React Native) or an OPFS name (browser) |
| — default | `zebridge_<principal>.sqlite3`, kept across runs | the same |
| `dbUrl` | ✅ a PostgreSQL replica | — |
| `engine` | `sqlite` \| `duckdb` | `sqlite` \| `pglite` (browser, Node) |
| `clientId` | ✅ the msg_id prefix and tiebreak value; default `c-<random>` | the same |
| `heartbeatMs` | ✅ (0 = off) | ✅ |
| `seedChunkRows` | ✅ | ✅ |
| `seedStreaming`, `seedStreamingAboveBytes` | ✅ | ✅ |
| `jsDomain` | ✅ | ✅ |
| `caFile` | ✅ a PEM bundle of trusted roots for `https://` enrollment and renewal and a `tls://` NATS URL, in place of the system's. Required on iOS, where Zig cannot read the system's trust store | — (the platform's TLS: browser, Node, React Native) |
| `bulkCdc`, `bulkStatement`, `cdcBatchEvents` | — (the DuckDB path is always bulk, SQLite per event) | ✅ tuning |
| `platform` | — (one build per platform) | asserts the entry the bundler picked |
| `storage`, `transport`, `connect`, `zstdDecompress`, `zstdDecompressStream`, `zstdCompress` | — | overrides, for a test or a storage of your own; an app never needs them |

## Pinned by fixtures — identical by construction

One conformance suite, `zb-client-ts/fixtures/core-fixtures.json`, drives both cores (`core.ts` through `core.test.ts`, `core.zig` through `libzb/python/runner.py`). A rule in a fixture group cannot diverge without a test failing on one side.
The groups, 39 today: seedGate, chainPlan, fullPredates, scope, position, caughtUp, fkKind, pgTsToWire, lsnToNumber, keyChange, upsert, delete, chainUpsert, chainRowParams, cdcBulk, columnDdl, fkClauses, createTable, rebuildSteps, diffColumns, fkDiffer, viewSteps, indexPlan, nextVersion, subjectSafe, envelope, normalizeVersion, hlcVersion, outboxWatermark, tombstoned, update, exists, pgArrayLiteral, heartbeat, shape, retyped, readOnlySql, tableSet, mergeRegisters.

Everything below the core — the shells — is where parity is by hand, and where this document earns its place.

## Parity matrix

✓ same behaviour · ≠ different · — not applicable.

| contract | libzb | zb-client-ts |
| --- | --- | --- |
| enrollment from an invite: the key pair made on the device, the seed never sent, `/enroll` redeemed, the identity stored | ✓ | ✓ |
| one identity format: a file libzb writes, zb-client-ts on Node reads, and back | ✓ | ✓ |
| https enrollment and renewal | ✓, except iOS: Zig cannot read the system trust store, so the app enrolls itself and passes `creds` | ✓ |
| JWT renewal: checked every eighth of the JWT's life (at most every 60 s), renewed with a quarter left, by signing with the device's key (`/renew`); the next reconnect uses the new JWT. Timed and stamped on the bridge's clock (`clock_offset` in the identity, taken from each JWT's `iat`); a stamp the bridge refuses comes back with its time and is retried once; a JWT that NATS refuses is renewed at once, whatever the clock says. libzb counts from its last JWT on a clock that keeps running while the phone sleeps and that the phone's settings cannot move, and renews at open when the stored offset is over 60 s; zb-client-ts counts on the device's clock | ✓ in `poll` | ✓ on a timer |
| purge on revocation (`bridge --revoke --purge`): the ban's `"purge": true`, or `/renew`'s 403 with it, deletes the replica and the stored identity | ✓ in the C layer after `poll` / `sync`; `zb_client_revoked` still answers 1, every other call `Revoked` | ✓ `purged` set, the replica deleted, the identity emptied |
| seed gate by stream seq, never by LSN | ✓ | ✓ |
| on-demand tables: schema followed, nothing seeded or tailed, rows from `request` + `ingest` | ✓ | ✓ |
| seeding scoped to gapped streams, shared route included | ✓ | ✓ |
| a chain older than the replica's position is refused | ✓ | ✓ |
| seed with foreign keys off, on again after | ✓ | ✓ |
| consumer floor from the seed's cutoff, not the tail | ✓ | ✓ |
| chain-orphan check (cutoff vs stream first seq) | ✗ | ✗ |
| a table with no chain yet | re-asks at every poll until seeded | the table stays registered and marked unseeded, its events held in the inbox; asked again every 15 s for as long as the connection lives |
| position persisted per batch, held and gated events included | ✓ | ✓ |
| quiet stream: stored 0 takes the tail | ✓ | ✓ |
| position beyond the stream's last seq = restart, reset | ✓ | ✓ |
| table existence decided from the database, not memory | ✓ | ✓ |
| rebuild copies data with foreign keys off | ✓ | ✓ |
| indexes dropped before ALTERs, synced on every outcome | ✓ | ✓ |
| schema changes applied | live, a KV watch drained at every poll | live, a KV watch |
| shape record: a re-key rebuilds empty + re-seeds, a re-type alters or rebuilds keeping rows | ✓ `_zbz_shape` | ✓ `_zebridge_shape` |
| a rebuild that cannot carry the rows degrades to emptied + re-seed | ✓ | ✓ |
| descriptor epoch above the stored one → watermark dropped, re-seed; a manifest below the descriptor's epoch refused | ✓ | ✓ |
| a chain object naming a column the replica lacks | wait for the next full | wait for the next full |
| suspended descriptor | table skipped this sync | table skipped, surfaced to the app |
| drop tombstone → local table dropped | ✓ | ✓ |
| CDC batch in one transaction, position in it | ✓ | ✓ |
| child before parent: held durably, retried, pruned by a seed past it or a drop | ✓ `_zbz_inbox` | ✓ `_zebridge_inbox` |
| foreign keys deferred inside a batch, isolated replay with holds when COMMIT refuses | ✓ (2026-09-06) | ✓ `defer_foreign_keys` + `applyBatchIsolated` |
| held events discarded when their row's DELETE goes by; multi-level holds released to a fixpoint | ✓ | ✓ |
| cascaded deletes idempotent | ✓ | ✓ |
| the duty outlives the instrument (tail, status watch) | reopen-then-swap; reopen from the stored position on a consumer death | tail loop with idle guard; status loop recreated until close |
| outbox durable, optimistic apply + queue in one transaction | ✓ | ✓ |
| version stamped by the client (HLC) | ✓ | ✓ |
| verdicts: accepted / stale / rejected / row_deleted, two revert targets | ✓ | ✓ |
| a stale UPDATE rebased onto the winning row when the columns are disjoint, dropped and surfaced when they overlap; the winner before or after the verdict, a slow clock | ✓ `mutate_at` stamps a write explicitly | ✓ `mutate(…, { version })` |
| a write with no socket queues in the outbox and goes out on the next connect | ✓ (the host's flush) | ✓ (was a silent return) |
| the CDC echo that confirms a write carries the write's own stamp — another client's row on the same key is not our echo | — (settles on verdicts only) | ✓ (was by key alone) |
| an UPDATE that changes a key column is refused before it is queued (`KeyChange`); rename = delete + create | ✓ core | ✓ core |
| the host can see refusals: cumulative verdict counts by status | ✓ `flush` report `verdicts{…}`; refusals printed with reason and detail | ✓ per-verdict log lines |
| the optimistic row carries the write's own stamp in the version column | ✓ | ✓ |
| `row_deleted` removes the local row (the server's word is "deleted"); `rejected` restores the before-image | ✓ | ✓ |
| the wire grammar compiled in; `grammarHash` at open refuses a fork; a bridge that cannot be reached does not block opening | ✓ `@embedFile`, `zb_grammar_hash()` | ✓ packaged copy, `grammarHashHex()`, pinned by test |
| missed verdicts recovered by direct get | ✓ | ✓ |
| a chain object past 2 MiB (a full with tombstones) | ✓ own chunk reader | ✓ pull-consumer reader with the object's SHA-256 checked (the object store's push reader stalls at 2 MiB, @nats-io/obj 3.4.0) |
| one seed per table at a time — a second request joins the one in flight | — (one sync path) | ✓ (was twice at first sight) |
| the gap rule LIVE: a delivered sequence beyond stored + 1 is a hole the stream pruned under the consumer — re-seed at once, never read past it | ✓ per poll (was connect-time only) | ✓ per delivery (was connect-time only) |
| a chain whose cutoff fell off the stream is refused (`predates the stream`), the next generation awaited | ✓ | ✓ |
| outbox watermark gate before a flush | ✓ | ✓ |
| a failed optimistic echo still queues the write | ✓ | ✓ |
| liveness of the NATS connection | host-driven (`NoResponders` → reopen tails) | RTT poll every 10 s, re-sync on recovery |
| fleet heartbeat (PROTOCOL §11): a core publish, through `$JS.<domain>.API.` with a domain | on every poll and at sync end | from tenant resolution, before the seed |
| a command from another thread ends the poll's wait | `zb_client_wake(h)`, made by the bindings with every command | — (self-driven) |
| auth error named | ✓ `AuthorizationViolation` / `AuthExpired` from `poll` | ✓ the server's `error` status and `closed()`'s reason logged by name (2026-09-06) |
| the ban (`mutation_ack.<p>.revoked`): hang up now, stay hung up on reconnect, every call answers Revoked | ✓ `error.Revoked` | ✓ `revoked`, logged, closed |
| the wipe is explicit, never automatic | ✓ `zb_client_wipe` | ✓ `wipe()` |
| a principal with no tenant mapping (revoked, never enrolled) | tenant-scoped tables skipped audibly, public followed | same; a purged mapping's DEL marker reads as none |
| tenant revoked while connected | next connect | next connect |
| inbox pruning | ✓ (`_zbz_inbox`, pruned at the seed's lsn) | ✓ |
| zstd chain objects | built in | Node built in; browser needs `zstdDecompress` |
| a chain step's apply | a msgpack cursor, rows sorted by key, transactions of `seedChunkRows` (50,000), bound straight from the payload, a 128 MB page cache while the seed lasts; 3 M rows in 11 s | the same sort, chunks and page cache over the decoded document; on SQLite a chunk is one statement through `json_each`, row by row for a table with a BLOB and on PGlite; 3 M rows in 26 s |
| a `rate_limited` verdict (`failed`, `retry_after_ms`) | kept in the outbox, flushes held until the time has passed; counted as `rate_limited` on the flush report | kept, flushes held the same way |
| STRICT tables | every synced table is `CREATE TABLE … STRICT`; a replica from before is rebuilt once with the rows cast to the declared types | same, on the sqlite dialect (PGlite types its own columns) |
| the streaming seed (`seedStreaming`) | opt-in: the object read through a pull consumer eight chunks at a time and inflated through a window; on SQLite the rows are staged in a temp table, one index build sorts them on disk, pages come back in key order; 3 M rows in 23 s at 329 MB peak (the whole-object path: 11 s, 1.1 GB); a step below `seedStreamingAboveBytes` compressed (8 MiB) takes the whole-object path, so a delta never streams | not built: the browser and Node hold the document (a phone runs libzb) |
| arrays | JSON text as the wire carries it (`json_extract` reads it); a local write stores JSON text too | SQLite: same; PGlite: the JSON text becomes the array literal on apply (`pgArrayValues`), native arrays in the replica |
| bytes (`bytea`, PostGIS) | BLOB; the host sees and sends `{"$bin": "<base64>"}` on the JSON card | BLOB; `Uint8Array` in and out (the Node example prints the same `$bin` marker) |
| pgvector (`vector`, `halfvec`, `sparsevec`), `bit(n)` | BLOB in the wire's normalised shape (sqlite-vec reads it); on the PostgreSQL engine, pgvector's text form on every apply (`core.vecLiteral`); on DuckDB `vector`/`halfvec` are `FLOAT[n]` from the list text, `bit` is `BIT`, `sparsevec` stays the BLOB | BLOB (`Uint8Array`); on the postgres dialect, the text form on apply (`vecLiteral`) |
| survives its host killed mid-seed | proven (`client_kill.py`) | not tested |
| killed mid-migration: first sight drops the watermark, a half-finished rebuild is adopted, no shape record → the physical key decides | ✓ | ✓ |
| `query()` cannot write — the bookkeeping is out of the application's reach | ✓ a READONLY SQLite connection | ✓ a read-only connection on Node; `core.isReadOnlySql` where there is one handle (OPFS, PGlite) |

## Divergences that matter, ranked

1. ~~libzb loses held events with its host~~ — closed 2026-09-06: `_zbz_inbox`,
   written in the batch transaction, retried from the table, pruned by a seed past
   it or a drop. Parity with the TS inbox.
2. ~~Migrations reach libzb late~~ — closed 2026-09-06: libzb drains a watch on the
   schemas bucket at the top of every poll; both clients now walk every
   migration shape of `MIGRATIONS.md` side by side (`scripts/scenarios/migrate_both.py`).
3. ~~The TypeScript client follows every schema key~~ — closed: both clients
   follow `tables` (a list, or `"*"`) and nothing else.
4. ~~Missing chain: retry or exclude~~ — closed 2026-09-11: the TypeScript client
   keeps the table, holds its events (bounded) and asks for the chain without a deadline;
   the late seed replays what was held. Measured live on a table enabled between two ticks.
5. **Chain-orphan check** is built in neither. Retention ≥ cadence keeps it rare, not
   impossible.
6. **The TypeScript client follows one tenant.** A principal in several tenants gets the
   first one only, with a warning in the log; libzb follows every membership and can
   join or leave at runtime.
7. **libzb cannot enroll over https on iOS.** Everywhere else the first ten lines are the
   whole setup; on iOS with an https bridge, the app redeems the invite itself and passes
   `creds`.

## What is tested, per client

Both clients walk every migration shape together in `migrate_both` (owns).
libzb carries the chaos program: `matrix`, `churn`, `cascade`, `client_gap`,
`shared_gap`, `client_kill`, `chain_kill`, `jwt_expiry`, `clockskew`, `fleet`,
`grammar_served`, `leaksoak`, and the swarm's Python workers. Enrollment is in the
battery through `jwt_expiry` (an invite redeemed with a tiny TTL), and renewal through
`jwt_renew` (both clients hold 40 s JWTs across 100 s, and recover from a clock estimate
2 h off either way). Real iPhone JWT renewal testing: `scripts/phone/renew`, libzb on an
iPhone 12 with 5-minute JWTs, the phone's clock moved by hand while the app ran and while
it was closed. The TypeScript client
is exercised by `objstore_race`, the swarm's Node/PGlite workers, the
Node consumer example, and the browser by hand. Nothing kills a TypeScript host
mid-seed, nothing migrates a table under a TypeScript client in the battery, and the
browser path has no automated run at all.
