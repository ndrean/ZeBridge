# Test scenarios

Exercises the `PROTOCOL.md` contract end to end: Postgres → bridge → NATS →
`web-consumer` (WASM SQLite).

**Roles.** The Elixir `emitter/` is the *admin*: it is the only component with a
direct Postgres connection, so migrations and load generation both live there. The
`web-consumer` is a pure protocol client — it touches nothing but NATS. That split is
the privilege boundary, and keeping it honest is itself part of the test.

## ⚠️ Half of this is blocked on client work

`web-consumer` today **logs** INIT messages but never applies them, keeps no
`last_applied_lsn`, and subscribes with core NATS (no replay). So it cannot yet
resume, detect a gap, or seed from a snapshot.

| group | runnable now |
| --- | --- |
| **A** schema & CDC | ✅ yes |
| **B** bootstrap / resume / gap | ❌ needs client item **D** |
| **C** snapshot cache & windows | ⚠️ bridge side only, via `nats` CLI |
| **D** offline schema changes | ⚠️ partly — see notes |
| **E** error paths | ✅ yes |
| **F** suspension lifecycle | ✅ yes — verified end to end |
| **G** binary COPY & exotic types | ✅ yes — verified end to end |

Running a B scenario before D is done looks like success (no error appears) while
nothing is actually verified. Do not tick those off early.

---

## Setup

```bash
# fresh everything
docker compose -f docker-compose.full.yml down -v --remove-orphans
docker compose -f docker-compose.full.yml up -d \
  postgres-primary nats-config-gen nats-server nats-init bridge-init

# admin: create tables + publication membership
cd emitter && mix run --no-start -e 'path = Path.join([File.cwd!(),"priv","repo","migrations"]); \
  {:ok,_,_} = Ecto.Migrator.with_repo(Emitter.Producer.Repo, fn r -> Ecto.Migrator.run(r, path, :up, all: true) end)'

# bridge
set -a && source .env && set +a
./zig-out/bin/bridge --slot my_slot --pub my_pub --port 9090

# client
cd web-consumer && npm run dev      # http://localhost:5173
```

### Knobs for forcing edge cases

```bash
# shrink the CDC window so a "long absence" takes seconds, not a day
nats stream edit CDC --max-age=30s   --server=... --nkey=...

# shrink the snapshot freshness window
SNAP_RET_SECONDS=20 ./zig-out/bin/bridge ...

# inspect
nats stream info CDC | grep -E "first_seq|last_seq|messages"
nats stream subjects INIT
nats kv get schemas users --raw | python3 -m json.tool
```

⚠️ Keep the invariant in view while tuning: **`CDC_RET > SNAP_RET + apply time`**.
Scenario C3 deliberately violates it — that is the point of C3.

---

## A. Schema and CDC — runnable now

| # | setup | expect |
| --- | --- | --- |
| A1 | Fresh stack, no tables yet, open client | KV empty → no tables created locally. Bridge logs *0 boot schemas* (only `zebridge_ddl_events`, skipped as internal). |
| A2 | Run migrator with client open | Two `MIGRATE` entries (`users`, `test_types`) pushed by `kv.watch()`, no button press. |
| A3 | `Emitter.Produce.stream(50,200,5)` | Rows appear locally. Row counts match Postgres. |
| A4 | INSERT+UPDATE+DELETE in one txn | Three subjects, **not** one batch. `cdc.users.delete` present. |
| A5 | `Emitter.Chaos.trigger_schema_evolution()` | `HOLD` → `MIGRATE +[kyc_status] via ALTER` → `DRAIN`. **Row count survives.** |
| A6 | `ALTER TABLE users DROP COLUMN kyc_status` | `-[kyc_status] via ALTER`, rows still there. Not a rebuild. |
| A7 | `Emitter.Chaos.trigger_table_drop()` | Tombstone → local table dropped. |
| A8 | `CREATE INDEX` on a published table | **Nothing** reaches the client — indexes are not schema. |

## B. Bootstrap, resume, gap — needs client item D

| # | setup | expect |
| --- | --- | --- |
| B1 | Fresh client, tables **empty**, no snapshot | Schema only. Nothing to seed. |
| B2 | Tables have 1000 rows, fresh client, no snapshot | Client requests a snapshot, seeds, then follows CDC. Local count = 1000. |
| B3 | As B2, but a **fresh snapshot already exists** | Client seeds from the cached one. Bridge logs *Reusing snapshot* — **no new COPY**. |
| B4 | Client connected, disconnect ~5 s, reconnect | ⭐ **Resumes.** No snapshot request, no chunks, no truncate. The most important case: a phone blip must not redownload the table. |
| B5 | Disconnect, `nats stream purge CDC`, reconnect | Gap detected → snapshot → truncate → seed. |
| B6 | Disconnect, insert 100 rows, reconnect inside window | Resumes and applies exactly the 100 missed rows. No re-seed. |

## C. Snapshot cache and retention windows

C1–C2 are exercisable now with the `nats` CLI; C3 needs a client to observe the
failure it is designed to prevent.

| # | setup | expect |
| --- | --- | --- |
| C1 | Two `snapshot.request.users` within `SNAP_RET` | One `COPY`. Second logs *Reusing snapshot*, and `init.snap.meta.users` count increments (the requester is still answered). ✅ verified |
| C2 | Request, wait past `SNAP_RET`, request again | Two `COPY`s, new `snapshot_id`. |
| C3 | `SNAP_RET=60s`, `CDC max-age=30s`. Snapshot, wait 40 s, connect a fresh client | ⭐ Snapshot is *fresh* but **older than the CDC window**. Client must reject it and request a new one. Skipping this check is the silent-divergence bug. |
| C4 | Two clients reconnect simultaneously, no snapshot | One `COPY`, both served. |

## D. Schema changes while disconnected

D1 is runnable (drive it with the `nats` CLI and inspect KV); the rest need client D.

| # | setup | expect |
| --- | --- | --- |
| D1 | Disconnect, `ALTER ... ADD COLUMN`, reconnect | KV holds the new schema. Client migrates, then replays old-shape rows — they apply fine (missing column takes NULL). |
| D2 | Disconnect, **two** ALTERs (add then drop), reconnect | ⭐ KV holds only the **latest**. Intermediate events reference a dropped column → `event.lsn < schema.lsn` → **STRIP**, not hold. Holding here deadlocks: no newer schema is coming. |
| D3 | Disconnect, `DROP TABLE`, reconnect | Tombstone still in KV (not a deleted key) → local table dropped. |
| D4 | Disconnect, `RENAME COLUMN`, reconnect | Reads as drop+add → rebuild preserving common columns. The renamed column's values are lost — known gap (`NOTES.md` §1.2). |

## E. Error paths — runnable now

| # | setup | expect |
| --- | --- | --- |
| E1 | `nats pub snapshot.request.nope ''` | `init.snap.error.nope` with `error_type: table_not_monitored` + `available_tables`. ✅ verified |
| E2 | Create a table, add to publication **after** bridge start, request snapshot | ⚠️ **Currently fails**: `monitored_tables` is boot-time, so it is rejected with a misleading `available_tables`. Fixed by the pending publication refresh. |
| E3 | `REVOKE SELECT ON users FROM bridge_reader`, request snapshot | `init.snap.error.users` with `error_type: generation_failed`. |
| E4 | Stop NATS mid-load, restart | Bridge backpressures, WAL accumulates, then drains. No loss. |

---

## F. Suspension lifecycle — runnable now ✅ verified

The one group that needs no client work beyond what is built, and the only one that
exercises refusal, suspension, and recovery end to end. Driven by `Emitter.Scenario`,
which runs migrations **one at a time** (`Ecto.Migrator.up/4` by version) so you can
watch the bridge and the consumer react between steps.

```bash
docker compose -f docker-compose.full.yml down -v --remove-orphans
docker compose -f docker-compose.full.yml up -d \
  postgres-primary nats-config-gen nats-server nats-init bridge-init

set -a && source .env && set +a
./zig-out/bin/bridge --slot my_slot --pub my_pub --port 9090   # keep visible
cd web-consumer && npm run dev                                  # keep open

cd emitter
mix run --no-start -e 'Emitter.Scenario.reset()'    # only if not starting fresh
```

Then one step per command, checking the middle column before moving on:

| # | command | bridge | consumer |
| --- | --- | --- | --- |
| F1 | `Emitter.Scenario.step1()` | 🔴 `REFUSING 'users_no_pk'` → `$KV.schemas.users_no_pk` gets a **suspension** | suspension banner; **no table created** |
| F2 | `Emitter.Scenario.step2()` | ✅ `users`, `test_types` schemas published | both tables created |
| F3 | `Emitter.Scenario.step3()` | `cdc_events_published` +15 | 15 rows in `users` |
| F4 | `Emitter.Scenario.step4()` | 🔴 `REFUSING 'users'` mid-stream → suspension **overwrites** the live schema | ⭐ banner appears, **rows stay** — not dropped |
| F5 | `Emitter.Scenario.step4b()` | `cdc_events_published` **unchanged**; `refused_events_dropped` +10 | ⭐ row count **does not move** while Postgres reaches 25 |
| F6 | `Emitter.Scenario.step5()` | ✅ `no longer refused`; no composite warning — the key is ordinary now | banner clears; table rebuilt with `PRIMARY KEY ("name","email")` |
| F7 | `Emitter.Scenario.step6()` | `cdc_events_published` +10 | rows flow again, keyed on `(name, email)` |

Verified run: `cdc_published 15 → 15 → 25`, `refused_events_dropped 0 → 10 → 10`,
`users` 15 → 25 → 35 in Postgres.

**F4 and F5 are the two that matter.** F4 is the difference between suspension and a
tombstone: a client that drops its table here has lost data over a migration mistake
that F6 undoes seconds later. F5 is the proof that refused events are *dropped*, not
buffered — if the consumer catches up later, the bridge is queueing events for a table
whose schema it withheld.

### Two things this scenario exposes on purpose

**PostgreSQL rejects the clean-up in step 5.** `UPDATE` on a keyless table that
publishes updates fails with `55000 cannot update table "users" because it does not
have a replica identity and publishes updates` — preflight's `writes_rejected` finding,
hit for real. The migration lifts `REPLICA IDENTITY FULL` first and restores `DEFAULT`
after the key exists.

**The dedupe DELETEs in step 5 are invisible to the consumer.** They run while `users`
is still refused, so their events are dropped and the consumer keeps rows that no
longer exist upstream. That is the suspension contract working as designed — local data
is frozen at the suspension LSN, not kept in sync — and it is why a client should
re-seed from a snapshot after a suspension rather than trusting its frozen copy. F6
should end with a re-seed: snapshots page on composite keys now, so the only thing
still missing is the client's ability to apply one.

---

## G. Binary COPY and exotic types — runnable now ✅ verified

Continues the `Emitter.Scenario` steps. Needs no client work: the assertions are the
bridge log and the chunk payload read with the `nats` CLI.

```bash
cd emitter
mix run --no-start -e 'Emitter.Scenario.step7()'   # ENUM + HSTORE + NUMERIC + TEXT[]
mix run --no-start -e 'Emitter.Scenario.step8()'   # populate, and print PG's own text
```

`step8` prints exactly what PostgreSQL renders, which is the reference to diff against:

```
[1, "happy", "\"a\"=>\"1\", \"b\"=>\"has, comma\"", "0.10000000", "{x,\"y,z\"}"]
[2, "sad",   "\"k\"=>\"v\"",                          "12345.67890000", "{solo}"]
[3, "ok",    nil, nil, nil]
```

| # | action | expect |
| --- | --- | --- |
| G1 | `nats pub snapshot.request.exotic_types ''` | 🔴 **refused**: `column 'attrs' has unsupported type OID <n> (typtype 'b')`. ✅ verified |
| G2 | Same, after `ALTER TABLE exotic_types DROP COLUMN attrs` | ✅ succeeds — the ENUM passes through as its label (`happy`/`sad`/`ok`, exact). ✅ verified |
| G3 | Read the chunk, check `price` | `0.10000000` / `12345.67890000` — byte-identical to PG text. The `atttypmod` padding. ✅ verified |
| G4 | Snapshot `users` (composite PK, after F5) | ✅ succeeds — `paginating on a 2-column primary key`; the emitted key sequence is identical to `SELECT name,email FROM users ORDER BY name,email`. ✅ verified 2026-08-15 |
| G5 | Snapshot a table with a comma or newline inside a `TEXT` value | Value intact. Under CSV this split the row and shifted every later column. ✅ verified on `test_types` |

**G4 needs volume to mean anything.** As the scenario leaves it, `users` has 15 rows —
one chunk, and pagination bugs only exist at chunk *boundaries*. Padding it to 12 515
rows over 6 repeated `name` values puts the boundary inside a run of equal names, on a
`varchar(255)` composite key under a non-C collation (`User-*` sorts *after* `n-*`
there, so byte order is not the ordering being tested):

```sql
INSERT INTO public.users (name, email, inserted_at, updated_at)
SELECT 'n-' || lpad(((i-1)/2000)::text, 3, '0'),
       'e-' || lpad(i::text, 6, '0') || '@example.com', now(), now()
FROM generate_series(1, 12000) i;
```

Chunk 0 ended at `(n-004, e-009500@…)` and chunk 1 resumed at `(n-004, e-009501@…)`,
and the 12 515 emitted keys were **identical** to PostgreSQL's own
`ORDER BY name, email` — the strongest form of this check, since it compares against
the server's collation rather than the client's idea of ordering.

The same was verified on a synthetic int+text key, which is where the numbers below
come from: 25 000 rows over `PRIMARY KEY (tenant, name)` with 3 000 names repeated in
every tenant, every name carrying a `'` so each cursor literal exercises quote
escaping.

```sql
CREATE TABLE public.ck (tenant int NOT NULL, name text NOT NULL, payload text,
                        PRIMARY KEY (tenant, name));
INSERT INTO public.ck (tenant, name, payload)
SELECT (i-1)/3000, 'u''-' || lpad(((i-1) % 3000)::text, 5, '0'), 'p' || i
FROM generate_series(1, 25000) i;
```

Result: chunks of 10 000 / 10 000 / 5 000, chunk 0 ending at `(3, u'-00999)` and
chunk 1 resuming at `(3, u'-01000)` — 25 000 rows, 25 000 distinct keys, globally
ascending.

**G1 is the one that matters.** Before the `typtype` guard, this snapshot *succeeded*
and shipped hstore's binary wire form as a string — `\0\0\0\1\0\0\0\1k\0\0\0\1v` —
behind a single `log.warn`. A green log line was not evidence of a correct snapshot,
which is the reason this group exists.

### Not yet asserted here

- **Array quoting.** Confirmed systematic across two tables: the bridge writes
  `{"x","y,z"}` and `{"solo"}` where Postgres writes `{x,"y,z"}` and `{solo}`. Both are
  valid array literals and parse identically, but a golden test asserting byte equality
  with text `COPY` will fail until this is settled (`COPY_BINARY_PLAN.md` §F).
- **Client-visible errors.** G1 and G4 abort server-side without publishing to
  `init.snap.error.<table>` (`PROTOCOL.md` §6). The client sees silence, not a reason.
- **CDC with an exotic column.** ✅ Landed and verified 2026-08-15
  (`COPY_BINARY_PLAN.md` §E): an INSERT into a table with an `hstore` column now
  suspends it with `reason: "unsupported_column_type"` instead of shipping the binary
  wire form as a string. Worth folding into this group as G6 with the enum control
  (`feeling mood` decodes to its label, `attrs hstore` refuses).
- **Cross-version.** The decoder deserves a golden-value run against several PG majors.
  Note `pgoutput`'s `binary` option needs **PG 14+**, so the bridge will not start on
  older servers — but `COPY ... FORMAT binary` works back to 7.4, so the decoder alone
  can be exercised much further back.

---

## H. Registry invalidation — runnable now ✅ verified

The bridge keeps caches of PostgreSQL catalog facts: `relation_map`, `schema_cache`,
`refused_tables`, `type_registry`. None is durable — every one is rebuilt at boot — but
each needs an explicit invalidation rule, and a missing rule shows up as the bridge
reporting a world that no longer exists.

This group exists because one rule *was* missing: a refused table that got **dropped**
stayed in the registry. `logStatus` re-announced it every 15 s for the life of the
process, and `bridge_refused_tables` never returned to 0 — so an alert on it could
never clear, which is the failure that matters operationally.

(A table recreated with the same name *and* a primary key does clear itself, because
its `CREATE TABLE` DDL event carries a valid schema and takes the `refused.clear` path.
The registry keys on name and names are reusable, so that recovery is luck of ordering
rather than design — another reason the entry should not survive the drop.)

Run with the bridge up throughout. Nothing here needs a client.

| # | action | expect |
| --- | --- | --- |
| H1 | let the emitter run **all** migrations in version order on boot (`iex -S mix`) | `🔴 REFUSING 'users_no_pk'` at its CREATE; `users` refused by the drop-pk migration, still refused across the `REPLICA IDENTITY FULL` step, then `✅ 'users' is no longer refused (was: no_primary_key)` when the composite key lands |
| H2 | wait two metrics intervals | the reminder repeats for `users_no_pk` — it still exists and still has no key |
| H3 | `Emitter.Scenario.reset()` | `✅ 'users_no_pk' is no longer refused (was: no_primary_key)` + a tombstone for every dropped table. ⭐ **the reminder stops** |
| H4 | `Emitter.Scenario.step1()` | `🔴 REFUSING 'users_no_pk'` again, reminder resumes — a recreated table is refused on its own merits, not remembered |
| H5 | `Emitter.Scenario.reset()` | refusal lifts again |
| H6 | `Emitter.Scenario.step2()` | `users` and `test_types` publish live schemas; **no refusals**, and none reappear |
| H7 | `curl -s localhost:9090/metrics \| grep refused_tables` | `bridge_refused_tables 0` |

**H3 is the one that matters.** Before the fix it failed silently — the log kept naming
a table that `psql \dt` no longer showed, which is exactly how a stale cache announces
itself. Verified 2026-08-15 across a full drop → recreate → drop → recreate cycle, with
the bridge running the whole time and the steady state clean for 100 s afterwards.

The KV payload sizes make the transitions readable without decoding anything:

```txt
📤 Published KV schema: 542 bytes to $KV.schemas.users        ← live schema
📤 Published KV schema:  75 bytes to $KV.schemas.users        ← suspension
📤 Published KV schema:  47 bytes to $KV.schemas.users        ← tombstone
```

### Not covered here

- **A drop while the bridge is down.** Preflight rebuilds the registry from the
  publication at boot, so a table dropped in the meantime is simply never refused. That
  is correct by construction rather than by rule, and worth asserting once.
- **`type_registry`.** Deliberately never invalidated — a type's OID lives as long as
  the type, so a stale entry describes something that can no longer reach the wire.
  Drop and recreate an enum and the new one arrives with a new OID on a new DDL event;
  worth an H8 to pin that reasoning.

---

## I. Burst throughput — runnable now ✅ verified

**Not a trickle.** Every load test before 2026-08-15 paced small transactions
(`Produce.stream(5, 1000, 10)` = 10 rows per transaction every 5 ms), and every one of
them passed while a **quadratic bug in the WAL reader** capped bursts at ~1.2k msg/s.
The bug was invisible at a trickle by construction: its cost is proportional to how many
unread bytes sit in libpq's buffer, and a bridge that keeps up never accumulates any.

So this group exists to test the property a CDC bridge is actually for: **surviving a
burst it cannot keep up with**, and draining it afterwards.

| # | action | expect |
| --- | --- | --- |
| I1 | build `ReleaseFast` (`zig build -Doptimize=ReleaseFast`) | a Debug build is several times slower and will muddy every number below |
| I2 | 2000 × 1000-row `INSERT` in separate transactions (2M rows, see README "Measured throughput") | PostgreSQL absorbs it in ~6 s |
| I3 | watch the `LOOP` line while it drains | `idle` **falls toward 0** and `recv_ms` stays *small*; `cpu` well under 100%. ⭐ `recv_ms` climbing toward the 15 000 ms interval with `idle=0` and `cpu≈100%` is the failure signature |
| I4 | watch `cdc_events` in `METRICS` | reaches 2 000 000; on an M2 Pro, under 20 s from bridge start |
| I5 | `nats stream info CDC` | message count consistent with the batches; no publish errors in the log |
| I6 | after the drain | `idle` returns to ~11 900 per interval (the loop sleeps 1 ms when there is nothing to read) and `bridge_wal_confirmed_lag_bytes` falls back to ~0 |

**I3 is the whole point.** The other rows can pass while the bridge is 300× too slow —
`cdc_events` still climbs, `queue_usage_percent` still reads 0% (that gauge watches the
*flush* side, and the flush side was never the problem), and nothing errors. Only the
split between `recv_ms` and `proc_ms` distinguishes "reading is the bottleneck" from
"decoding is the bottleneck", which is why the numbers exist.

Reference figures from the run that fixed it — same machine, same load, only
`wal_stream.zig` changed:

| | before | after |
| --- | --- | --- |
| `recv_ms` / 15 000 | 14 884 (99.2%) | 130 (0.9%) |
| `proc_ms` | 112 | 1 191 |
| `idle` | 0 | 10 790 |
| throughput | ~1.2k msg/s | ~1.44M events in the first interval |
| bridge CPU (`cpu=` on the LOOP line) | 100% of a core | 31% while draining, 3% at rest |

### Variants worth running once

- **`REPLICA IDENTITY FULL`** on the burst table: every UPDATE then carries an old tuple
  as well, roughly doubling decode volume. `proc_ms` should rise, `recv_ms` should not.
- **Wide rows** — a `jsonb` document or a long `text` column per row, still under
  `BASE_BUF`. This moves the load from framing into decoding and encoding.
- **A row over `BASE_BUF`** mid-burst: the table must suspend (`row_too_large`, §H) and
  every *other* table must keep draining.
- **NATS stopped mid-burst**: the ring buffer fills, `queue_usage_percent` climbs to
  100%, the WAL loop back-pressures, and PostgreSQL retains WAL. On restart it must
  drain rather than lose events — this is the only test that exercises the buffer's
  actual purpose.

---

## Order

1. **A + E + H + I now** — they need no client work and cover the paths already built.
2. **C1, C2 now** via the CLI — bridge-side caching.
3. **Then D (client)**, and B, C3, C4, D2 become runnable.

B4 and D2 are the two that most deserve care: B4 is the difference between a
local-first app and a slow one, and D2 is the only case that can deadlock a client
permanently.
