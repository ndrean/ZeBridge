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
| F6 | `Emitter.Scenario.step5()` | ✅ `no longer refused` + ⚠️ composite snapshot warning | banner clears; table rebuilt with `PRIMARY KEY ("name","email")` |
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
re-seed from a snapshot after a suspension rather than trusting its frozen copy. Once
snapshots support composite keys, F6 should end with a re-seed.

---

## Order

1. **A + E now** — they need no client work and cover the paths already built.
2. **C1, C2 now** via the CLI — bridge-side caching.
3. **Then D (client)**, and B, C3, C4, D2 become runnable.

B4 and D2 are the two that most deserve care: B4 is the difference between a
local-first app and a slow one, and D2 is the only case that can deadlock a client
permanently.
