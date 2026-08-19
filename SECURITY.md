# Security model

What ZeBridge enforces, who has to configure it, and what breaks if they get it wrong.

⚠️ **Status is marked throughout.** ✅ built and tested. 🚧 designed, not built. A claim
without a mark is not a claim.

The one sentence the rest follows from:

> **A client never writes. It asks, and state arrives through CDC.**

The write path carries a *request*. PostgreSQL decides what happens to it. The replica is
a projection of what PostgreSQL actually did, so a refused write cannot produce state — no
phantom row to detect, no undo to apply. That holds against a hostile client, not just a
buggy one (PROTOCOL.md §7.0).

---

## Part 1 — The DBA

### 1.1 PostgreSQL: three roles, and why they are separate

| role | holds | must never have |
| --- | --- | --- |
| **`postgres`** (admin) | creates roles, runs `init.sql`, owns tables | to appear in `.env.bridge`. It is not a bridge credential |
| **`bridge_reader`** | `SELECT` + `REPLICATION` | any write privilege. It is *physically* unable to write |
| **`bridge_writer`** | per-table `SELECT, INSERT, UPDATE`, granted one table at a time | `BYPASSRLS`. That attribute is what makes RLS enforce writes |

There is **no tenant role.** A tenant is a column value, not a login. One writer role
serves every principal; who they are arrives as a session setting, and PostgreSQL resolves
the tenant from it. Adding a tenant is a row in a mapping table, never a `CREATE ROLE`.

✅ `scripts/scenarios/writable.py` — asserts the refusal *and* the SQLSTATE, so a verdict
naming the wrong reason fails the test.

✅ Row-level refusal verified end to end: the bridge stamps the authenticated principal
into the session (`set_config('zb.principal', $1, true)`, one mutation per transaction),
PostgreSQL resolves it to a tenant through `zebridge_user_tenants`, and a row belonging to
another tenant is refused — `42501 new row violates row-level security policy` — with the
verdict reaching the client immediately, since that SQLSTATE is classified permanent.

⚠️ Before this the bridge never set the session variable, so any policy reading it saw
NULL and refused **every** write. The design was validated in psql and inert in the
bridge — enabling RLS would have looked like the bridge breaking.

**Risk if wrong.** `bridge_writer` with `BYPASSRLS` silently disables every row policy —
writes still succeed, so nothing looks broken. `bridge_reader` with a write grant makes
"the read path cannot write" false while the logs stay identical. Neither failure is
visible at runtime; both are visible in `pg_roles`.

### 1.2 PostgreSQL: opening a table, and the only supported way

`bridge_writer` starts with **no table privileges**. Ingress is closed until a DBA opens a
table, so a new table is never silently writable.

```sql
SELECT zebridge_grant_edge_writes('public.orders');            -- writable from the edge
SELECT zebridge_publish_tenant_table('public.orders',
         'tenant_id', 'acme', 'my_pub');                       -- 🚧 tenant-scoped reads
```

⚠️ **Do not use a bare `ALTER PUBLICATION ... ADD TABLE`.** An event trigger refuses it
for a table that is neither scoped nor recorded in `zebridge_public_tables`. Publishing a
table with no row filter and no RLS sends every row to every subscriber, and nothing in the
bridge can detect that — it is a pass-through by design.

✅ Manual for the refusal itself (NOTES §1.8); ✅ `scripts/scenarios/render.py` proves the
guard **exists** after a render, which is the failure that actually happened — the trigger
vanished along with six functions when `envsubst` ate a dollar tag.

Deliberately public tables are recorded, with a reason and an author:

```sql
INSERT INTO zebridge_public_tables (tbl, reason)
VALUES ('public.currencies', 'ISO list — identical for every tenant');
```

`SELECT * FROM zebridge_audit_publications();` answers *is anything published without being
scoped?* — the invariant a pass-through bridge cannot check for itself.

### 1.3 PostgreSQL: what the schema must satisfy

A table is **readable** as it is. Being **writable** from the edge, or **tenant-scoped**,
constrains it:

| requirement | why | if missing |
| --- | --- | --- |
| primary key | rows must be identifiable; DELETE is otherwise ambiguous | table refused outright |
| key is `uuid`, minted by the client | a `bigserial` cannot be minted offline — two clients pick the same id and one row is silently overwritten | delayed duplicate-key collisions |
| version column that changes on every write | last-write-wins compares it | every mutation fails `NoVersionColumn` |
| tiebreak column (3rd `SYNC_RULES` field), if equal versions are possible | equal versions are otherwise **refused**, so two replicas holding different rows refuse each other forever | silent permanent divergence, no error anywhere |
| `timestamptz`, not `timestamp` | naive columns record no zone; a client writing local time wins or loses on its offset, silently | wrong write wins, no error |
| tombstone column (soft delete) | an offline client's queued edit is overruled instead of resurrecting the row | deletes are physical; offline edits resurrect rows |
| every `NOT NULL` column has a `DEFAULT`, or the client sends it | the schema descriptor carries no nullability | writes fail; the client learns only from the rejection |
| 🚧 tenant column inside the **replica identity** | a DELETE carries only the replica identity — a tenant outside it means deletes cannot be routed | ✅ preflight refuses the table |

The tenant requirement is two statements, and does **not** need a primary-key change:

```sql
CREATE UNIQUE INDEX orders_zb_ri ON orders (tenant_id, uid);
ALTER TABLE orders REPLICA IDENTITY USING INDEX orders_zb_ri;
```

⚠️ Dropping that index drops the replica identity with it, and `UPDATE`/`DELETE` then fail
with an error naming something else entirely.

### 1.4 PostgreSQL: after every migration

1. **Open new tables explicitly** — `zebridge_grant_edge_writes` / `zebridge_publish_tenant_table`. A
   bare publication add is refused, which is the reminder.
2. **Re-run the audit** — `SELECT * FROM zebridge_audit_publications();`
3. 🚧 **Re-`CLUSTER` tenant-scoped tables.** Locality decays: new rows land wherever there
   is space, so tenants interleave again. ✅ Measured on 200k rows / 20 tenants: one
   tenant's snapshot read **6,250 blocks** interleaved versus **313** after
   `CLUSTER t USING t_zb_ri` — the difference between N snapshots costing N table scans
   and costing one. `CLUSTER` takes an `ACCESS EXCLUSIVE` lock and is not maintained;
   partitioning by tenant gives the same locality permanently.
4. **Check preflight at the next boot.** It reports grants that the schema cannot honour,
   version columns that are naive, and tenant columns outside the replica identity.

### 1.5 PostgreSQL: maintenance — tombstone GC, and why

A soft delete leaves the row with its tombstone set so a late edit from an offline client
is overruled rather than resurrecting it. Tombstones must eventually be reaped, and
`GC_THRESHOLD_MS` is therefore **the maximum offline window this deployment supports**: a
client offline longer than that can resurrect a row, because the tombstone that would have
overruled it is gone.

The sweeper (`zig-out/bin/bridge_sweeper`) runs as `bridge_writer`, never as the admin — a
process that deletes rows must not be able to delete anything it likes.

⚠️ **With RLS enabled the sweeper needs a policy of its own**, or it silently reaps
nothing: it is a background process acting for nobody, so `current_setting('zb.principal')`
is unset, the tenant predicate is NULL, and it sees **zero rows** — measured, 0 of 4. The
tombstones then accumulate forever and the GC watermark quietly stops holding.

```sql
CREATE POLICY zb_sweeper_all ON <table> FOR ALL TO bridge_writer
  USING (coalesce(current_setting('zb.principal', true), '') = '');
```

⚠️ `coalesce(..., '') = ''`, **not `IS NULL`**: `SET LOCAL` resets to the empty string at
COMMIT, so `IS NULL` holds only until the connection's first mutation — 2 rows visible
before, 0 after. It carries no `WITH CHECK`, so it grants no right to write into another
tenant; verified that a principal is still refused a foreign `tenant_id`.

⚠️ **The two failure directions are not symmetric.** Reaping too *little* grows the table:
annoying and obvious. Reaping too *early* removes the tombstone protecting a client that is
still inside its allowed offline window — nothing errors, and a deleted row comes back
weeks later. That asymmetry is why the cutoff is computed by `now()` on the **server**: a
sweeper whose host clock ran fast would reap early, which is the direction that loses data.
A clock running slow only delays the sweep, which costs disk and nothing else.

✅ The sweep set is derived from `SYNC_RULES` — the same variable the bridge reads — so the
two cannot disagree about which column is the tombstone. A table with no tombstone is not
swept: its deletes are physical and there is nothing to reap.

```
GC: sweeping test_types on tombstone column 'deleted_at'
GC: reaped 1 tombstone(s) from test_types older than 3600000ms
```

✅ Verified against real rows: a 2-hour-old tombstone reaped, a 1-minute-old one kept, a
live row untouched — so the threshold boundary holds, not just the delete.

⚠️ It previously deleted `WHERE _deleted = true AND _hlc < $1` — columns from an older
HLC design that **no table has**. The sweeper matched nothing, and the GC-watermark
guarantee was not enforced at all. Fixing it also required adding `DELETE` to
`zebridge_grant_edge_writes`: without it the sweeper failed `permission denied`, silently,
in a sidecar nobody reads.

⚠️ The session is pinned to `UTC` so a naive tombstone column is read as UTC rather than as
the server's default zone, and the cutoff is computed by `now()` **on the server** — a
sweeper with a drifted host clock would otherwise reap tombstones early, which is exactly
the window a client needs.

### 1.6 NATS: credentials and permissions

Two credential shapes, on purpose:

| | credential | permissions |
| --- | --- | --- |
| the bridge | nkey, seed passed on the command line, in no env file | `publish: >`, `subscribe: >` — it is the trusted writer and already holds replication rights |
| a client | user/password today, JWT next | allow-listed to its own subtree |

**The permission block is what makes the protocol true.** PROTOCOL.md §7.1 says a
principal is trustworthy because NATS authorises subjects. Delete the permissions and the
principal becomes a self-asserted string; the bridge cannot tell the difference and does
not try.

✅ `web-consumer/zb-probe.mjs` replays the client's startup against the real allow-list
and prints ok/FAIL per step — it is how the missing `$JS.API.INFO` and `CONSUMER.INFO.*`
grants were found. The refusals (`mutation.bob.…`, `cdc.>`, `$KV.>`, `MUTATIONS`, `INIT`
purge, `mutation_ack.bob.>`, forging a verdict) are manual, recorded in NOTES §1.8.

⚠️ **JetStream denials surface as client timeouts, not errors.** The server drops the
denied `$JS.API.…` publish and the caller waits out its own deadline. A too-narrow
allow-list looks like a hung broker.

⚠️ **A password in a browser bundle is enforced, not secret.** It authenticates the bundle,
not the person. That is why JWT is the endpoint (§2.4).

### 1.7 NATS: the subject invariant

> `<stream>.<identity>.<everything that can vary>`

The identity goes **immediately after the stream prefix**. Not for obscurity — a client can
name any subject and still be refused — but so that **one grant stays correct as the schema
changes**. `cdc.acme.>` covers every table, operation and suffix, including ones that do not
exist yet.

⚠️ Learned the hard way: with the identity *last*, batching appended `.batch` and turned
`cdc.orders.insert.acme` into a five-token subject that no tenant grant matched. A tenant's
rows arrived under light load and stopped under heavy load — a bug whose trigger is
*volume*. ✅ Fixed by putting the identity first, and confirmed with a subscriber holding
**only** `cdc.acme.>` receiving both the single and the batched event while never seeing
`globex`. ⚠️ **No scenario file** — the reproduction needs a burst large enough to batch.

Held today by `mutation.<principal>.…`, `mutation_ack.<principal>.…` and
🚧 `cdc.<tenant>.…`. Not yet by the snapshot, request and KV subjects.

⚠️ **Values interpolated into subjects are validated** (`utils.isSubjectToken`). A tenant
value is row data and can contain anything: a dot splits the token and the row vanishes
from its own tenant's feed while still existing in PostgreSQL — divergence no later event
can repair. ✅ Such rows are quarantined to `.unrouted`, never published bare —
`src/utils.zig` (6 unit tests on the validator) plus a live run inserting `evil.acme`, `*`
and `a b`, all three quarantined.

---

## Part 2 — The consumer

### 2.1 Invariants — break these and nothing protects you

1. **Never take identity from a payload.** It is the second token of the subject, vouched
   for by the broker. A `principal` field in a body is a claim, not an identity.
2. **Never write to a synced table.** Only the CDC applier writes there. Optimistic writes
   belong in a separate table joined by a view — otherwise unconfirmed state becomes
   indistinguishable from confirmed state, silently.
3. **Mint the primary key yourself**, as a `uuid` (v7 sorts by creation time). It is also
   your correlation token: the echo of your own write carries the key you chose.
4. **Send `version` exactly as CDC renders it** — `2026-08-18T04:57:10.827000Z` for
   `timestamptz`, the same without `Z` for `timestamp`. ⚠️ Migrating the column changes the
   wire format of every value in it, and PostgreSQL parses both, so nothing fails while
   your comparisons quietly stop agreeing with the feed.
5. **Treat a reply as a verdict, not as data.** State always arrives through CDC.
6. **Keep `Nats-Msg-Id` stable across retries**, and subject-safe (no `.`, `*`, `>`,
   whitespace) — it becomes a subject token.

### 2.2 The workflow

```
connect            credential = identity; the tenant comes from the JWT claim (🚧) or a
                   per-principal KV entry — never from user code
seed               request a snapshot, replay chunks, then apply CDC from the snapshot LSN
steady state       apply CDC with INSERT … ON CONFLICT(pk) DO UPDATE — idempotent, so a
                   replayed or redelivered event converges
write              publish mutation.<principal>.<table>.<op>; wait for the PubAck
confirm            the CDC echo carries your key — that is success. A verdict on
                   mutation_ack.<principal>.<msg_id> means failure. Neither within ~10s
                   means unconfirmed
```

### 2.3 Risks

| risk | what happens | what to do |
| --- | --- | --- |
| **write refused** | ✅ verdict: `rejected` (permanent — do not resend) or `failed` (retry budget exhausted) | branch on `status` |
| **write lost** | no echo, no verdict — the bridge died before reporting | timeout, then retry; `Nats-Msg-Id` makes it idempotent |
| **stale write** | LWW rejected it; no event, no verdict | the winner arrives via CDC. Do not hand-revert |
| **history aged out** | the CDC stream pruned past your position | compare your `seq` against the stream's `first_seq`; re-snapshot |
| **offline too long** | your tombstone was reaped; a queued edit resurrects a deleted row | check the GC watermark before flushing |
| **PubAck ≠ applied** | the row can still be refused by PostgreSQL afterwards | a PubAck means "the bridge will see this", never "this was written" |

### 2.4 Best practice — JWT / operator mode

Today: one `user`/`password` per principal in the server config. It works and is enforced,
but does not scale and is not secret in a browser.

The endpoint is a **scoped signing key**:

```shell
nsc edit signing-key --account APP --role client --sk A… \
  --allow-pub "mutation.{{name()}}.>" \
  --allow-sub "cdc.{{tag(tenant)}}.>" --allow-sub "_INBOX.>"
```

`{{name()}}` expands at user-creation time, so the login id your auth server already has
*becomes* the principal, and adding a user changes no server config. Two properties that
matter more than the convenience:

* permissions come from the **signing key's scope**, not from the user JWT — a compromised
  minting service can name a user but cannot widen what that user may do;
* the JWT **expires**, so a leaked credential has a bounded window.

Neither is obscurity. Both are *binding*: the grant is derived from the identity and cannot
drift away from it — the same property the subject invariant gives the namespace.

---

## What is not protected

Stated plainly, because half of what was found while building this was assumed rather than
written down.

* **A stolen credential** — full access to that identity. No subject grammar helps.
* **A publisher bug** 🚧 — NATS enforces who may *subscribe*; nothing verifies the bridge
  tagged a row with the right tenant. That is the cost of one bridge serving many tenants.
  A publication and slot per tenant moves that guarantee back into PostgreSQL.
* **Reads, today** — every client subscribed to `cdc.>` receives every published table's
  changes. Tenant routing is ✅ built for CDC and 🚧 not for snapshots.
* **Schema metadata** — `$KV.schemas.<table>` is readable by every client: table names and
  column names, never row values. A deliberate trade; per-tenant schema copies would cost
  more than they protect.
* **Credentials in git** ⚠️ — `.env.bridge` and `.env.admin` are tracked, and contain
  `DATABASE_WRITER_URL`, `DATABASE_URL` and the superuser password. They are `*_changeme`
  dev values today, but the *shape* is the exposure: rotating them later does not remove
  them from history.

  `DATABASE_WRITER_URL` is the one that matters most, because it is strictly more powerful
  than anything built on top of it. The tombstone sweeper is bounded four ways — tables by
  `GRANT`, rows by RLS, window by a compiled floor, and it only ever deletes rows already
  soft-deleted — while a holder of that URL can run

  ```sql
  DELETE FROM <table> WHERE deleted_at IS NOT NULL;   -- no threshold, no scoping, no floor
  ```

  in one line of `psql`. ⚠️ So gating the *sweeper* (a launch key, a signed authorisation)
  protects nothing: every guard it would add sits in the same env file as the credential
  that makes it unnecessary. The protection that counts is keeping the URL out of the
  repository and out of shells that do not need it.

  The NKEY seed already gets this treatment — deliberately in no env file, passed on the
  command line. The database credentials should get the same.

* **The HTTP telemetry server** ⚠️ — it binds `0.0.0.0:9090` (`INADDR_ANY`) with no
  authentication on any endpoint. Verified reachable: `/metrics` and
  `/streams/info?stream=CDC` both answer `200` to an unauthenticated caller.

  | endpoint | exposure |
  | --- | --- |
  | `POST /shutdown` | **stops CDC in one request.** It duplicates SIGTERM, which the bridge already handles (`bridge.zig:304`) — but SIGTERM needs process ownership, this needs a socket. Removing it is safer than authenticating or rate-limiting it, because a limiter still permits the kill |
  | `GET /streams/info?stream=` | one **NATS round trip per HTTP request** (amplification), and it discloses stream names and configuration |
  | `GET /metrics`, `/status`, `/health` | disclosure of table names, lag and throughput; cheap to serve |

  In order: drop `/shutdown`, bind `127.0.0.1` by default (configurable for a remote
  Prometheus), then rate-limit — the limiter earns its place mainly on `/streams/info`,
  where each call costs a broker request rather than a counter read.

* **Read filtering by RLS** — RLS is evaluated for a *query*, and logical decoding runs no
  query. ✅ Measured: a policy returned 1 row to a `SELECT` and the WAL carried 2. Use a
  publication row filter for CDC; RLS bounds writes and snapshots only.

---

## Where each claim is tested

⚠️ **Nine of the claims below have no automated test.** They were verified once, by hand,
against a live stack. That is weaker than it sounds: none of them re-runs after a reset, a
refactor, or a migration — and most of the defects found while building this were found by
*running* something, not by reading it.

| claim | where |
| --- | --- |
| grant vs schema disagreement; refusal reports SQLSTATE 42501 | ✅ `scripts/scenarios/writable.py` |
| a future-dated version is clamped to `now()` + tolerance, and the client is told what was stored | ✅ `scripts/scenarios/clamp.py` — asserts the cap, that the row unfreezes once the window passes, the verdict's wire format, and that a within-tolerance version is left untouched |
| every accepted write gets one definitive reply, and `stale` is distinguished from `row_deleted` | ✅ `scripts/scenarios/replies.py` — both zero-row outcomes produced deliberately, plus the first-write case that must not be mistaken for a grave |
| a writer that bypasses the bridge still stamps the version and still soft-deletes; the sweeper alone may reap | ✅ `scripts/scenarios/guards.py` — 6 assertions, including both halves of the sweeper bypass |
| mutation envelope round trip, and the verdict it returns | ✅ `scripts/scenarios/mutate.py`, `web-consumer/zb-mutate.mjs` |
| the principal reaches RLS: `set_config` and the upsert share one transaction, now the pipeline's implicit one | ✅ `scripts/scenarios/writable.py`, `tiebreak.py`, `invalidate.py` — every RLS-scoped write would be refused if it did not |
| client's JetStream permission set is complete | ✅ `web-consumer/zb-probe.mjs` |
| snapshot memory is bounded by the message budget rather than the table; an unpublishable row is refused before transfer; an aborted snapshot leaves nothing reachable | ✅ `scripts/scenarios/wide.py` — 9 assertions. 52 MB of table cost +6.8 MB RSS and 522 MB cost +12.0 MB, measured against one shared baseline |
| snapshot key order matches PostgreSQL's `ORDER BY`; chunks fit `max_payload` | ✅ `scripts/scenarios/snapshot.py` |
| one snapshot request per table per window | ✅ `scripts/scenarios/stampede.py` |
| a schema change reaches every cache: KV schema, relation decode, refusal registry, write-path catalog | ✅ `scripts/scenarios/invalidate.py` — found the added-column half unwritable until restart, and verified to *fail* before the fix |
| a malformed mutation dead-letters and does not block the queue | ✅ `scripts/scenarios/poison.py` |
| credentials and endpoint resolution | ✅ `scripts/scenarios/credentials.py`, `endpoint.py` |
| cross-file config coherence | ✅ `scripts/scenarios/envcheck.py` |
| the ring is refused when it cannot fit memory or a message, and out-of-range tunables clamp rather than silently becoming larger | ✅ `scripts/scenarios/sizing.py` — 7 assertions, including that the guard and the allocator report the same total |
| reconnect and fault behaviour | ✅ `scripts/scenarios/faults.py`, `burst.py` |
| subject-token validation (dot, `*`, `>`, whitespace, length) | ✅ `src/utils.zig`, 6 tests |
| ack-subject parsing, both JetStream forms | ✅ `src/mutation_listener.zig`, 4 tests |
| header parsing survives the stripped trailing CRLF | ✅ `src/mutation_listener.zig`, 4 tests |
| a verdict never inherits the previous write's SQLSTATE | ✅ `src/mutation_listener.zig`, 3 tests |
| permanent vs transient SQLSTATE classification | ✅ `src/mutation_listener.zig`, 5 tests |
| writer role parsed from the connection URL | ✅ `src/preflight.zig`, 7 tests |
| `init.sql.template` renders to what it says: no eaten dollar tags, no dropped definitions, every function and event trigger present in a fresh database | ✅ `scripts/scenarios/render.py` — verified to *fail* when the bug is reinjected (2 of 8 functions, 0 of 3 triggers) |
| tombstone GC reaps past the threshold and keeps rows inside it | ✅ `scripts/scenarios/sweeper.py` — seeds ±1 minute either side of the boundary, and verified to *fail* on a too-early reap |
| the subject's principal **is** the authenticated user (`bob`, `alicex`, `admin` all refused) | ✅ `scripts/scenarios/credentials.py` §D |
| other NATS refusals (`cdc.>`, `$KV.>`, `MUTATIONS`, purge, verdict forging) | ⚠️ manual — NOTES §1.8 |
| **publication guard refuses a bare `ALTER PUBLICATION`** | ⚠️ manual |
| **tenant CDC routing, including DELETE carrying its tenant** | ⚠️ manual |
| **hostile tenant values quarantined end to end** | ⚠️ manual |
| **batched events reach a tenant-only grant** | ⚠️ manual |
| **RLS filters SELECT and is ignored by CDC** | ⚠️ manual |
| **publication filter on a non-key column blocks UPDATE/DELETE** | ⚠️ manual |
| **`CLUSTER` collapses per-tenant snapshot I/O 20×** | ⚠️ manual |
| **`bridge_reader` policy vs `BYPASSRLS`** | ⚠️ manual |

Run the automated set with:

```bash
zig build test                                              # 366 unit tests
NATS_URL=nats://alice:s3cret@127.0.0.1:4222   scripts/scenarios/.venv/bin/python scripts/scenarios/writable.py
cd web-consumer && node zb-probe.mjs                        # permission set

set -a && . ./.env.admin && set +a
scripts/scenarios/.venv/bin/python scripts/scenarios/render.py   # bootstrap integrity
```
