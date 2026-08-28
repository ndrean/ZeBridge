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
| **`bridge_reader`** | `SELECT` + `REPLICATION` — plus one deliberate exception: `INSERT`+`DELETE` (never `UPDATE`) on `zebridge_generations`, the generation producer's own bookkeeping, because the content query must run as the reader (SELECT-everywhere + `zb.tenant` RLS), and the bookkeeping row is written by that same connection — after the objects and manifest are live, so it never vouches for artifacts that don't exist (append-only by privilege; `scripts/scenarios/generations.py` proves the boundary) | any write privilege on user data. It is *physically* unable to write anything a client reads |
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

**Granting a principal a tenant needs no restart.** The mapping is resolved *inside the
policy*, as a subquery evaluated per statement, against a `zb.principal` the bridge sets
per transaction. No Zig code reads `zebridge_user_tenants` and nothing caches it, so

    INSERT INTO zebridge_user_tenants (principal, tenant_id) VALUES ('carol', 'acme');

takes effect on the very next mutation. Restarting the bridge for it is harmless and
pointless. What *does* need a restart is a different axis, and confusing the two is easy:
adding a NATS **principal** means regenerating `nats-server.conf` and reloading **NATS**
(not the bridge), while the per-table rules are `zebridge_catalogue` rows read at
boot, so changing *which tables* are tenant-routed does need the bridge to come back.

⚠️ Before this the bridge never set the session variable, so any policy reading it saw
NULL and refused **every** write. The design was validated in psql and inert in the
bridge — enabling RLS would have looked like the bridge breaking.

**Risk if wrong.** `bridge_writer` with `BYPASSRLS` silently disables every row policy —
writes still succeed, so nothing looks broken. `bridge_reader` with a write grant makes
"the read path cannot write" false while the logs stay identical. Neither failure is
visible at runtime; both are visible in `pg_roles`.

### 1.2 PostgreSQL: opening a table, and the only supported way

`bridge_writer` starts with **no table privileges**. Ingress is closed until a DBA opens a
table, so a new table is never silently writable. Three shapes, each a fixed sequence of
calls — not one function each, because the calls compose (a writable table is also
tenant-scoped; a public table is neither) and skipping one leaves a table that *looks*
finished but silently isn't. Every function referenced below is defined once, in
`init.write.template.sql` (or `init.core.template.sql` for `zebridge_catalogue`) —
`grep` for the name rather than a line number, which drifts.

**Readable via CDC (public — every tenant, every subscriber sees it).**

```sql
SELECT * FROM zebridge_enable(
    'public.currencies'::regclass,
    public_reason => 'ISO list — identical for every tenant',
    publication   => 'my_pub',
    dry_run       => false
);
```

⚠️ **The reason is mandatory.** A public table is a `zebridge_catalogue` row with
`tenant_col IS NULL`, and the table's CHECK forces a recorded `public_reason` — who decided
this table is public, and why. The event trigger refuses a bare
`ALTER PUBLICATION ... ADD TABLE` for a table that is neither tenant-scoped nor in the
catalogue. Deliberately: a bare `ALTER PUBLICATION`
publishes with no row filter and no RLS, sending every row to every subscriber, and nothing
in the bridge can detect that — it is a pass-through by design.

✅ Manual for the refusal itself (NOTES §1.8); ✅ `scripts/scenarios/render.py` proves the
guard **exists** after a render, which is the failure that actually happened — the trigger
vanished along with six functions when `envsubst` ate a dollar tag.

`SELECT * FROM zebridge_audit_publications();` answers *is anything published without being
scoped?* — the invariant a pass-through bridge cannot check for itself.

**Writable from the edge.**

```sql
SELECT * FROM zebridge_enable(
    'public.orders'::regclass,
    writable      => true,
    version_col   => 'updated_at',
    tombstone_col => 'deleted_at',
    tiebreak_col  => 'last_writer',
    publication   => 'my_pub',
    dry_run       => false
);
```

The call also writes the table's `zebridge_catalogue` row (`version_col`,
`tombstone_col`, `tiebreak_col`) in the same transaction; the bridge reads the catalogue
at boot, so a restart makes it pick the table up. (`SYNC_RULES` in the bridge's
environment is an optional per-table override for emergencies; production leaves it
unset.)

This grants the table (`SELECT, INSERT, UPDATE, DELETE`, and refuses `zebridge_ddl_events`
by name — see §1.7) and attaches the triggers that make the columns named in the catalogue
true for *every* writer, not just the bridge: `zebridge_bump_version_t` stamps the version
column when a statement leaves it alone, `zebridge_soft_delete_t` turns a DELETE into a
tombstoning UPDATE when a tombstone column is given. Skip either argument and that guarantee
is fiction — the column exists, but nothing enforces what it's supposed to mean, and it
produces no error: writes still succeed, deletes still "work". `zebridge_audit_write_guards()`
reports what is actually attached, to compare against the catalogue.

**Tenant-scoped (multi-tenant, routed per row — many tenants share one bridge, one
publication, one stream family).**

`zebridge_enable()` is the entry point — one call writes the `zebridge_catalogue` row
and does grants, guards, RLS (read and write), and publication registration together,
atomically, and prints what it did as its own return table:

```sql
-- The table needs a NOT NULL tenant column, and every writer needs to be in this table —
-- N-1: exactly one tenant per client principal.
INSERT INTO zebridge_user_tenants (principal, tenant_id) VALUES ('alice', 'acme');

SELECT * FROM zebridge_enable(
    'public.orders'::regclass,
    tenant_col    => 'tenant_id',
    writable      => true,
    version_col   => 'updated_at',
    tombstone_col => 'deleted_at',
    tiebreak_col  => 'last_writer',
    publication   => 'my_pub',
    dry_run       => false
);
```

The catalogue row (`tenant_col`, `version_col`, `tombstone_col`) lands in the same
transaction; the bridge reads the catalogue at boot, so a restart makes it pick the table
up. (`TENANT_RULES`/`SYNC_RULES` in the bridge's environment are optional per-table
overrides for emergencies; production leaves them unset.)

`zebridge_audit_write_guards()` reports what triggers/policies/RLS state are actually
attached to a table, to compare against what the catalogue claims. Reads
routing correctly is not evidence writes are scoped — they are two different mechanisms,
on two different sides of the bridge, and neither implies the other; check both.

For a table that only needs read scoping (no `writable => true`), or to compose the pieces
by hand for a case `zebridge_enable()` doesn't cover, the underlying functions are still
callable directly: `zebridge_grant_edge_writes`, `zebridge_install_write_guards`,
`zebridge_scope_writes_by_tenant`, `zebridge_scope_reads_by_tenant` — each documented at
its own definition in `init.write.template.sql`/`init.core.template.sql`.

⚠️ `zebridge_scope_publication_to_one_tenant()` is a different shape entirely — it pins a
publication (and therefore the bridge) to **one** tenant value, for one-bridge-per-tenant
deployments. It is not the multi-tenant, per-row path above, and does not compose with it.

**The publication itself — where its name comes from, and how tables reach it.**

A ZeBridge publication is **created by name, on request, and never as a side effect**.
The templates create none: `zebridge_create_publication('my_pub')` is the only supported
way to make one, and `zebridge_enable(..., create_publication => true)` calls it for you.
Neither has a default name.

That function exists because the name is not the whole story. Three tables must be in
**every** publication a bridge attaches to, and a hand-written `CREATE PUBLICATION`
leaves all three out:

| table | what its absence costs that bridge |
| --- | --- |
| `zebridge_ddl_events` | never learns a schema changed — clients keep the old shape |
| `zebridge_gc_watermark` | clients have no offline-window watermark to read |
| `zebridge_user_tenants` | `$KV.tenants` is never populated: PROTOCOL Step 0 fails for every client |

None of that is visible from outside. The publication is not empty, the bridge boots, the
feed carries user rows, and every health check is green. `zebridge_create_publication`
attaches the three; the write half's own install attaches `zebridge_user_tenants` to every
publication that already exists, so the two orders (core→write→create, core→create→write)
both end complete.

`BRIDGE_CDC_PUBLICATION` no longer enters any template. It is an ordinary input to the
bridge — the one that lets it boot with no flags from `.env.bridge` — with `--pub`
overriding it, and neither has a fallback: unset stops the boot. Migrations name their
publication as an argument.

Tables reach a publication by exactly three paths, all funnelled through the publication
guard: a migration calling `zebridge_enable(..., publication => 'pub_x')`;
`zebridge_create_publication` for the three bridge-owned tables above; and
`zebridge_scope_publication_to_one_tenant()` for the one-bridge-per-tenant shape.

Divergence cannot end in a half-state. A bridge given no publication at all — no `--pub`,
no `BRIDGE_CDC_PUBLICATION` — refuses to boot rather than falling back to a compiled name;
one naming a missing publication refuses to boot (`PublicationNotFound`, with the hint);
one naming an existing publication that lacks a table simply never sees that table — no
CDC, no boot schema — which the boot log's explicit table list
(`Publication '…' verified (N tables: …)`) is there to make visible. A migration naming
a publication that does not exist fails hard: the `ALTER PUBLICATION … ADD TABLE` inside
`zebridge_enable()` raises, and the transaction rolls back **everything** the call did —
grants, guards, RLS included — so `mix ecto.migrate` stops with nothing half-applied.

**A dedicated publication** (to pair with its own slot and bridge — e.g. partitioning
tables across walsenders) is created in the same migration that populates it, atomically
and with no env var anywhere:

```sql
-- One call. It creates pub_orders AND attaches the three bridge-owned tables, which
-- is the half a hand-written CREATE PUBLICATION silently skipped.
SELECT * FROM zebridge_enable('public.orders'::regclass,
    tenant_col => 'tenant_id', writable => true, version_col => 'updated_at',
    publication => 'pub_orders', create_publication => true, dry_run => false);
```

`create_publication` is opt-in for the same reason `allow_physical_deletes` is: without
it, `publication => 'pub_odrers'` would manufacture a second publication, silently, and
the typo would show up as a bridge that replicates one table and nothing else. Unknown
name + no opt-in = a raise naming both ways out.

The matching bridge runs `--pub pub_orders --slot slot_orders`. ⚠️ Running several
bridges concurrently against one NATS deployment is not yet supported: the MUTATIONS
durable is a fixed name shared by every bridge, so two bridges steal each other's
ingress messages — NOTES.md §1.13 carries the details and the missing piece.

### 1.3 PostgreSQL: what the schema must satisfy

A table is **readable** as it is. Being **writable** from the edge, or **tenant-scoped**,
constrains it:

| requirement | why | if missing |
| --- | --- | --- |
| primary key | rows must be identifiable; DELETE is otherwise ambiguous | table refused outright |
| key is `uuid`, minted by the client | a `bigserial` cannot be minted offline — two clients pick the same id and one row is silently overwritten | delayed duplicate-key collisions |
| version column that changes on every write | last-write-wins compares it | every mutation fails `NoVersionColumn` |
| tiebreak column (the catalogue's `tiebreak_col`), if equal versions are possible | equal versions are otherwise **refused**, so two replicas holding different rows refuse each other forever | silent permanent divergence, no error anywhere |
| `timestamptz`, not `timestamp` | naive columns record no zone; a client writing local time wins or loses on its offset, silently | wrong write wins, no error |
| tombstone column (soft delete) | an offline client's queued edit is overruled instead of resurrecting the row | deletes are physical; offline edits resurrect rows |
| every `NOT NULL` column has a `DEFAULT`, or the client sends it | the schema descriptor carries no nullability | writes fail; the client learns only from the rejection |
| ✅ tenant column inside the **replica identity** | a DELETE carries only the replica identity — a tenant outside it means deletes cannot be routed | ✅ preflight refuses the table (`tenant_not_in_replica_identity`) |

Call `zebridge_scope_writes_by_tenant('public.orders', 'tenant_id')` (§1.2) — **not** the
raw SQL directly. It performs exactly this fix when the tenant column isn't already covered
(`CREATE UNIQUE INDEX ... (tenant_id, <pk>)`, `ALTER TABLE ... REPLICA IDENTITY USING INDEX
...`, no primary-key change needed), idempotently, but also enables RLS and installs the
write-scoping policies in the same call — the part a hand-written version of just the index
and identity statements leaves silently missing. That composition is the whole reason this
is a function and not a snippet: the two-statement fix alone was tried and left a table with
correct CDC routing and an entirely unscoped write path.

⚠️ Dropping that index drops the replica identity with it, and `UPDATE`/`DELETE` then fail
with an error naming something else entirely.

**Timestamps are `timestamptz`, and the rule is mechanical.** Version columns travel
in §7.2's UTC wire format and are compared and clamped as absolute instants; a naive
`timestamp` lets two writers in different zones disagree about which write is newer,
silently, per row. `zebridge_timestamp_guard` (an event trigger, same pattern as the
publication guard) refuses any `CREATE TABLE`/`ALTER TABLE` in `public` that
introduces a `timestamp without time zone` column — the migration fails whole, inside
its own transaction. `zebridge_is_internal_table` names are exempt (Ecto's
`schema_migrations` is naive by design). A deliberate exception is a DBA act:

```sql
ALTER EVENT TRIGGER zebridge_timestamp_guard_t DISABLE;  -- migrate, then ENABLE
```

### 1.4 PostgreSQL: after every migration

1. **Open new tables explicitly** — the full sequence for the shape the table needs (§1.2:
   readable, writable, or tenant-scoped). A bare publication add is refused, which is the
   reminder for the *publication* half — nothing equivalent refuses a table that is
   writable with no guards, or tenant-routed with no RLS, which is why the next step
   matters as much as this one.
2. **Re-run both audits** — `SELECT * FROM zebridge_audit_publications();` (is anything
   published without being scoped?) and `SELECT * FROM zebridge_audit_write_guards();` (is
   anything writable without the version/tombstone/tenant triggers it claims to have?). The
   first catches an unscoped read path; the second is the one that would have caught
   today's gap — a table with correct CDC routing and zero write guards reports clean on
   the first and not on the second.
3. 🚧 **Re-`CLUSTER` tenant-scoped tables.** Locality decays: new rows land wherever there
   is space, so tenants interleave again. ✅ Measured on 200k rows / 20 tenants: one
   tenant's chain build read **6,250 blocks** interleaved versus **313** after
   `CLUSTER t USING t_zb_ri` — the difference between N tenants costing N table scans
   and costing one. `CLUSTER` takes an `ACCESS EXCLUSIVE` lock and is not maintained;
   partitioning by tenant gives the same locality permanently.
4. **Check preflight at the next boot.** It reports grants that the schema cannot honour,
   version columns that are naive, and tenant columns outside the replica identity.
5. **Restart the bridge.** There is nothing to transcribe: the `zebridge_enable(...)`
   migration already wrote the table's `zebridge_catalogue` row (tenant column or public
   reason, version/tombstone/tiebreak columns), and the bridge reads the catalogue once at
   boot — it will not see the new table until it restarts. (`SYNC_RULES`/`TENANT_RULES`
   remain as optional per-table env overrides for emergencies; production leaves them
   unset.)
6. **NATS: nothing by hand.** A tenant-scoped table is already covered by the tenant's
   existing `cdc.<tenant>.>` grant. A public table gets its own named subject
   (`cdc.<table>.>`), not a wildcard — and the restart in step 5 is what binds it: at boot
   the bridge sets `CDC_PUBLIC`'s subject list authoritatively from the catalogue's public
   tables. The seeding side needs nothing extra: the producer derives its table set from
   the publication and writes public tables under the open tenant's manifests.

### 1.4b ⚠️ One table, two publications: the narrowest bridge sets the ceiling

Publishing a table into more than one publication is legitimate — two NATS
deployments, or a staged migration — and it has one sharp edge worth knowing
before you do it.

The row-width guard bakes `MIN(max_row_bytes)` across **every instance whose
publication carries that table**. So the bridge with the smallest `BASE_BUF`
sets the write ceiling for *all* writers of that table, including those who only
talk to the wider feed. Worse, it is retroactive in one direction: rows already
stored above the new ceiling remain perfectly valid in PostgreSQL and on the
wider feed, but the narrow bridge cannot encode them — so **it quarantines the
table on its own feed** (boot preflight, or `suspendForRowTooLarge` on the next
touch) while the other bridge carries on. A partial, per-feed outage.

That is the correct behaviour — a row must fit the narrowest carrier, and the
alternative is a silent quarantine instead of a loud refusal — but it is silent
about its *cause*. Two things now say it out loud: `zebridge_enable` returns a
`WARNING` row when the table is already in another publication, and
`scripts/zbdoctor.py` reports instances that disagree on the budget, naming each
slot. Keep their `BASE_BUF` equal, or keep the publications disjoint.

⚠️ Column lists (`zebridge_enable(..., columns => …)`) do **not** soften this:
the guard is per-table, not per-projection, so a narrow bridge constrains writes
to columns it does not carry. See NOTES §10ab.

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

✅ The sweep set is derived from `zebridge_catalogue.tombstone_col`, read on the sweeper's
own writer connection — the same table the bridge reads — so the two cannot disagree about
which column is the tombstone (`SYNC_RULES` is an optional override). A table with no
tombstone is not swept: its deletes are physical and there is nothing to reap.

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

Held by `mutation.<principal>.…`, `mutation_ack.<principal>.…`, `cdc.<tenant>.…`, the
tenant-keyed generation names (`$KV.generations.<tenant>.<table>`, `gen-<tenant>`), and
`$KV.tenants.<principal>` alike — one invariant, applied everywhere a name carries an
identity to grant against.

⚠️ **Values interpolated into subjects are validated** (`utils.isSubjectToken`). A tenant
value is row data and can contain anything: a dot splits the token and the row vanishes
from its own tenant's feed while still existing in PostgreSQL — divergence no later event
can repair. ✅ Such rows are quarantined to `.unrouted`, never published bare —
`src/utils.zig` (6 unit tests on the validator) plus a live run inserting `evil.acme`, `*`
and `a b`, all three quarantined.

---

### 1.8 The row-width budget: every size guard, one table

The change feed packs each row into a fixed `2^BASE_BUF` buffer (default 16 KB);
NATS accepts up to `max_payload` (1 MB). Between the two sits every row a writer can
legally create and the feed cannot carry. These are the checks that close that gap,
by location and path. Consequences: **warning** — logged, flow continues;
**reject** — atomic refusal charged to the writer (verdict for an edge write, plain
ERROR for psql; nothing commits); **quarantine** — the table is suspended,
frozen-and-valid for every reader, lifted only by a bridge restart whose preflight
re-proves the cause is gone.

| where | CDC path | generation path |
| --- | --- | --- |
| **PostgreSQL** (the row, any writer) | `zebridge_width_guard` trigger — installed by `zebridge_enable`, unbounded columns only, budget baked into the trigger as a literal, re-derived at every bridge boot from `zebridge_limits` (one row per instance) as MIN over the instances carrying the table: row width ≥ budget → **reject** (SQLSTATE 23514, in the writer's own transaction) — ✅ `widthguard.py` (both doors, atomicity, ceiling-not-tax) | *the same trigger* — it guards the row, not the path — ✅ same |
| **bridge ingress** (mutation listener) | payload ≥ event buffer → **reject** (`RowTooLargeToReplicate`, dead-lettered on first delivery, verdict). A deliberate lower bound: the CDC event is always larger than the payload, so nothing legitimate is refused — ✅ `rowsize.py` (incl. the published `max_row_bytes`) | — (generations are read-path; there is no ingress) |
| **bridge egress** | boot preflight (stored rows + column defaults vs buffer) → **quarantine** before a byte streams — ✅ `legacybait.py` (re-derived every boot, and the readmission after repair); decode-time slot overflow → **quarantine** (`suspendForRowTooLarge`: ACK past, suspension published) — ✅ `legacybait.py` (log + `"suspended":true` in `$KV.schemas`); | widest row, measured free in the producer's encode loop, ≥ event buffer → **warning** on every build (chains carry it; CDC will suspend on its next touch) — the detector for rows that predate the trigger — ✅ `legacybait.py` (warns on the first build over planted bait). Object chunking (nats.zig, 128 KB) means no wire limit exists on this path to check |
| **NATS broker** | any single message > `max_payload` → publish refused — the floor under `BASE_BUF`'s ceiling (2^20 = 1 MB). Broker contract, not bridge code — no scenario, by design | chunk messages are 128 KB by construction; the broker limit is unreachable |

Two properties to read off the matrix. Down the CDC column, consequences soften as
checks move earlier: quarantine remains only where a row got past every reject —
the reject rows exist to starve the quarantine row. And the generation column's
near-emptiness is earned, not missing: the trigger covers its writes, chunking
dissolves its wire limit, and only the legacy detector remains.

The budget is **not** maintained by hand. Each bridge registers its own
`2^BASE_BUF` at boot — `zebridge_register_limits(slot, publication, bytes)`, one
row per INSTANCE, keyed by slot — so the trigger's ceiling is always the buffer the
narrowest instance carrying that table actually runs with. It was a manual `UPDATE` until 2026-08-26, this file said so,
and it was forgotten the first time `BASE_BUF` moved: buffer 4 KB, table still
16384, PostgreSQL accepting rows that suspend the table on the first CDC touch.

Three properties fall out of the shape:

* **per instance, not per database.** `BASE_BUF` is a per-process setting, so two
  bridges may legitimately carry different ceilings. The trigger takes `MIN` over
  the instances that carry the table — a row must fit the narrowest of them.
* **self-cleaning.** A retired bridge's rows are the ones whose `slot` is gone
  from `pg_replication_slots`; the next boot of any bridge deletes them, so a
  decommissioned instance stops constraining everyone else.
* **the reader gains no write privilege.** The registrar is SECURITY DEFINER and
  the reader holds only `EXECUTE` on it, so a read-only deployment registers its
  budget while `bridge_reader` keeps SELECT + REPLICATION and no table writes.

Every cell above names the scenario that exercises it — a check the table cites
but nothing runs is prose, and the untested cells were found exactly that way.

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
seed               apply the generation chain, then follow CDC from the manifest's cutoff
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
| **history aged out** | the CDC stream pruned past your position | compare your `seq` against the stream's `first_seq`; re-seed from the chain |
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
  changes. Tenant routing is ✅ built for CDC and for the per-tenant generation buckets.
* **Schema metadata** — `$KV.schemas.<table>` is readable by every client: table names and
  column names, never row values. A deliberate trade; per-tenant schema copies would cost
  more than they protect.
* **Credentials in git** ⚠️ — `.env.bridge` and `.env.admin` are tracked, and contain
  `DATABASE_WRITER_URL`, `DATABASE_READER_URL` and the superuser password. They are `*_changeme`
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
  | ~~`POST /shutdown`~~ | ~~stops CDC in one request. It duplicates SIGTERM, which the bridge already handles (`bridge.zig:304`) — but SIGTERM needs process ownership, this needs a socket. Removing it is safer than authenticating or rate-limiting it, because a limiter still permits the kill~~ |
  | `GET /streams/info?stream=` | one **NATS round trip per HTTP request** (amplification), and it discloses stream names and configuration |
  | `GET /metrics`, `/status`, `/health` | disclosure of table names, lag and throughput; cheap to serve |

  In order: drop `/shutdown`, bind `127.0.0.1` by default (configurable for a remote
  Prometheus), then rate-limit — the limiter earns its place mainly on `/streams/info`,
  where each call costs a broker request rather than a counter read.

* **Read filtering by RLS** — RLS is evaluated for a *query*, and logical decoding runs no
  query. ✅ Measured: a policy returned 1 row to a `SELECT` and the WAL carried 2. Use a
  publication row filter for CDC; RLS bounds writes and the producer's content queries only.

---

## Why there are so many rules on the write path

Reading asks a client for about five rules; writing asks for most of `PROTOCOL.md`. The
asymmetry is not accidental and it is not complexity for its own sake: **every write rule
buys back something an ordinary database connection gives away free** — serialisation,
key allocation, exactly-once, a return value, session identity. A client writes across an
asynchronous broker with no transaction spanning it and no connection to answer on, so each
of those has to be reconstructed. PROTOCOL.md §7 opens with the full mapping.

The bill is opt-in: a deployment that never grants edge writes stays in the five-rule
world, and §7 does not apply to it. The rules arrive with the capability.

For the memory side of the same trade — a fixed pre-allocated ring, sized by two knobs that
multiply — see README, "Sizing `BASE_BUF` and `RING_BUFFER_COUNT`", which now carries the
full double-entry table.

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
| a sequence-backed primary key on an edge-writable table is **refused** on the write path (`DbAllocatedKey`), not merely warned about — the one hazard whose damage is invisible when it happens | ✅ `scripts/scenarios/keys.py` |
| a refused write costs the client **one message, not its subscription**: the same connection still receives CDC for the table it was refused on, no `suspended` schema is published, the shared registry is untouched, and other tables are unaffected | ✅ `scripts/scenarios/keys.py` — asserted on the refused client's own connection |
| a client cannot suspend a table for everyone with one oversized write; the limit is discoverable as `max_row_bytes` | ✅ `scripts/scenarios/rowsize.py` — the DoS was measured before the guard existed |
| a row the change feed cannot carry is refused at WRITE time, both doors, atomically: edge writes get a `rejected` verdict (SQLSTATE 23514), psql an ordinary ERROR; bounded-only tables get no trigger at all | ✅ `scripts/scenarios/widthguard.py` — 6 assertions, including the small-payload fattening edit the ingress check cannot see |
| a legacy oversized row (pre-guard data) is detected by the generation producer on its first build, quarantines the table on its first CDC touch (`$KV.schemas` says `"suspended": true`), is re-flagged by every boot's preflight from the stored data, and the de-quarantine recipe (repair, reboot) readmits mechanically | ✅ `scripts/scenarios/legacybait.py` — 7 assertions; owns the only bridge |
| mutation envelope round trip, and the verdict it returns | ✅ `scripts/scenarios/mutate.py`, `web-consumer/zb-mutate.mjs` |
| the principal reaches RLS: `set_config` and the upsert share one transaction, now the pipeline's implicit one | ✅ `scripts/scenarios/writable.py`, `tiebreak.py`, `invalidate.py` — every RLS-scoped write would be refused if it did not |
| client's JetStream permission set is complete | ✅ `web-consumer/zb-probe.mjs` |
| ~~the snapshot-serving invariants~~ | retired with snapshot-on-demand (NOTES §10o–§10p): `wide.py`, `snapshot.py`, `stampede.py` deleted with the path they tested |
| a schema change reaches every cache: KV schema, relation decode, refusal registry, write-path catalog | ✅ `scripts/scenarios/invalidate.py` — found the added-column half unwritable until restart, and verified to *fail* before the fix |
| a malformed mutation dead-letters and does not block the queue | ✅ `scripts/scenarios/poison.py` |
| credentials and endpoint resolution | ✅ `scripts/scenarios/credentials.py`, `endpoint.py` |
| cross-file config coherence | ✅ `scripts/scenarios/envcheck.py` |
| the ring is refused when it cannot fit memory or a message, and out-of-range tunables clamp rather than silently becoming larger | ✅ `scripts/scenarios/sizing.py` — 7 assertions, including that the guard and the allocator report the same total |
| reconnect and fault behaviour | ✅ `scripts/scenarios/burst.py`, `downtime.py` |
| subject-token validation (dot, `*`, `>`, whitespace, length) | ✅ `src/utils.zig`, 6 tests |
| ack-subject parsing, both JetStream forms | ✅ `src/mutation_listener.zig`, 4 tests |
| header parsing survives the stripped trailing CRLF | ✅ `src/mutation_listener.zig`, 4 tests |
| a verdict never inherits the previous write's SQLSTATE | ✅ `src/mutation_listener.zig`, 3 tests |
| permanent vs transient SQLSTATE classification | ✅ `src/mutation_listener.zig`, 5 tests |
| writer role parsed from the connection URL | ✅ `src/preflight.zig`, 7 tests |
| `init.{core,write}.template.sql` renders to what it says: no eaten dollar tags, no dropped definitions, every function and event trigger present in a fresh database | ✅ `scripts/scenarios/render.py` — verified to *fail* when the bug is reinjected (2 of 8 functions, 0 of 3 triggers) |
| tombstone GC reaps past the threshold and keeps rows inside it | ✅ `scripts/scenarios/sweeper.py` — seeds ±1 minute either side of the boundary, and verified to *fail* on a too-early reap |
| the subject's principal **is** the authenticated user (`bob`, `alicex`, `admin` all refused) | ✅ `scripts/scenarios/credentials.py` §D |
| a client cannot **forge a verdict** — to itself, or to another principal — nor a dead letter on `mutation_error.>`, which is what makes PROTOCOL §7.1's outbox rules safe to follow | ✅ `scripts/scenarios/credentials.py` §D |
| the dead-letter channel `mutation_error.>` is **operator-only**: readable by the bridge's own credentials and granted to no client, because its payload carries the server's full message and a `DETAIL` can quote another tenant's rows. ⚠️ Deliberately absent from PROTOCOL.md — a client has no use for it, and naming a forbidden subject in the client's document only advertises it | ✅ `nats-server.conf.template` (not in any client's allow-list) |
| other NATS refusals (`cdc.>`, `$KV.>`, `MUTATIONS`, purge, verdict forging) | ⚠️ manual — NOTES §1.8 |
| **publication guard refuses a bare `ALTER PUBLICATION`** | ⚠️ manual |
| **tenant CDC routing, including DELETE carrying its tenant** | ⚠️ manual |
| **hostile tenant values quarantined end to end** | ⚠️ manual |
| **batched events reach a tenant-only grant** | ⚠️ manual |
| **RLS filters SELECT and is ignored by CDC** | ⚠️ manual |
| **publication filter on a non-key column blocks UPDATE/DELETE** | ⚠️ manual |
| **`CLUSTER` collapses per-tenant chain-build I/O 20×** | ⚠️ manual |
| **`bridge_reader` policy vs `BYPASSRLS`** | ⚠️ manual |

Run the automated set with:

```bash
zig build test                                              # 366 unit tests
NATS_URL=nats://alice:s3cret@127.0.0.1:4222   scripts/scenarios/.venv/bin/python scripts/scenarios/writable.py
cd web-consumer && node zb-probe.mjs                        # permission set

set -a && . ./.env.admin && set +a
scripts/scenarios/.venv/bin/python scripts/scenarios/render.py   # bootstrap integrity
```
