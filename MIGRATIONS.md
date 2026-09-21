# Migrations

What happens to a replica when the PostgreSQL table it follows changes shape. 

One row per shape, both client libraries (`libzb`, `zb-client-ts`), no client restart and no bridge restart in any of them.

Proven by `scripts/scenarios/migrate_both.py` and the `libzb/python/migrate_*.py` proofs.

The rule underneath: a migration whose values a replica can DERIVE is applied
locally; one whose values only PostgreSQL knows is a re-seed. The re-seed is one
lever, `zebridge_reseed(table)`, pulled by hand or by the DDL trigger.

| shape | descriptor carries | replica does | rows | re-seed |
| --- | --- | --- | --- | --- |
| `ADD COLUMN` (nullable) | the column | `ALTER TABLE ADD COLUMN` | kept, NULL in the new column | no |
| `ADD COLUMN … DEFAULT <constant>` | the column + `default` | `ALTER TABLE ADD COLUMN … DEFAULT` — the engine fills the old rows | kept, converge with PostgreSQL | no |
| `ADD COLUMN … DEFAULT now()` (any expression) | the column, no `default` | `ADD COLUMN`; the old rows hold NULL where PostgreSQL holds a value | diverge silently | **`zebridge_reseed(t)` by hand** |
| `RENAME COLUMN` | `renamed: {new: old}` (same attnum) | `ALTER TABLE RENAME COLUMN` | kept | no |
| `DROP COLUMN` | without the column | indexes on it dropped first, then `DROP COLUMN` (a rebuild if refused) | kept | no |
| `ALTER COLUMN TYPE` on a non-key column | same name, new type | `ALTER COLUMN TYPE … USING` (PGlite) or a row-keeping rebuild (SQLite) | kept, then replaced | **automatic**: the trigger bumps the epoch |
| primary key re-typed (bigserial → uuid) | `pk_columns` + the new type | rebuilt EMPTY, watermark dropped, held events and queued writes discarded | gone, then a fresh full | **automatic**, FK closure included |
| primary key gains or loses a column | `pk_columns` | same as above | same | **automatic** |
| `ADD/DROP FOREIGN KEY` | `foreign_keys` | ALTER in place (PGlite) or a row-keeping rebuild (SQLite) | kept | no |
| `CREATE/DROP INDEX` | `indexes` | `CREATE/DROP INDEX` | kept | no |
| `DROP TABLE` | tombstone (`dropped: true`) | local table dropped, queued writes discarded loudly | gone | — |
| `DROP TABLE` then `CREATE TABLE` under the same name | a new descriptor | first sight | the producer sweeps the old chain; seeds from g1 | — |
| table no longer meets the rules (no pk, a row too large) | suspension | table kept, CDC not followed | kept, stale | lifted live when the cause goes; a lift after dropped rows re-seeds |
| a migration grows the table past `MAX_COLUMNS` | suspension (`too_many_columns`) | table kept, CDC not followed; the bridge stays up | kept, stale; rows written meanwhile are dropped by the bridge | restart the bridge (or set `MAX_COLUMNS`): the boot sees the inherited suspension, bumps the epoch, lifts; both replicas grow the columns and re-seed (`column_flood.py`) |

**A rebuild that cannot carry the rows** (a child whose parent stands empty, a NOT
NULL the old rows cannot meet) degrades to the re-key path: emptied, watermark
dropped, seeded afresh. A table is never left stuck in its old shape.

## What the server does

* The DDL trigger compares the new definition with the one from **before the
  transaction**, and bumps `zebridge_catalogue.seed_epoch` once per table per
  transaction when the key shape (pk names + types) or any column's type changed —
  through `zebridge_reseed`, so tables referencing it by foreign key follow.
* The producer forces a full generation when the epoch moved, or when the column
  shape (`name:type` list) moved since the last generation, and sweeps the chain of
  any table that left the publication.
* The clients drop their watermark when the descriptor's epoch is above the one they
  seeded at, refuse a manifest whose epoch is below the descriptor's, and refuse a
  chain object naming a column they lack — then wait for the producer's next full.

**Where the epoch comes from.** The column is created with the catalogue itself, at
`bridge-init`. A table's value is born the moment `zebridge_enable` first writes its
row: the insert does not name the column, so the table starts at 0. From there it only
ever goes up — `zebridge_reseed` by hand, the DDL trigger, or the bridge lifting a
suspension that dropped events.

Re-running `zebridge_enable` on a table that already has a row updates its rules and
leaves the epoch untouched. Deliberately: changing a version column or a tenant column
is no reason for every replica to re-seed, and an epoch that went backwards would be
worse than pointless — a replica holding a higher one would never again see a
descriptor above its own, so the re-seed rule would stop firing for that table for
good. Only the comparison means anything; the number itself never does.

## The re-key recipe

One transaction, then re-run the enable:

```sql
BEGIN;
ALTER TABLE parent ADD COLUMN new_id uuid NOT NULL DEFAULT gen_random_uuid();
ALTER TABLE child  ADD COLUMN new_parent_id uuid;
UPDATE child c SET new_parent_id = p.new_id FROM parent p WHERE p.id = c.parent_id;
ALTER TABLE child  DROP COLUMN parent_id;
ALTER TABLE parent DROP COLUMN id;
ALTER TABLE parent RENAME COLUMN new_id TO id;
ALTER TABLE parent ADD PRIMARY KEY (id);
ALTER TABLE child  RENAME COLUMN new_parent_id TO parent_id;
ALTER TABLE child  ALTER COLUMN parent_id SET NOT NULL;
ALTER TABLE child  ADD FOREIGN KEY (parent_id) REFERENCES parent(id);
COMMIT;
-- the old pk took the replica identity with it; zebridge_enable is idempotent
SELECT * FROM zebridge_enable('public.parent', <the same arguments as before>);
```

One transaction matters: the trigger's before/after comparison is per transaction.
Statement by statement, the child's re-type reads as add, drop and rename, and its
epoch does not move — the child then converges through CDC only if the remap was an
UPDATE the feed saw.

If the migrating role may not update the catalogue, the trigger raises a WARNING
naming the call to run as the owner: `SELECT zebridge_reseed('parent')`.

## What a re-seed costs

Every replica of the table, and of every table reaching it by foreign key, downloads
one full generation. On a large table that is the migration's real price; plan the
re-key like a downtime, not like an ALTER.

---

# Swapping one dataset for another

Retiring a replicated table and putting another in its place is the largest shape change
there is, and the protocol has an answer for it that needs no downtime and no
coordination: the new table is enabled, the old one is dropped, and each client learns
both facts from the same schema bucket it already watches. This is that move as it was
actually made (NOTES §10hr: `osm_pois`, 2.1M OpenStreetMap points, replaced by
`charge_points`, 16,173 OpenChargeMap points), the checks that prove each half, and what
it does to a client that already pulled the old schema.

## The move, in order

```sql
-- 1. the new table, enabled in one call (the catalogue row and the guards, atomically)
SELECT * FROM zebridge_enable('public.charge_points'::regclass,
    writable => true, version_col => 'updated_at', tombstone_col => 'deleted_at',
    public_reason => 'OpenChargeMap charge points, an open dataset',
    publication => 'my_pub', dry_run => false);
-- 2. load it (examples/08-map/load_chargers.py)
-- 3. move the readers: the service's query, the apps' table name
-- 4. only then, the old one
DROP TABLE public.osm_pois;
-- 5. its declaration, which the DROP leaves behind ON PURPOSE (below)
DELETE FROM zebridge_catalogue WHERE tbl = 'osm_pois';
```

The order matters in one place only: **drop last**. Between step 1 and step 4 both
tables are published and both replicate, which is what makes the switch free of
downtime — a client may hold the old one while another already holds the new one.

## What to check in PostgreSQL

```sql
-- the declaration of each: public or tenant-scoped, and its LWW columns
SELECT tbl, coalesce(tenant_col,'(public)') AS tenant, version_col, tombstone_col, seed_epoch
  FROM zebridge_catalogue WHERE tbl IN ('osm_pois','charge_points');

-- what is actually published. This is the list the bridge's boot verifies.
SELECT tablename FROM pg_publication_tables WHERE pubname = 'my_pub';

-- the new table is writable for real: the triggers, not the intention
SELECT * FROM zebridge_audit_write_guards() WHERE tbl = 'charge_points';
SELECT * FROM zebridge_audit_publications();     -- is anything published unscoped?

-- and the old one is gone as an object, not merely unpublished
SELECT to_regclass('public.osm_pois') IS NULL AS gone;
```

What that showed after the move: `charge_points` public, `updated_at`/`deleted_at`,
four guards attached (version bump, soft delete, tombstone-children, width); `osm_pois`
absent from the publication and `to_regclass` null.

⚠️ **The `DROP TABLE` does not delete the catalogue row.** That is deliberate — the
declaration outlives the table, so a table reborn under the same name keeps its rules —
and it has one visible consequence, below.

## What to check in NATS

```bash
N="nats --server nats://127.0.0.1:4222 --creds scripts/native/creds/bridge.creds"

# the old table's key is not deleted: it holds a TOMBSTONE, and that is what clients read
$N kv get schemas osm_pois --raw          # {"table":"osm_pois","dropped":true,"lsn":757938424736}
$N kv get schemas charge_points --raw     # the new descriptor: pk, columns, writable, seed_epoch

# the public stream's subject list — this comes from the CATALOGUE, not the publication
$N stream info CDC_PUBLIC --json | jq '.config.subjects'

# the chain: the producer sweeps a table that left the publication
$N object ls gen-_default | grep -E 'osm_pois|charge_points'
$N kv get generations "_default.osm_pois"     # expect: key not found
```

The bridge narrates the drop in one line of its log, and it is the line to look for:

```
🧹 drop prune for 'osm_pois': 2 stream purge(s), 7 chain object(s), 1 KV key(s)
🗑️  DROP TABLE tombstone published for 'osm_pois'
```

and, on the generation producer's next cadence (not at boot — the sweep is periodic,
and a failed one says "next cadence retries"):

```
🧬 '_default'/'osm_pois' left the publication — chain swept; a table reborn under this name starts at g1
```

## Nothing here needs a restart

The bridge reads the catalogue at boot, which is why it is easy to assume a new public
table is bound by a restart. It is not. Any transaction that touches
`zebridge_catalogue` reloads the catalogue at its COMMIT and reconciles `CDC_PUBLIC`'s
subject list there and then, because the catalogue rides the publication like any other
table. Measured on a throwaway table, one bridge process throughout:

| step | subjects | the new name bound? |
| --- | --- | --- |
| before | 11 | — |
| `zebridge_enable(…, dry_run => false)` | 12 | yes, no restart |
| `DROP TABLE` | 12 | **still yes** |
| `DELETE FROM zebridge_catalogue` | 11 | no, no restart |

The third row is the one to remember, and it is the next section.

## Three things the drop leaves behind

All three were found by running the checks above rather than by reading the code.

**A dead subject on `CDC_PUBLIC`.** The bridge sets that stream's subject list from the
**catalogue's** public rows, not from the publication, and the catalogue row survives
the drop — so `cdc.osm_pois.>` stayed bound after the table was gone. Nothing publishes
there, so it costs nothing but drift, and `DELETE FROM zebridge_catalogue WHERE tbl = …`
clears it live, as the table above shows.

**One chain object.** The prune took seven objects and the manifest key, and left
`osm_pois-g3-dict`, the generation's zstd dictionary — 112 KiB, referenced by nothing
(no `generations` key, no `zebridge_generations` row). Remove it with
`nats object rm gen-<tenant> <name>-g<N>-dict`. Worth a look at `object ls` after any
drop.

**The schema tombstone, forever.** `kv get schemas <table>` still answers
`{"dropped":true,…}` long after everything else is gone, and that is the one leftover
that must NOT be swept on a schedule: it is how a client that was offline for the whole
migration learns, on its next connect, that the table went away. The cost is that the
bucket accumulates one dead key per retired table — this dev bucket holds 15, mostly
test fixtures — and a client following `tables: "*"` follows every key, so it greets
each dead name once:

    osm_pois: schema unusable (no columns) — skipped

Delete a tombstone only when no replica can still be carrying the table: every client
either re-seeded since the drop or was wiped. Removing the KEY is itself a drop signal
(a client reads a vanished key exactly as it reads a tombstone), so a premature sweep
is not silent — it just reaches clients that had already moved on.

## What it does to clients

| the client | what happens |
| --- | --- |
| **holds the old table, online** | the tombstone arrives on the schema watch: the local table is DROPPED, the events held for it are discarded, and its queued writes are discarded **loudly** — a client with unsent edits is told, not silently emptied. The discard is scoped to that one table; another table's queue is untouched |
| **holds it, offline during the drop** | the same, on its next connect: the schema bucket keeps the last value per key, and that value is the tombstone. There is no window in which the drop is missed |
| **follows every table (`tables: '*'`)** | picks up the new table on its own, seeds it, and drops the old one. No deploy |
| **names its tables explicitly** | picks up neither: a list is a promise about which tables this client holds. It needs a new list and a restart |
| **holds the old one ON DEMAND** | the same tombstone, the same drop. An on-demand table is a table |
| **asks the old QUERY name** | `NoResponders` — immediately, not a timeout. A responder that no longer answers a name is a silent subject, and NATS says so at once |

Measured on a client that still named the dropped table beside the new one:

```
osm_pois: schema unusable (no columns) — skipped
osm_pois: dropped locally — the table was dropped upstream
charge_points: created — watermark dropped, re-seeding from a fresh full
charge_points: seeded 16173 row(s) from chain g3 (_default) — apply 63 ms
local tables: ['charge_points', 'sqlite_sequence']
```

⚠️ That queued-writes row was half true when this was written: the TypeScript client
discarded the outbox on a drop, a re-key and an emptied rebuild; libzb discarded only
the held events and left the outbox to retry forever against a table that no longer
exists. Writing this playbook is what found it. libzb now has the same `discardOutbox`
at the same sites (NOTES §10hs), so the row above describes both clients.

One consequence that is easy to miss: **the old table's rows are gone from every replica,
including the edits a phone made to it.** A drop is not a migration — nothing is carried
across. If rows must survive the swap, write them into the new table before step 4, as
an ordinary INSERT … SELECT, while both are still published.

## The rename that is not a rename

There is no rename verb, and that is the point. `ALTER TABLE … RENAME` would reach the
DDL trigger as a drop and a create anyway, and every replica would do exactly what it
does here: tombstone one table, seed the other. Naming the new table for what it holds,
rather than reusing the old name, buys one thing worth having — during the overlap, a
client can hold both, and a reader can be moved over deliberately instead of at the
instant the rename commits.

