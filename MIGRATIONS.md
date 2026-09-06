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
