# Cooperative editing over LWW

One row, several editors, no lost move. This is the contract: what the table must be, what the document must look like, what PostgreSQL and the client library do, and what this construction does **not** promise.

Proven by `scripts/scenarios/route_crdt.py` (two libzb clients of two tenants), `scripts/scenarios/registers.py` (a late offline write against PostgreSQL's merge, with and without it) and the `mergeRegisters` fixtures (8 cases, run against both client libraries and the SQL function).

---

## The one sentence

> Last-write-wins at the ROW decides whether a write is accepted.
> Last-write-wins at the REGISTER decides which value survives.
> PostgreSQL merges every write it accepts; the client merges before it writes.

Two grain sizes of one rule. The row race is the protocol's, unchanged and firm: a write
older than the row is refused (`stale`). The register race decides what an accepted write
changes: each field keeps its newest edit, whoever wrote the row last.

## It works like

* **Cassandra and ScyllaDB**: every cell (a column of a row) carries its own write
  timestamp and the newest wins per cell, so two clients updating different columns of
  one row never overwrite each other. Here a register is the cell, inside one `jsonb`.
* **Figma's multiplayer**: each object's properties are separate last-writer-wins values,
  ordered by the server. Two people moving one shape: one wins. One moving it while another
  recolours it: both apply.
* **Riak's maps of LWW registers**: the same construction, named as a CRDT.

It is a CRDT, a map of last-writer-wins registers. It is not a text CRDT: two people typing
in the same field at once end with one winner (Yjs, Automerge and Google Docs exist for
that case).

---

## The moving parts

| part | where it lives | what it does |
| --- | --- | --- |
| an ordinary writable table | PostgreSQL, via `zebridge_enable` | the row, its version, its tiebreak, its tombstone |
| a `jsonb` column | that table | the cooperative document: a map of registers |
| `zebridge_merge_registers` | PostgreSQL: a trigger `zebridge_enable(register_cols => …)` installs | merges each accepted write into the stored document, register by register |
| `mergeRegisters` | **both client cores** (`core.zig`, `core.ts`) | the same merge, before a write; one fixture file pins all three |
| the write-and-reconcile loop | the application | merges its registers into what it sees, re-merges after a `stale` |

Nothing is added to the bridge, the producer, the chain, the grammar or the wire. A replica that has never heard of registers replicates this table correctly; it simply sees a `jsonb` value it does not interpret.

---

## The table: what is yours, what is fixed

Everything about the table is **your** choice, because the client reads the column names from the catalogue's descriptor, never from a convention:

```sql
CREATE TABLE public.routes (
  id          uuid PRIMARY KEY,                    -- the key: a uuid you mint (v7 sorts by creation)
  name        text NOT NULL,                       -- anything else you want on the row
  doc         jsonb NOT NULL DEFAULT '{}'::jsonb,  -- THE cooperative document
  updated_at  timestamptz NOT NULL,                -- the version column
  last_writer text,                                -- the tiebreak column
  deleted_at  timestamptz                          -- the tombstone column
);

SELECT * FROM zebridge_enable('public.routes'::regclass,
    writable      => true,
    version_col   => 'updated_at',
    tombstone_col => 'deleted_at',
    tiebreak_col  => 'last_writer',
    register_cols => ARRAY['doc'],                                     -- PostgreSQL merges it
    public_reason => 'a shared route, the cooperative-editing demo',   -- or tenant_col => 'tenant_id'
    publication   => 'my_pub', dry_run => false);
```

* **The column NAMES are free.** `doc`, `updated_at`, `last_writer`, `deleted_at` are what the demo calls them; the catalogue carries whatever you name, and the query that reads them is application code.
* **The key is a `uuid` you mint**, v7 by preference so it sorts by creation time (SECURITY §1.3). A well-known constant id is fine for a shared singleton row, which is
  what the demo's `11111111-…` is.
* **Public or tenant-scoped, as you wish.** The demo is public so a phone on tenant `kilo` and a browser on tenant `acme` can edit one row; give it a `tenant_col` instead and the same machinery works inside one tenant, invisible to the others. Tenancy is a property of the table, not of this construction.
* **Writable is required**, since every editor writes through `mutate`.
* **The version column is required** and does the work it always does. It is what makes a write `stale`, which is the signal to merge.
* **`register_cols` names the register columns.** Without it, an accepted write replaces the whole document with what its writer last saw: a late write from an editor who was offline rolls back registers written meanwhile by others, accepted, and no verdict says so. With it, PostgreSQL merges the write register by register (`zebridge_merge_registers`, a `BEFORE INSERT OR UPDATE` trigger), so it can only add or advance registers. The column must hold a JSON object of registers, or `NULL`: anything else is refused, and the write comes back `rejected`. A refused (`stale`) write never reaches the trigger. Removing a register is writing it again with a newer `t` and an empty `v`. Like every option of `zebridge_enable`, it is declared on each call: a call without it removes the merge.

## The document: what is NOT yours

The `jsonb` column must hold a **flat map of registers**, and the key names inside a register are fixed, `{v,t,w}`, because the library reads them:

```json
{
  "start": { 
    "v": { "lat": 47.2184, "lng": -1.5536 }, //any value
    "t": "2026-09-20T06:20:26.474000Z", // timesstampZ
    "w": "phone-omar" // principal
  },
  "end":   {
    "v": { "lat": 47.2076, "lng": -1.5497 }, 
    "t": "2026-09-20T06:20:29.781000Z", 
    "w": "browser-alice"
  }
}
```

| | meaning | who reads it |
| --- | --- | --- |
| the map's keys | the fields two people may edit independently | your application |
| `v` | the value, any JSON | **your application only** — the library carries it and never looks inside |
| `t` | when that value was written | the library, to order registers |
| `w` | who wrote it | the library, to break a tie on equal `t` |

⚠️ **`t` and `w` are hardcoded in both cores.** `v` is convention: `mergeRegisters`
compares `t` then `w` and copies the winning register whole, so a register may carry
extra fields and they travel with it. A register with no `t` is treated as the oldest.

⚠️ **`t` is the WIRE FORM of a `timestamptz`, and it is compared as a STRING**:
RFC 3339, UTC, `Z`, and **exactly six fractional digits**. The comparison is lexical,
which equals chronological only because the width is fixed. Pad it:

```dart
// Dart prints three digits when the microseconds are zero, and "…123Z" sorts AFTER "…123456Z"
final s = DateTime.now().toUtc().toIso8601String();
final dot = s.indexOf('.');
'${s.substring(0, dot + 1)}${s.substring(dot + 1, s.length - 1).padRight(6, '0')}Z'
```

```ts
new Date().toISOString().replace('Z', '000Z')      // three digits become six
```

```python
datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"   # %f is six
```

⚠️ **`w` must be stable and unique per editor.** It is the tie-break, so two editors sharing a `w` can disagree about the winner of an equal-`t` race. The principal alone is not enough when one principal has two editors open; the demo uses `phone-omar` and `browser-alice`.

---

## The merge

`mergeRegisters(a, b)` returns, per key, the register with the higher `(t, w)`, and the union over keys. That is a join of a semilattice: commutative, associative, idempotent.

Replicas converge whatever the order or duplication of deliveries, and no key is ever removed by a merge.

* **libzb**: `zb_call("mergeRegisters", {"a": …, "b": …})` — the handle-free core entry point, so a Dart, Python or Node host runs the library's own code rather than a copy.
* **zb-client-ts**: `import { mergeRegisters } from 'zb-client-ts'`.
* Both are pinned by `zb-client-ts/fixtures/core-fixtures.json` § `mergeRegisters`. A port is correct exactly when it passes those cases.

## The loop

```txt
read    the row from the LOCAL replica — it is what CDC last delivered
merge   the document you saw with YOUR OWN registers (all of them, every time)
write   mutate(table, 'UPDATE', {key}, {doc: merged})
stale?  the row moved under you: the echo brings the winner — merge into it, write again
settle  repeat until merge(observed, mine) == observed
```

Two rules earned by measurement, both of which look unnecessary until they are not:

1. **Ship state, not deltas.** Every write carries the union of ALL your own registers, not just the one you moved. Merging only "what I saw plus my newest change" loses your un-echoed keys to the other writer's accepted overwrites, and it does so without a single `stale` verdict, because the row version is fresh while the echo lags.
1. **"Accepted" is not convergence.** The stopping condition is `observed ⊇ mine`. Stop at the verdict and, with fresh clocks, whoever writes last with a lagging echo erases the other silently. The loop terminates because merge is monotone: registers only gain, so a fixed point exists.

With `register_cols`, PostgreSQL enforces both rules itself: every accepted write is merged into the stored document, so no accepted write can erase a register, whatever its writer shipped. The loop is then left with one job, the re-merge after a `stale`. Without `register_cols` (a table enabled before the option), both rules are the application's to keep.

Bound the loop. The reference apps stop at ten rounds.

## Drawing it

Draw from the DOCUMENT, never from the gesture. The phone's pins and the browser's pins are rendered from the row as CDC delivers it, so what is on screen is what the row holds, whoever moved it last — and a losing edit visibly jumps back instead of lingering on the screen that made it.

---

## What this is, and what it is not

It **is** a state-based CRDT: a map of last-write-wins registers, a named construction.
The merge is a genuine semilattice join, which is what "converges" means here.

It is **not**:

* **causally tracked.** There are no vector clocks. Two concurrent edits of the SAME key end with one winner, by design. Different keys never conflict, which is the whole gain over replacing the document.
* **clock-safe.** The order is a wall clock. A client whose clock steps backwards can write a register that the merge swallows, and the row's own version clamp (PROTOCOL §7.2) does not apply inside the `jsonb`. A hybrid logical clock in `t`, keeping `w` as the tiebreak, is the fix when this matters.
* **a sequence type.** No list, no text, no fractional indices. A tour's ordered stops edited concurrently would want those; a list of registers with fractional positions inside the same document is the shape, still application-level.
* **delivered by the algebra alone.** PostgreSQL can refuse a write as `stale`, which a textbook merge never does. Convergence here is the algebra, applied by PostgreSQL to every write it accepts, plus one re-merge by the client after a `stale`.

A CRDT library (Yjs and its kind) was considered and refused: its state is an opaque binary document with its own update log and persistence, which duplicates the outbox and the echo, and contradicts the one thing this project insists on — PostgreSQL owns the truth and the table stays a table anyone can query.

---

## Not to be confused with `ingest`

`zb_client_ingest` (libzb) and `zb.ingest` (zb-client-ts) belong to the ON-DEMAND path, not to this one. `ingest` takes a SERVICE's answer — `{columns, rows}`, the chain object's own shape — and applies it into a table the client holds on demand, through the same version-guarded upsert a seed uses, with an optional `scope` naming the area the answer is complete for so rows there that the answer did not carry are deleted.
It writes only the LOCAL replica; nothing reaches PostgreSQL.

| verb | direction | reaches PostgreSQL |
| --- | --- | --- |
| `mutate` | this client's write request | yes, judged there, echoed back |
| `ingest` | a service's answer into a local table | no |
| `mergeRegisters` | a pure function on two documents | no, it touches no table at all |
| `serve` / `reply` | this client ANSWERS questions about its replica | no |
