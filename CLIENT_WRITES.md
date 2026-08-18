LWW is already order-independent

That's the property you bought when you wrote WHERE existing._hlc < EXCLUDED._hlc. Take two mutations for the same row, A (hlc=5) and B (hlc=3):

- A then B → A inserts, B's update fails the guard → A
- B then A → B inserts, A's update passes the guard → A

Same result. For different rows there was never a constraint at all. So there is no ordering to preserve — which means you may group, reorder, and parallelize freely. The heterogeneity you're worried about (different tables, different verbs) is a bucketing problem, not an ordering one.

The guard `WHERE t._hlc IS NULL OR t._hlc < EXCLUDED._hlc` reads as: a server-side write leaves _hlc untouched, therefore any client mutation with a newer stamp overwrites it — and if the row was never client-written,_hlc IS NULL means every client mutation wins unconditionally, however stale. Backend edits are invisible to conflict resolution.

The fix is small and makes the column self-maintaining, which also removes the "every migration must remember" objection to option A:

```sql
CREATE FUNCTION zebridge_stamp_hlc() RETURNS trigger AS $$
BEGIN
  -- A write that carries no stamp is a server-side write: stamp it with the
  -- server clock so it participates in LWW instead of being invisible to it.
  IF NEW._hlc IS NULL OR NEW._hlc = OLD._hlc THEN
    NEW._hlc := ...;   -- server clock, same encoding as the client's
  END IF;
  RETURN NEW;
END $$ LANGUAGE plpgsql;
```

Attached by the same init SQL that installs your DDL triggers, applied per replicated table — so adding a table to the publication also makes it sync-capable, rather than requiring a hand-written migration that someone will forget (which is exactly why test_types has no_hlc today).

One more, while you're deciding

Store it as bigint, not text. '1786790981234-0001' < '999...' is lexicographic: it works only while every timestamp has identical digit width, and it's collation-dependent. ms * 10000 + counter in a bigint compares numerically, indexes cleanly, and makes the GC cutoff a plain integer. If you later add a real logical component, it still fits.

---

Check, don't impose.

The bridge shouldn't demand a name or a type. It should accept what's there and verify the properties that actually matter, at preflight, per table:

┌────────────────────────────┬───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬────────────────────────────────────────────┐
│           check            │                                                        why                                                        │                if it fails                 │
├────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────┤
│ column exists              │ nothing to compare against                                                                                        │ table is outbound-only                     │
├────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────┤
│ is a timestamp/bigint type │ must be orderable                                                                                                 │ outbound-only                              │
├────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────┤
│ precision ≥ microseconds   │ second precision means frequent ties, and < drops the loser silently                                              │ warn loudly                                │
├────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────┤
│ NOT NULL                   │ a NULL existing value makes existing < EXCLUDED evaluate to NULL, so the upsert's WHERE rejects the write forever │ keep the IS NULL OR guard you already have │
├────────────────────────────┼───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┼────────────────────────────────────────────┤
│ timestamptz vs naive       │ ordering breaks the moment one writer uses local time                                                             │ warn, don't refuse                         │
└────────────────────────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────┘

That last row is a warning rather than a refusal precisely because of your own schema — refusing would exclude standard Ecto and Rails setups.

**Configuration shape**:

Reuse the format you already have for TRANSITION_RULES, since operators have learned it once:

SYNC_RULES=users:updated_at,deleted_at;orders:modified;audit_log:-
SYNC_VERSION_COLUMN=updated_at        # default for tables not listed

table:version[,tombstone], - meaning explicitly outbound-only. Everything unlisted falls back to the default name; a table that doesn't have it becomes outbound-only automatically, with the reason in the preflight summary.

But the important part is where it ends up. The version and tombstone columns are part of the client contract — the consumer must know which column to send back as its version and which one means "deleted". So it belongs in the schema KV payload, alongside pg.columns and sqlite.columns, per table:

{ "table": "users",
  "pg": { "columns": [...] },
  "sync": { "version": "updated_at", "tombstone": "deleted_at", "writable": true } }

Then clients discover it exactly the way they discover the schema, and there's no second contract to keep in step with topology.json. A table with "writable": false tells the client "don't offer editing here", which is a genuinely useful thing for a UI to know.

The legacy-database answer

You can't add a column to someone's existing schema — but you often can add a trigger, which changes no shape and breaks no query:

```sql
CREATE TRIGGER bump_version BEFORE UPDATE ON legacy_orders
FOR EACH ROW EXECUTE FUNCTION zebridge_bump_version('modified');
```

That matters because the assumption "updated_at always changes on every update" is only true for writes going through the ORM. A cron job, a psql session, or another service doing raw SQL leaves it stale — and then a client's older write wins against a newer server write, silently. The trigger makes the column trustworthy without touching the schema, and it's opt-in for exactly the databases you don't control.

That gives you three tiers to promote, none of which demand a migration:

1. PK only → replicated outbound.
2. PK + a timestamp column you already have → editable from the edge.
3. - a trigger, or a deleted_at you already have → trustworthy under concurrent writers, with deletes.

Attached by the same init SQL that installs your DDL triggers, applied per replicated table — so adding a table to the publication also makes it sync-capable, rather than requiring a hand-written migration that someone will forget (which is exactly why test_types has no_hlc today).

One consequence of what you just ticked

Both PowerSync details you liked are reply-shaped:

- { conflict: 'row_deleted' } is a reply payload — it only exists because the backend answers.
- "the queue cannot advance without an acknowledgement" is a rule the client can only follow if there is an acknowledgement.

So approving the Deletes section pulls the reply channel in with it. That's fine — it was on the list anyway — but it moves from "correctness nice-to-have" to a dependency of the delete semantics you just accepted. Without it, the resurrection guard you liked has no way to reach the client.

Where "I follow you" works, and where it doesn't

Happy to drive the technical choices — batching, error handling, SQL shape, GC mechanics. Two items are different, because they change what your web-consumer must send, and you'd be the one rewriting it:

1. Table and operation move from the payload to the subject. Today the client publishes mutation.test_types.insert and puts table/operation in the body; afterwards the subject is authoritative and the body carries parameters only. Small client change, but it's a contract change.
2. The reply. The client goes from fire-and-forget to request/reply — a queue that waits, retries, and handles row_deleted. That's the largest client-side change in the whole list, and it's the one that makes the queue "blocking FIFO" like PowerSync's.

Everything else is bridge-internal and invisible to clients.

┌──────────────────────────────────────────────────┬──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                     decision                     │                                                                rationale                                                                 │
├──────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ LWW, not server-authoritative intent             │ simpler client — no optimistic-state store, no rollback machinery. Bought at the cost of a schema convention and clock caveats.          │
├──────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Constraints stated up front, not buried          │ every system in this space imposes them; PowerSync asks for per-field *_modified_at columns, so a single version column is the light end │
├──────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Capability ladder + escape path, not a hard gate │ a table without the column still replicates outbound; only edge-writing needs it                                                         │
├──────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Version column is configured, not imposed        │ there is no cross-ORM convention — your own Ecto tables are timestamp without time zone, not timestamptz                                 │
├──────────────────────────────────────────────────┼──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Soft delete + time-bounded GC                    │ resurrection protection; standard compromise, must publish the window                                                                    │
└──────────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

The one correction before you walk through it

Your fallback — "else (legacy without) pass me the OS microsecond time" — can't give LWW, because the server has nowhere to store it. Comparison needs a persisted value; a stamp that only exists in the message can only be compared against nothing.

So the fallback degrades to a different guarantee, and that's fine as long as it's named:

┌──────────────────────┬──────────────────────┬───────────────────────────────────────┬───────────────────────────────────────────┐
│        table         │     client sends     │              server does              │                 guarantee                 │
├──────────────────────┼──────────────────────┼───────────────────────────────────────┼───────────────────────────────────────────┤
│ has a version column │ that column's value  │ WHERE existing.<col> < EXCLUDED.<col> │ timestamp LWW — intent time wins          │
├──────────────────────┼──────────────────────┼───────────────────────────────────────┼───────────────────────────────────────────┤
│ no version column    │ its OS micros anyway │ plain upsert                          │ last-arrival-wins (= PowerSync's default) │
└──────────────────────┴──────────────────────┴───────────────────────────────────────┴───────────────────────────────────────────┘

The stamp is still worth sending in both cases — it dedups retries, orders the client's own upload queue, and lets the server reject stamps from a skewed clock. It just isn't adjudicating anything in tier 2.

Suggested wire format (fills your "xxxx")

{ "table": "users", "op": "UPSERT",
  "key":  { "id": 42 },
  "data": { "name": "…", "updated_at": "2026-08-15T12:00:00.123456Z" },
  "version": 1786790981234567,
  "client_id": "c-8f3a",
  "msg_id": "…" }

- version — int64 microseconds since epoch, not text: numeric comparison, no zero-padding landmine, no collation. Mirrors the table's version column when there is one, else the client's clock.
- client_id — the tiebreaker. Equal timestamps otherwise let two replicas pick different winners and diverge permanently; (version, client_id) is a total order, which is all convergence needs.
- I'd name it version, not hlc — it isn't a hybrid logical clock, and calling it one invites people to assume causality guarantees it doesn't have. If you later add a logical component, the field still fits.
- msg_id should move to the Nats-Msg-Id header if you want JetStream dedup; in the body it does nothing.

Checklist — reconciled against the code, 2026-08-18

⚠️ Every line was `[ ]` until this pass, including eleven that had been built. A plan that
disagrees with the code is worse than no plan: it hides what is left. Each item below was
checked by grepping for the thing that implements it, not from memory.

**Config and discovery**

- [x] `SYNC_RULES=table:version[,tombstone[,client_id]]` + `SYNC_VERSION_COLUMN` default —
      `args.parseSyncRules`. Gained a **third** field since this was written: the tiebreak
      column (PROTOCOL.md §7.4).
- [x] preflight validates the version column: exists, orderable, precision, NOT NULL,
      naive-vs-timestamptz — `preflight.classifyVersionColumn`, and it now also refuses a
      tenant column outside the replica identity.
- [x] the schema KV publishes what a client must discover — `version_column`,
      `tombstone_column`, `mutation_timeout_ms`, and `required` per column. The last one
      closed a real gap: a client previously learned that `inserted_at` was NOT NULL from
      a `23502` rejection.

**Security**

- [x] table, operation **and principal** from the subject, never the payload —
      `mutation_listener.parseSubject`, enforced by NATS subject permissions (§7.1)
- [x] identifiers validated against the catalog, never interpolated — `meta.hasColumn`
- [x] `zebridge_ddl_events` excluded — `isForbiddenTable`, and
      `zebridge_grant_edge_writes` refuses it too
- [x] `bridge_writer` created with no table privileges — `init.sql.template`; tables are
      opened one at a time

**Correctness**

- [x] a reply per write — `mutation_ack.<principal>.<msg_id>` carries
      `{status, reason, sqlstate, detail, seq}`. Not on a reply-to inbox, deliberately: an
      inbox dies with the page, and an outbox needs to collect verdicts after a crash.
- [x] `max_deliver` + dead-letter — plus SQLSTATE classification, so a privilege error is
      reported in milliseconds instead of consuming a retry budget meant for outages
- [x] the `IS NULL OR` guard, and now `(version, client_id)` when the table declares a
      tiebreak column — `scripts/scenarios/tiebreak.py`
- [ ] **clamp future timestamps.** Not built. A client with a skewed clock writes a version
      nobody can beat until the world catches up, and every later write is silently
      rejected as stale. ⚠️ §7.2 already *promises* this ("the bridge clamps and tells you
      what it used"), so the protocol currently overstates the implementation.

**Deletes**

- [x] `deleted_at` when configured, physical delete otherwise — `applyDelete`
- [x] `GC_THRESHOLD_MS` is the stated maximum offline window, with a 60s floor so a units
      mistake (`3600` reads as an hour, means 3.6 seconds) refuses to start
- [ ] **publish the GC watermark**, so a client can drop queued writes older than it. Not
      built: a client returning from a long offline period has no way to know its queued
      edits are unsafe to flush.
- [ ] **keep the sweeper's deletes off the wire.** Not done, and it is not free: `my_pub`
      publishes `iud`, so every reaped tombstone ships a `cdc.<table>.delete` to every
      client for a row they already know is gone. Needs a separate publication with
      `publish='insert,update'` for swept tables, or a filter.

**Then throughput**

- [ ] batch commits, `synchronous_commit=off` — still last, and still after the above

The three things, separated

┌──────────────────────────────────────────────────────┬───────────────────────────────────────┬───────────────────┐
│                                                      │             what it fixes             │ needs the server? │
├──────────────────────────────────────────────────────┼───────────────────────────────────────┼───────────────────┤
│ optimistic apply — write to local SQLite immediately │ UI feels instant                      │ no                │
├──────────────────────────────────────────────────────┼───────────────────────────────────────┼───────────────────┤
│ outbox queue — persist the intent until confirmed    │ edits survive offline, restart, crash │ no                │
├──────────────────────────────────────────────────────┼───────────────────────────────────────┼───────────────────┤
│ reply — server says accepted / stale / row_deleted   │ client can dequeue, and learn it lost │ yes               │
└──────────────────────────────────────────────────────┴───────────────────────────────────────┴───────────────────┘

The trap is that the middle one doesn't work without the third: a queue you can't acknowledge is a list you can never empty. That's why PowerSync's queue is a blocking FIFO — the ack is what pops the head.

Your scenario, played out

Two clients holding row 42:

┌───────┬─────────────────────────────┬───────────────────┬──────────────────┐
│ time  │ client A (offline at 09:00) │      server       │     client B     │
├───────┼─────────────────────────────┼───────────────────┼──────────────────┤
│ 09:00 │ user edits row 42           │                   │                  │
├───────┼─────────────────────────────┼───────────────────┼──────────────────┤
│ 10:00 │                             │ tombstone written │ B deletes row 42 │
├───────┼─────────────────────────────┼───────────────────┼──────────────────┤
│ 17:00 │ A reconnects, flushes queue │                   │                  │
└───────┴─────────────────────────────┴───────────────────┴──────────────────┘

Today: A's 09:00 edit was dropped at publish time. The user's typing vanished from their own screen. Nothing to reconcile — the data never existed.

With optimistic + outbox, no reply: A shows the edit locally, queues it, sends it at 17:00. The bridge upserts. But A never learns the outcome — it can't tell "accepted" from "rejected as stale" from "the row is gone", so it can't safely remove the queue entry, and its local row silently disagrees with the server until some later CDC event happens to correct it.

With the reply: A sends, the bridge answers row_deleted. Now A knows — drop the local row, or ask the user "this was deleted on another device, restore it?". The queue entry pops. The next queued mutation goes out.

Why FIFO, specifically

The between-clients conflict is decided by timestamps (LWW). FIFO solves a different problem: your own edits in your own order. If A created row 99 and then renamed it, sending those two concurrently means the rename can land before the create — a phantom, or an error. Blocking FIFO preserves the user's causal order without any distributed machinery.

Two mechanisms, two problems. Conflating them is the usual confusion, and it sounds like that's what you were circling.

What the client actually stores

One table and a flush loop, not a framework:

CREATE TABLE _zebridge_outbox (
  seq        INTEGER PRIMARY KEY AUTOINCREMENT,  -- FIFO order
  table_name TEXT, op TEXT, key TEXT, data TEXT,
  version    INTEGER,   -- the timestamp we discussed
  msg_id     TEXT,      -- so a retry is idempotent
  state      TEXT       -- pending | inflight | done
);

One alternative worth knowing before you commit

You might not need a reply subject for the happy path. Electric's client watches for its own write coming back through the sync stream (awaitTxId) and pops the queue on recognition. You could do the same: include msg_id in the row or match on version, and dequeue when the CDC event for your own mutation arrives.

The catch is the negative cases: a rejected mutation produces no CDC event, so "rejected" and "slow" look identical, and there's no channel for row_deleted. So realistically it's CDC-as-ack for success, reply for failure — which is why I'd still build the reply, just with the knowledge that it mostly carries bad news.

the queue only matters while the ack is outstanding. Online, it empties in milliseconds and none of this arises.

Two timelines

A is online — no conflict exists, because the events are sequential:

┌───────┬───────────────────────────────────────┬───────────┬───────────┐
│       │                   A                   │  server   │     B     │
├───────┼───────────────────────────────────────┼───────────┼───────────┤
│ 09:00 │ update 42 → local apply, queue, send  │ ack → pop │           │
├───────┼───────────────────────────────────────┼───────────┼───────────┤
│ 10:00 │ receives CDC delete → deletes locally │ tombstone │ delete 42 │
└───────┴───────────────────────────────────────┴───────────┴───────────┘

Converged, and the queue was empty the whole time after 09:00.

A is offline — the queue entry survives, and that's the case it exists for:

┌───────┬───────────────────────────────────────────┬─────────────────────────────────────────┬───────────┐
│       │                A (offline)                │                 server                  │     B     │
├───────┼───────────────────────────────────────────┼─────────────────────────────────────────┼───────────┤
│ 09:00 │ update 42 → local apply, queued, not sent │                                         │           │
├───────┼───────────────────────────────────────────┼─────────────────────────────────────────┼───────────┤
│ 10:00 │ (sees nothing)                            │ tombstone @10:00                        │ delete 42 │
├───────┼───────────────────────────────────────────┼─────────────────────────────────────────┼───────────┤
│ 17:00 │ reconnects: CDC catch-up + queue flush    │ update@09:00 vs tombstone@10:00 → stale │           │
└───────┴───────────────────────────────────────────┴─────────────────────────────────────────┴───────────┘

Both directions of arrival give the same answer, because LWW compares timestamps, not arrival order. That's the property doing the work.

And note the mirror case, which is LWW behaving as designed rather than a bug: if B deleted at 09:00 and A edited (offline) at 10:00, A's edit is newer, so it wins and un-deletes the row. row_deleted in the reply is exactly so the app can decide whether that's what it wants — restore, or discard the user's edit with an explanation.

"Pop, or play the new state?"

Pop. The reply is a verdict, not data.

┌───────────────────────────┬──────────────────────────────────────────────────────────────────────────┐
│           reply           │                               client does                                │
├───────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
│ accepted                  │ pop                                                                      │
├───────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
│ stale                     │ pop — do not hand-revert; the winning row arrives via CDC and overwrites │
├───────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
│ row_deleted               │ pop, and surface to the user                                             │
├───────────────────────────┼──────────────────────────────────────────────────────────────────────────┤
│ timeout / transport error │ don't pop, retry — msg_id makes it idempotent                            │
└───────────────────────────┴──────────────────────────────────────────────────────────────────────────┘

Keeping state on one path (CDC) and verdicts on another is what stops you having two sources of truth. The client never has to merge the reply into SQLite; it only has to decide whether the queue entry is finished.

Yes — SQLite, and in the same transaction

An in-memory array dies on page reload; the offline window can be days. So the outbox is a table in the same database as the replica, and the local apply plus the queue insert happen in one transaction:

BEGIN;
  UPDATE users SET … WHERE id = 42;       -- what the user sees
  INSERT INTO _zebridge_outbox (…) VALUES (…);  -- the intent to send
COMMIT;

Split them and a crash in between leaves you with an edit the server will never hear about, or a queued intent the user can't see. That's the client-side twin of the bridge's own rule — never ACK an LSN whose data didn't reach NATS. Same invariant, other end of the wire.

SQLocal/OPFS setup is durable across reloads, so it holds.

"first write wins" is a real policy, just not the one you fall back to.

The three policies

┌────────────────────────────┬─────────────────────────────────────┬───────────────────────────────────────────────────────────────────────┬─────────────────────────────────────────────────────────────┐
│           policy           │             server rule             │                     what happens to a stale write                     │                         client must                         │
├────────────────────────────┼─────────────────────────────────────┼───────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────┤
│ LWW (yours)                │ WHERE stored.version <              │ silently discarded — the newer intent wins                            │ stamp a version; nothing else is required for correctness   │
│                            │ incoming.version                    │                                                                       │                                                             │
├────────────────────────────┼─────────────────────────────────────┼───────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────┤
│ Last-arrival-wins          │ plain upsert, no version            │ silently overwrites — whoever arrives last wins, regardless of when   │ nothing                                                     │
│                            │                                     │ they edited                                                           │                                                             │
├────────────────────────────┼─────────────────────────────────────┼───────────────────────────────────────────────────────────────────────┼─────────────────────────────────────────────────────────────┤
│ FWW / optimistic           │ WHERE stored.version =              │ rejected, and the client is told                                      │ keep the version it read, handle rejection, re-read, retry  │
│ concurrency                │ version_i_read                      │                                                                       │ or merge                                                    │
└────────────────────────────┴─────────────────────────────────────┴───────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────────────────────────┘

First-write-wins is what HTTP If-Match/ETags, CouchDB's _rev, and most REST APIs do: a write based on a stale read is refused and the user gets "someone else changed this". Note it's more client work than LWW, not less — the client must carry the version it read and be able to recover from a rejection.

What you actually get by omission

Not a weaker policy — an undefined one:

- client publishes with no queue, while offline → nc.publish drops it. Not a conflict outcome; the edit simply never existed. Silent data loss.
- client publishes with no reply handling → the server may have applied it, rejected it as stale, or failed; the client behaves identically in all three cases.
- table with no version column → last-arrival-wins, which is the one case that genuinely is a policy, just not the one you're advertising.

So the honest sentence for the docs is closer to:

▎ ZeBridge resolves concurrent writes by last-write-wins on the row's version column. A client that does not implement the write protocol below does not get a weaker guarantee — it gets lost writes, because an unqueued mutation published while offline is discarded and an unacknowledged one is indistinguishable from a rejected one.

That's a much stronger motivator than "otherwise it's FWW", because "lost writes" is unambiguous and nobody argues with it.

What "explain what he has to implement" looks like

A conformance list in PROTOCOL.md, MUST/SHOULD, roughly:

MUST

1. Persist queued mutations in the same database as the replica, written in the same transaction as the optimistic local apply.
2. Send in FIFO order, one outstanding at a time — your own edits keep your own causal order.
3. Stamp every mutation with a version (the table's version column, or your clock if it has none) and a stable msg_id so a retry is idempotent.
4. Pop the queue only on a definitive reply (accepted / stale / row_deleted). Retry on timeout — never pop on send.
5. Treat the reply as a verdict, not data: state always arrives via CDC.
6. Before flushing after a long offline period, compare the oldest queued version against the published GC watermark; older entries cannot be applied safely.

SHOULD
7. Apply optimistically so the UI is instant.
8. Surface row_deleted to the user rather than silently discarding their edit.
9. Bound the queue, and tell the user when it stops draining.

Nine rules, and 1–4 are the ones that carry the guarantee. That's a page in the docs and roughly one SQLite table plus a flush loop per client — which is a fair price to state up front, in the same spirit as "your table needs a primary key".

1) No — created_at / inserted_at cannot be the version column

They're set once at insert and never touched again. That's not a weaker version column, it's a broken one, in either direction:

- Client sends the row's stored value (still 09:00 after any number of edits) → stored(09:00) < incoming(09:00) is false → every update is rejected, forever. Sync looks dead.
- Client sends "now" instead → the server writes 10:00 into inserted_at, and the app's own column now lies about when the row was created. You've silently repurposed data the application depends on.

So the answer to "maybe inserted_at is the updated_at" is no, and it's the one case I'd have the bridge refuse by name: a configured version column matching created% / inserted% gets rejected at preflight with an explanation, rather than accepted and quietly breaking. A guard rail, not a guess — the operator can still override it explicitly if they know their column really is maintained on update.

What to do when a table genuinely has no version column — and I'd add this rung to the ladder rather than forcing a migration:

┌─────────────────────────┬────────┬─────────────────────────────────────────────────────┬───────────────────────────────┐
│        table has        │ writes │                       policy                        │      how you ask for it       │
├─────────────────────────┼────────┼─────────────────────────────────────────────────────┼───────────────────────────────┤
│ version column          │ ✅     │ LWW — later intent wins                             │ SYNC_RULES=orders:modified_at │
│ none, and you accept it │ ✅     │ last-arrival-wins — whoever arrives last overwrites │ SYNC_RULES=orders:-arrival    │
├─────────────────────────┼────────┼─────────────────────────────────────────────────────┼───────────────────────────────┤
│ none, and you don't     │ ❌     │ outbound-only                                       │ default                       │
└─────────────────────────┴────────┴─────────────────────────────────────────────────────┴───────────────────────────────┘

The middle row is PowerSync's default behaviour, made explicit and opt-in. It means "no version column" doesn't have to mean "no editing" — it means a stated weaker guarantee, which is much easier to promote than "add a column or you can't write".

1) The view is for the server-side app, not the consumer

That's the crux, and no — the consumer does not use users_live.

┌────────────────────────────────────────────────────────────────────┬───────────────────────────────────────────┬─────────────────────────────────────────────────────────────────────────────────────────┐
│                                who                                 │                   sees                    │                                           why                                           │
├────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
│ Postgres app (your Elixir/Rails/Django backend, querying PG        │ users_live — live rows only               │ it never wants tombstones in its business queries                                       │
│ directly)                                                          │                                           │                                                                                         │
├────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
│ the bridge                                                         │ users — the real table, tombstones        │ a tombstone is the deletion event; filtering it would mean clients never learn about    │
│                                                                    │ included                                  │ deletes                                                                                 │
├────────────────────────────────────────────────────────────────────┼───────────────────────────────────────────┼─────────────────────────────────────────────────────────────────────────────────────────┤
│ the consumer                                                       │ cdc.users.*, tombstones included          │ it must apply them to remove the row locally                                            │
└────────────────────────────────────────────────────────────────────┴───────────────────────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────┘

So the naming stays completely stable on the wire: the publication has users, subjects are cdc.users.insert|update|delete, the KV key is users, snapshots COPY … FROM "users". Nothing in the protocol knows the view exists.

users_live is just:

CREATE VIEW users_live AS SELECT * FROM users WHERE deleted_at IS NULL;

— a convenience so the backend's existing queries don't start returning deleted rows. It's entirely optional: if the app is happy writing WHERE deleted_at IS NULL itself, don't create it at all.

The contrast with the Dashbit orientation is only about which name the app's queries keep:

- Dashbit: the view steals the name users, the table is renamed → app unchanged, bridge must learn a mapping in five places.
- Inverted: the table keeps users, the view gets a new name → bridge unchanged, app updates its queries.

And a nice symmetry falls out: the consumer can create exactly the same view in its local SQLite over its own replica, so "live rows" means the same thing on both ends — but that's the consumer's choice, not something the protocol requires.

<https://dashbit.co/blog/soft-deletes-with-ecto>
