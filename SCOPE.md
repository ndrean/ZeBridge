# Scope — what ZeBridge does not do, and what to do about it

The engine is sound inside a fence. This is the fence, drawn on the map so it is
not a surprise in production. Each boundary states the limit and the workaround.

Nothing here is a bug. These are deliberate narrowings or known edges; the wider
scope of Electric/PowerSync is mostly that their fence is further out and already
documented.

## Replication is by TENANT, not by query

A client receives its **whole tenant's** data for every table it follows, or none
of it. There are no per-query "shapes" and no per-row read scoping: the subject
ACL is `cdc.<tenant>.>`, so every client in a tenant holds every tenant row on its
device.

- `WHERE last_writer = 'me'` (or any filter) works as a **view** — the whole
  tenant is local, so the client can filter it with ordinary SQL. It is **not** a
  security boundary: a tenant-mate holds the same rows and can read them locally.
- ZeBridge is therefore **B2B / departmental-shaped**. A tenant is a natural unit
  when it is an org, a department, or a project. For B2C the only mapping is one
  tenant per user, which means one stream per user — millions of streams, a wall.
  B2C-consumer scale needs per-user shapes, the axis this design does not cover.

## The client holds the whole tenant

There is no eviction, LRU, or partial retention: the replica is the tenant. Fine
for thousands of rows, wrong for a tenant of millions on a phone. There is no
client storage-limit story. Large-tenant mobile is out of scope until a
retention/shape mechanism exists.

## Consistency model

Eventual convergence under last-write-wins. Precisely:

- **Same-device read-your-writes: yes, provisionally.** A write is applied
  optimistically to local SQLite before the server sees it, so you read it back
  at once — but if it loses LWW (`stale`), the CDC echo overwrites it with the
  winner.
- **Cross-device read-your-writes: eventual, not immediate.** Your phone writes,
  your laptop sees it only after CDC propagation.
- **Referential integrity is preserved in flight.** A child event waits for its
  parent (the FK-hold), so a foreign-key child never appears before its parent.
- **General causal ordering is NOT preserved.** LWW orders by version timestamp,
  not by causality. Two related rows in different tables, with no FK between them,
  can be observed out of their logical order for a moment. If order matters and
  there is no foreign key to carry it, put the logic in a PostgreSQL function
  (one transaction, server-side) — never rely on the sync layer to order it.

Multi-row invariants ("total equals the sum of lines") are the same story: LWW is
per-row and cannot protect them. They belong in a PostgreSQL function the mutation
dispatches to, not in the client.

## The client outbox dies on reinstall

The durable outbox lives in the client's SQLite. Reinstalling the app or clearing
its data loses any unconfirmed writes. `client_id` must be **stable** across
restarts (it is the tiebreak and the msg-id prefix); a client that regenerates it
also loses idempotency on anything still in flight.

## Schema migrations the client does not run

The client never runs migrations — it adapts to the schema the bridge publishes.
Two migration shapes need care:

- **ADD COLUMN with a non-NULL default diverges silently.** The client does
  `ALTER TABLE ADD COLUMN` with no default, so existing local rows read NULL. A
  constant-default ADD COLUMN in PostgreSQL emits no per-row WAL, so no CDC
  carries the default to the client — PostgreSQL's old rows have the default,
  every client's old rows have NULL, and nothing reconciles them. **Bump the
  generation to force a re-seed** after such a migration, or old rows stay NULL on
  every client forever. A column added with no default (NULL everywhere) is fine.
- **A primary-key TYPE change forces a full re-seed.** Changing a pk from
  `bigserial` to `uuid` changes the key *values*, not just the type — the client's
  rows are keyed by the old values and cannot be migrated in place. This is a
  re-key, not an ALTER; it must trigger a full re-seed. Treat any pk-shape change
  as re-seed-forcing.

## Per-principal write rate-limiting does not exist yet

HAProxy rate-limits `/enroll` and edge connections, but mutations flow down the
NATS WebSocket, which the proxy treats as one opaque long-lived tunnel — it never
sees the individual writes inside it. Nor does NATS throttle per user: a user JWT's
`payload`/`subs`/`data` limits are caps (message size, subscription count, a byte
budget), not rates, and JetStream's ingest limit is server-wide — a flooding
principal degrades its neighbours before it is stopped. So today one authenticated
client can flood a tenant's writes. The fix belongs in the bridge's mutation
listener: a token bucket per principal at classify time, refusing over-budget
writes with a `rate_limited` verdict (NOTES §10dd, shelved). Set the JWT caps at
enrollment anyway — cheap hygiene, not the answer.

## Not yet battle-tested at scale

Chaos-tested and stress-tested to ~21.5k mutations/s sustained on one machine, but
not run against a real fleet. Untested at time of writing: a genuinely large
initial seed on a constrained device, whole-system disaster recovery (the claim
that NATS state rebuilds from PostgreSQL at boot is sound but unproven as a
runbook), and fleet-wide client-lag observability.
