# Scope — what ZeBridge does not do, and what to do about it

## Replication is by TENANT, not by query

A client receives its **whole tenant's** data for every table it follows, or none of it. There are no per-query "shapes" and no per-row read scoping: the subject ACL is `cdc.<tenant>.>`, so every client in a tenant holds every tenant row on its device.

- `WHERE last_writer = 'me'` (or any filter) works as a **view** — the whole tenant is local, so the client can filter it with ordinary SQL. It is **not** a security boundary: a tenant-mate holds the same rows and can read them locally.
- ZeBridge therefore suits tenants that are organisations, departments or projects. One tenant per user means one stream per user, which does not scale to millions of users: that needs per-user shapes, which this design does not cover.

## The client holds the whole tenant

There is no eviction, LRU, or partial retention: the replica is the tenant. Fine for thousands of rows, wrong for a tenant of millions on a phone. There is no client storage-limit story.

Large-tenant mobile is out of scope: an app that needs it must build its own retention.

## Consistency model

Eventual convergence under last-write-wins. Precisely:

- **Same-device read-your-writes: yes, provisionally**: A write is applied optimistically to local replica before the server sees it, so you read it back at once — but if it loses LWW (`stale`), the CDC echo overwrites it with the winner.
- **Cross-device read-your-writes: eventual, not immediate**: Your phone writes, your laptop sees it only after CDC propagation.
- **Referential integrity is preserved in flight**: A child event waits for its parent (the FK-hold), so a foreign-key child never appears before its parent.
- **General causal ordering is NOT preserved**: LWW orders by version timestamp, not by causality. Two related rows in different tables, with no FK between them, can be observed out of their logical order for a moment. If order matters and there is no foreign key to carry it, put the logic in a PostgreSQL function (one transaction, server-side) — never rely on the sync layer to order it.

Multi-row invariants ("total equals the sum of lines") are the same story: LWW is per-row and cannot protect them. They belong in a PostgreSQL function the mutation dispatches to, not in the client.

## The client outbox dies on reinstall

The durable outbox lives in the client's SQLite. Reinstalling the app or clearing its data loses any unconfirmed writes. `client_id` must be **stable** across restarts (it is the tiebreak and the msg-id prefix); a client that regenerates it also loses idempotency on anything still in flight.

## Schema migrations the client does not run

The client never runs migrations — it adapts to the schema the bridge publishes.
Two migration shapes need care:

- **ADD COLUMN with an expression default diverges silently.** A constant default travels with the schema, and every replica fills its old rows with it. An expression (`DEFAULT now()`) is evaluated by PostgreSQL alone: its old rows get a value, every replica's old rows get NULL, and no change event reconciles them. Run `zebridge_reseed('t')` after such a migration. See [MIGRATIONS.md](MIGRATIONS.md).
- **A primary-key change re-seeds the table.** Changing a key's type or columns changes the key *values*, so replicas cannot migrate their rows in place. The DDL trigger notices and every replica re-seeds the table from a fresh full snapshot, by itself; re-run `zebridge_enable` for the table afterwards. Plan it like a downtime, not like an `ALTER`.

## Write flooding is limited per principal, not prevented upstream

A reverse proxy cannot see individual writes: they travel inside the NATS connection, one long-lived connection. NATS does not throttle per user either: a JWT's limits are caps (message size, subscriptions, bytes), not rates. So the limits sit where the writes arrive, in two layers:

- **Aggregate principal admission**, when `MUTATION_ADMISSION=1` and the dedicated ingress service is running: all tables/operations for one principal share `$ZB.mutation.<principal>`. Count and byte budgets bound that principal; the default effective count is 1,016, the minimum of the configured 5,000 and the 64 MiB principal budget divided by the 64 KiB message ceiling plus overhead. The stream has a shared 1 GiB ceiling, so enough principals can still fill it. Accepted writes have no age expiry. Legacy direct capture caps each complete route separately and does not provide this aggregate guarantee.
- **A service rate per principal and tenant**, when `MUTATION_RATE_PER_PRINCIPAL` is enabled: delayed redelivery schedules excess work. Deployment defaults enable it; development defaults leave it off. This reduces unfair processing, but does not promise unchanged latency under server/network saturation. Revocation remains the response to a persistent abusive credential.

What remains: a flooder still fills its own backlog, and it can cost a share of the bridge's ingress time until the limits catch it. Set `MUTATION_RATE_PER_PRINCIPAL` in production; it is off by default.

## Not yet battle-tested at scale

Chaos-tested and stress-tested to about 20k client writes/s sustained on one machine, and a 3.2M-row table seeded on a low-end Android phone, but not run against a real fleet. Untested: whole-system disaster recovery (the claim that NATS state rebuilds from PostgreSQL at boot is sound but unproven as a runbook). Two bridges side by side on one database pass a scenario, but are not recommended yet.
