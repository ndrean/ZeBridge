# Operating ZeBridge

The reference for running ZeBridge: the CLI, the doctor (`--diagnose`), what needs a restart, suspended tables, troubleshooting, configuration and sizing, the NATS streams and buckets, the sweeper and the replication slots, and how the bridge works inside. Installing it: [DEPLOYMENT](DEPLOYMENT.md). What it is and how an app uses it: the [README](README.md).

## Table of Contents

- [The CLI](#the-cli)
- [Diagnose](#diagnose)
- [Restart rules](#restart-rules)
  - [Checking a table / database against the bridge's rules](#checking-a-table--database-against-the-bridges-rules)
- [Suspended tables](#suspended-tables)
- [Troubleshooting](#troubleshooting)
- [Configuration](#configuration)
  - [Main Configuration](#main-configuration)
  - [Write limits, as a client sees them](#write-limits-as-a-client-sees-them)
- [Sizing the ring](#sizing-the-ring)
  - [Two things checked at startup, before a byte is allocated](#two-things-checked-at-startup-before-a-byte-is-allocated)
  - [What happens when a row does not fit](#what-happens-when-a-row-does-not-fit)
- [NATS streams and buckets](#nats-streams-and-buckets)
- [Sweeper](#sweeper)
- [Slots](#slots)
- [Architecture \& Internals](#architecture--internals)
  - [The main loop PG/ZB/NATS](#the-main-loop-pgzbnats)
  - [The NATS/Consumer loop](#the-natsconsumer-loop)
  - [Seeding](#seeding)
  - [Memory Management](#memory-management)
  - [Replication Slot Management](#replication-slot-management)
    - [When the slot is lost](#when-the-slot-is-lost)
  - [Reconnection Handling](#reconnection-handling)
  - [Catching up: the chain and the stream](#catching-up-the-chain-and-the-stream)
    - [What the producer builds](#what-the-producer-builds)
    - [The one rule](#the-one-rule)
    - [When the rule breaks](#when-the-rule-breaks)

## The CLI

```txt
  --slot <NAME>     Replication slot (created if absent). No default —
                    required here or as BRIDGE_CDC_SLOT.
  --pub <NAME>      Postgres PUBLICATION to stream. No default —
                    required here or as BRIDGE_CDC_PUBLICATION.
  --port <PORT>     HTTP telemetry port (default: 27434)

  --gen-nkey      Mint the bridge<->NATS nkey pair (seed to stdout, once)
  --diagnose      Doctor, bridge stopped or running: report what it meets, write nothing

  --init-nats [dev|operator]  Generate the whole NATS stack, no nsc (--force overwrites)
      [--dir DIR]             …where the files live on their host (default ./zb-nats)
      [--port N]              …the client port (default 4222), also in NATS_URL
      [--http-port N]         …the monitoring port (default 8222)
      [--ws-port N]           …the WebSocket port (default 8080)
      [--js-domain NAME]      …for a JetStream reached across a leaf link (conf, grants, env)
  --init-nats --update        Re-sign the account after a grammar change, same keys
      [--dir DIR]             …the directory holding nats-server.conf (default ./zb-nats)
      [--store PATH]          …the offline seeds (default DIR/operator.store; - reads standard input)
      [--js-domain NAME]      …add a domain to a running stack that has none (a first leaf); enrolled devices keep working (see Adding a leaf node)

  --init-nats --rotate-client-key  Replace the client signing key (ZB_SIGNING_SEED leaked):
                              after a NATS reload, every client JWT the old key signed is
                              refused, and devices renew on their own. Then restart the
                              bridge. Takes --dir and --store like --update
  
  --init-sql      The init SQL for this database, on stdout (pipe it to psql). Reads
                  DATABASE_READER_URL, DATABASE_WRITER_URL, BRIDGE_CDC_PUBLICATION
  --mint-responder --name NAME [--tenant T]... [--store PATH] [--ttl-days D]
                  Creds for a responder service, on stdout, signed from operator.store
  --mint-leaf --name NAME [--store PATH] [--ttl-days D]
                  Creds for a leaf node's remote, on stdout: what its clients may carry
                  --ttl-days: both default to 3650 (ten years): replaced with a redeploy
                  --store - reads the store from standard input (a password manager's pipe):
                  the seeds never touch this host's disk
  --revoke <principal>  Revoke: mapping + unused invites, three-clock narration.
                  Needs ADMIN_DATABASE_URL for the invocation (never stored in env)
      [--conf PATH]           …and close the token now: with OPERATOR_SEED and
                              ZB_ACCOUNT_PUB, re-sign the account's revocations
                              into this nats-server.conf (then reload the server)
      [--purge]               …and ask its devices to delete their local replica
                              and identity: at once if connected, when they renew
                              otherwise. Best-effort: offline for good keeps its data

  --revoke --key U… --conf PATH  Revoke one user key: a minted identity (the `user key`
                  a mint printed) or leaked creds. OPERATOR_SEED and ZB_ACCOUNT_PUB; the
                  account's revocations only grow. With ADMIN_DATABASE_URL, an enrolled
                  device's key is also marked in PostgreSQL, so /renew refuses it
  --view-slots    Every replication slot on the server: active, pid, LSNs, retained WAL
  --view-slot <slot>  The same for one slot
  --drop-slot <slot>  Drop an INACTIVE slot (frees its retained WAL). Needs
                  ADMIN_DATABASE_URL for the invocation, never stored in env

  --help, -h        Show this help message
```

For the first setup (`--init-nats`, `--init-sql`), see [Host setup on VPS or bare-metal](DEPLOYMENT.md#host-setup-on-vps-or-bare-metal), steps [2](DEPLOYMENT.md#2-generate-the-configuration) and [3](DEPLOYMENT.md#3-prepare-postgresql).

For the check before a start (`--diagnose`), see [Diagnose](#diagnose).

For slot management, see [Replication slot management](#replication-slot-management)

For identity management, see [Production: operator mode](README.md#production-use-operator-mode) and [4. Revoke](README.md#4-revoke).

[⬆️](#table-of-contents)

---

## Diagnose

One command, whether the bridge is running or not: `bridge --diagnose`. It reads PostgreSQL, NATS and the bridge's own HTTP port, and changes nothing. Load the same files as the bridge it checks; the publication and the slot have no default:

```sh
set -a; . zb-nats/.env.nats; . ./.env.bridge; set +a
bridge --diagnose            # or: bridge --diagnose --pub my_pub --slot my_slot
```

What it checks, in order:

- **The configuration**, as the boot would decide it: the init SQL applied, the publication, each table's verdict (primary key, writable or not, tenant scoping), whether the stored rows fit `BASE_BUF` and the smallest value that would, whether the buffer would shrink since the last boot, cascades against tombstones, the CDC window against the chain's cadence.
- **The catalogue**: `zebridge_check_all()`, every catalogued table against its own catalogue row; tenant-scoped tables whose writes nothing bounds; tenants the sweeper cannot reach; bridge instances with an empty publication, or disagreeing on the row budget.
- **The slots**: an invalidated slot, an inactive one holding WAL, and this bridge's own.
- **The bridge**: if it answers on its port, whether it is connected to NATS, its suspended tables, its reconnects. If not, the checks that need a running bridge are skipped, and said to be.
- **NATS**: reachable with these credentials; every stream and bucket (with the bridge stopped, a missing one is a note: the bridge creates them at boot); the MUTATIONS backlog cap (discard new, per principal) and the VERDICTS policy (discard old). The bridge never edits those two streams, so a wrong one is a finding to fix by hand, not something it repairs.
- **What a fresh client finds**, with the bridge running: a schema for every table, a tenant entry for every principal, and for every tenant and table a chain whose full object exists.

It ends with `🩺 DIAGNOSE: all clear` (exit 0) or the number of findings (exit 1).

More in [Verify the wiring](DEPLOYMENT.md#7-verify-the-wiring).

[⬆️](#table-of-contents)

---

## Restart rules

One rule covers almost everything:

> **The catalogue governs it → a migration. Data governs it → live. Neither restarts the bridge:** `zebridge_catalogue` rides the publication, so its rows reach the bridge through the WAL like DDL and it reloads its routing on the spot.

| change | what's needed |
| --- | --- |
| ✚ new table <br> (public or tenant-scoped) | ❗️ `zebridge_enable(...)` migration, <br> **no restart** — the bridge sees the catalogue row in the WAL, reloads its rules, reconciles CDC_PUBLIC's subjects, lifts the table's refusal and publishes its schema; the sweeper reaps its tombstones from its next pass. No env edit, no stream edit by hand. |
| changed _rule_ on an existing table (version / tombstone / tiebreak / tenant column, `register_cols`) | ❗️re-run `zebridge_enable`, <br> **no restart** — same path (the write path re-reads the catalogue on the same signal; the sweeper reads it at the start of every pass) |
| ✚ new tenant | **nothing** — a tenant is born with its first mapping (an invite redeemed, or an INSERT into `zebridge_user_tenants`): the bridge creates `CDC_<tenant>` on the spot, the producer its `gen-<tenant>` store on the first row, and the JWT's `tenant` tag carries the grants (the role's template, no server change) |
| ✚ new user on an existing tenant | **nothing** — an invite (or an INSERT into `zebridge_user_tenants`); the JWT carries the tenant tag |
| generations on/off, tenant growth, invites, enrollment | **nothing** — the producer and the mint read the database per tick/request |
| `DROP TABLE` | nothing for the bridge — the DDL trigger tombstones the schema and reaps the guard |
| `ADD` / `RENAME` / `DROP COLUMN`, an index, a foreign key | **nothing** — the DDL trigger publishes the new descriptor, every replica applies it live, rows kept ([MIGRATIONS.md](MIGRATIONS.md)) |
| a column filled by an expression default (`DEFAULT now()`) | `SELECT zebridge_reseed('t')`, **no restart** — the catalogue's `seed_epoch` rides the WAL; the producer builds a full, every replica re-seeds |
| primary key re-typed or re-shaped, a column's type changed | **nothing** — the DDL trigger bumps the seed epoch itself (foreign-key closure included). After a key change only, re-run `zebridge_enable` for the table, since the old key took the replica identity with it |
| `CREATE EXTENSION postgis` (or `vector`) while the bridge runs | **nothing** for a table created afterwards with such a column: the bridge reads the extension's types again when it meets an unknown one. A column of that type **added to a table already published**: the table is suspended (`unsupported_column_type`) until a **bridge restart**, which reads the types; the rows written meanwhile reach every replica through the re-seed |
| a migration grows a table past `MAX_COLUMNS` | the table is **suspended** (`too_many_columns`), the bridge stays up; **restart** to re-detect (or set `MAX_COLUMNS`) — the boot sees the suspension it inherited and re-seeds the table, so the rows written meanwhile reach every replica |
| | |
| change `BASE_BUF` / `RING_BUFFER_COUNT` <br> / other bridge env | **bridge restart needed** (the bridge re-registers its row-width budget and re-bakes the guards at boot). A table's descriptor rides the event buffer too, about 150 bytes per column: `2^BASE_BUF` bounds how wide a table can be described |
| change `MAX_COLUMNS` | **bridge restart needed** |

What makes this safe is that `zebridge_enable()` is the gate: its own preflight (the tombstone gate, the tenant column's existence, the width guard, the publication check) returns `preflight ERROR` rows and writes **no catalogue row** for a table that fails.
The running bridge never sees a rule it should refuse. What it does see it treats the way boot does — a table it cannot route (or that lost its row) is refused on the spot and its clients get a suspension, never a bare subject that blocks the publisher.
`zebridge_enable()` prints the bridge side as its `T3 bridge` step and the NATS side as `T4 nats`: both say there is nothing to do, since a new table needs no grant.

### Checking a table / database against the bridge's rules

After a migration, before (or without) a bridge, ask the database itself — the same
questions the bridge's preflight asks, from the same source of truth:

```sql
SELECT * FROM zebridge_check('orders', 'writable');
SELECT * FROM zebridge_check('orders', 'writable', 'updated_at', 'deleted_at', 'last_writer', 'tenant_id');
SELECT * FROM zebridge_check_all() WHERE status = 'ERROR';   -- an empty result is a clean bill
```

`intent` is what you _mean_ the table to be; the check compares it with the grants (who holds INSERT+UPDATE) and with the catalogue row.
Naming the columns makes it a pre-migration check: a declared name that disagrees with the catalogue is a finding, and a table with no row yet is checked against the names you gave.

`zebridge_check_all()` checks every catalogued table against its own catalogue row: the intent you already wrote with `zebridge_enable(...)` in your migration, including an accepted `allow_physical_deletes` (a WARNING, not an ERROR). A catalogue row whose table is gone is an ERROR.

Each row's `status` is `ok`, `NOTE`, `WARNING` or `ERROR`; only `ERROR` blocks the table.

`bridge --diagnose` runs it too, along with the slots, NATS and, with the bridge running, what a fresh client finds.

[⬆️](#table-of-contents)

---

## Suspended tables

A table that breaks a rule is **suspended**, not the bridge: its events are dropped and counted, its clients receive a suspension notice instead of a schema, and everything else keeps flowing.

`bridge_refused_tables` on `/metrics` says how many, the log says which and why, and `SELECT * FROM zebridge_suspensions` lists them.

| reason | what happened | how it lifts |
| --- | --- | --- |
| `no_primary_key` | the table has no primary key | add one; the DDL trigger lifts it live |
| `no_cdc_subject` | the table is published but has no catalogue row | `zebridge_enable(...)`; lifts live |
| `no_tenant_column` | the catalogue names a tenant column the table lacks | add the column or fix the catalogue row |
| `tenant_not_in_replica_identity` | a DELETE could not be routed to its tenant | a unique index on `(tenant, pk)` and `REPLICA IDENTITY USING INDEX` on it; `zebridge_enable` does this |
| `unsupported_column_type` | a column's type cannot be decoded | change or drop the column; the migration lifts it live. A PostGIS or pgvector column added to an already published table, on a bridge that started before the extension existed: restart the bridge |
| `row_too_large` | a row exceeded the event buffer | lifts at the first write that fits after a 30 s cooldown; a restart re-measures the widest row and keeps the suspension while it still exceeds `BASE_BUF` |
| `too_many_columns` | a migration grew the table past `MAX_COLUMNS` | drop columns (lifts live), or restart: the boot re-detects |
| `schema_too_large` | the table's schema descriptor is wider than the event buffer, so clients cannot be told its shape | restart with the `BASE_BUF` the bridge names (the boot lists every such table and the setting that fits them all), or a DDL that narrows the table lifts it live |

Rows written while a table was suspended never reached any replica. A lift after such drops, live or across a restart, bumps the table's seed epoch, so every replica re-seeds from a fresh full. Nothing to do by hand.

[⬆️](#table-of-contents)

---

## Troubleshooting

Start from what you see. Each row names the check and the rule behind it.

| you see | check | usually |
| --- | --- | --- |
| a table is missing on every client | `SELECT * FROM zebridge_suspensions`; the log line naming the table | suspended: see [Suspended tables](#suspended-tables). Or never enabled: `zebridge_enable` |
| rows are missing on every client | `bridge_refused_events_dropped_total` on `/metrics`; the table's `seed_epoch` in `zebridge_catalogue` | the table was suspended for a while; the lift re-seeds. If the epoch did not move, `zebridge_reseed('t')` |
| rows are missing on one client | `bridge_fleet_client_lag_events` for that client; its own log for `held`, `predates`, `waiting for the producer's full` | it is behind, or waiting for the next full after a re-seed; both resolve by themselves |
| a client back from a pause says `chain predates the stream` | `bridge_cdc_window_seconds` for its stream against 2 × cadence; `bridge_cdc_window_short` | the stream's window is shorter than the chain's reach: raise `CDC_MAX_AGE_SECONDS`, or a byte/message valve is ending the window first; the producer cuts a fresh generation at its next tick and the client seeds from it by itself |
| a connected client logs `pruned under the live consumer` | the same two metrics; the client's poll or read cadence | it stopped reading for longer than the stream's window (a backgrounded app, a throttled tab): it re-seeds on its own; if it happens to clients that read continuously, the window is too short |
| the bridge logs `fell off … during the build`, then `three builds in a row` | `bridge_cdc_window_seconds`; the build time on the generation's log line | the stream prunes faster than this table builds: a size valve under a burst, or an age under a build; raise the valve or the age, or lower the table's row width |
| `bridge_cdc_window_short` is 1 while `CDC_MAX_AGE_SECONDS` looks fine | `bridge_cdc_stream_bytes` and `bridge_cdc_stream_messages` against the two caps | a cap, not the age, is ending the window |
| old rows hold NULL in a new column, PostgreSQL does not | the column's default in PostgreSQL | an expression default; `zebridge_reseed('t')` |
| a client still shows the old shape after a migration | `nats kv get schemas <table>`; the client's log for `SCHEMA` | the descriptor did not publish (a suspension), or the client failed the ALTER and logged why |
| a write is rejected with `stale` | its outcome in `onVerdict` or the poll report: `rebased` or `lost` | last-writer-wins on the version; the client resends an edit whose columns the winner did not touch, and drops one on a contested column, by design |
| a write is rejected with `row_deleted` | the tombstone on the row | the row was deleted; the client's optimistic copy is reverted |
| writes never get a verdict | the client's outbox count; `nats consumer info MUTATIONS <principal>` | the client is offline, or the principal's grant does not cover `mutation.<principal>.>` |
| the client waits 90 s at connect | the client's log for `no generation chain yet` | a followed table with no chain: enable it, or remove a stale key from the `schemas` bucket |
| the bridge refuses to start | its first error line | a held slot, a missing `DATABASE_READER_URL`, a publication that does not exist; see [Restart rules](#restart-rules) |
| `bridge_wal_confirmed_lag_bytes` keeps rising | `bridge_queue_usage_percent`, `bridge_connected` | NATS is not draining, or the bridge lost PostgreSQL; see [OBSERVABILITY_TELEMETRY.md](OBSERVABILITY_TELEMETRY.md) |
| retained WAL grows for a slot nobody reads | `bridge_replication_slot_active` | an abandoned instance: `SELECT pg_drop_replication_slot('<slot>')` |

[⬆️](#table-of-contents)

---

## Configuration

All configuration constants are centralized in `src/config.zig` and `grammar.json`. Per-table replication rules (tenant column, LWW columns, tombstone) live in `zebridge_catalogue`.

### Main Configuration

ZeBridge operates with a **fixed-size buffer** for the changes egress (PG-WAL →  bridge → NATS), and can parallelize mutations ingestion (client → NATS → bridge → PG).

**1. Change Data Capture**: because the engine uses a pre-allocated ring buffer with zero allocation on the hot path, the primary runtime configuration for the CDC propagation from Postgres into NATS is by setting `BASE_BUF` (an integer between 10 and 20) and `RING_BUFFER_COUNT` (an integer between 1024 and 1M+). `MAX_COLUMNS` is detected at boot from the widest published table.
The defaults, `BASE_BUF=12` (4 KB per row) and `RING_BUFFER_COUNT=32768`, use about 150 MB.

Depending on the change volume, the schema sizes of your published tables, and if you have lengthy cascading transactions, the total buffer allocation can be configured anywhere from 16 MB to 6+ GB.

❗️ Read [Sizing the ring](#sizing-the-ring) below.

Changing `BASE_BUF`, `RING_BUFFER_COUNT` or `MAX_COLUMNS` needs a restart: see [Restart rules](#restart-rules).

**2. Mutations**: On the other side, for inserting client mutations into Postgres via NATS, you may need to raise `ZB_INGRESS_LANES` (1 to 8, default 1) with the write rate: one lane applies about 8,500 writes/s, and each lane adds a writer connection.
At boot the bridge checks the writer role's connection limit, and PostgreSQL's `max_connections`, against it.

### Write limits, as a client sees them

 writes per client are limited in backlog, on by default (`MUTATION_BACKLOG_PER_PRINCIPAL`, 5,000 queued writes: past it, that client's new writes are refused, and nothing already queued is evicted), and optionally in rate (`MUTATION_RATE_PER_PRINCIPAL`, off by default: over the rate, writes are delayed, not dropped). A delayed write changes nothing for `mutate()`, which returns at once: the client has already sent it, it waits in JetStream, and `pending()` counts it until its verdict comes back. The backlog limit refuses at the NATS publish, before the bridge: there is no verdict, the write stays in the device's outbox, and the library sends it again until the backlog has room. The backlog limit is set when the bridge first creates the MUTATIONS stream: change it on a fresh deployment, or edit the stream with `nats stream edit`.

**3. Chain, sweeper and stream retention**:

| variable | default | what it sets |
| --- | --- | --- |
| `GENERATIONS_ENABLED` | off (`1` in the generated env file of `--init-nats`) | turns the snapshot producer on. Without it no chain is built, and a new client cannot seed |
| `GENERATION_CADENCE_SECONDS` | 600 | how often the producer cuts a generation |
| `GENERATION_CHAIN_DEPTH` | 6 | generations kept per table and tenant when there are no checkpoints; with them, only how far a stalled lane may drift before a base is rotated in |
| `GENERATION_CHECKPOINT_SECONDS` | 1800 (0 turns the level off) | how often the lane cuts a checkpoint — the middle level a returning client walks instead of every delta. Only for tables of at least 100,000 rows |
| `GENERATION_BASE_REBUILD_PERCENT` | 100 | rebuild the base once the checkpoints above it weigh this percentage of it |
| `GENERATION_RETIRE_GRACE_SECONDS` | 600 | a pruned generation leaves the manifest at once; its objects survive this long, for clients mid-walk |
| `GENERATION_RETIRE_WINDOWS` | 4 | how many retirements the grace may hold at once — the bound that keeps a slow store from filling the disk |
| `GENERATION_ASYNC_FULLS` | true | the routine full (every depth) is built in the background while deltas keep being cut, then attached behind them; fulls a correctness rule asks for stay with their delta |
| `GENERATION_DEFER_FULLS` | true | while a stream has less time left than three builds of a table's full, that full waits and deltas continue; at 4 × depth generations it is built anyway |
| `GENERATION_WORKERS` | 1 | builders for the early cuts of bursting streams; more re-cut those pairs in parallel, each on its own connections, so a round lasts as long as its longest build. The cadence tick builds in turn whatever this says. Memory: workers × the biggest full's MessagePack size |
| `MUTATION_BACKLOG_PER_PRINCIPAL` | 5000 | the MUTATIONS stream's `max_msgs_per_subject`, with `discard new per subject` and workqueue retention, used when the bridge creates the stream. The subject carries the principal, so this is how many writes one principal may have queued before its publishes are refused at the door; nobody else notices. On an existing stream it changes nothing: the bridge never edits MUTATIONS (edit it with `nats stream edit`); `zbdoctor` checks the stream against the rate the bridge declares on `/status` |
| `MUTATION_RATE_PER_PRINCIPAL` | 0 (off) | writes per second one principal, and one tenant, may send. Beyond it a write is NAK'd with the delay of its place in the queue and redelivered by JetStream when its turn comes: a flood is served at the rate, other tenants' writes are answered as if it were not there, nothing is dropped. `MUTATION_RATE_BURST` (default: one second's worth) is what a quiet client may send at once |
| `ZB_INGRESS_LANES` | 1 (max 8) | parallel mutation listeners on the one ingress stream. Each lane pulls up to 64 writes and applies them in one transaction, on its own PostgreSQL writer connection and NATS connection; JetStream spreads the writes across lanes. Measured on one Mac: one lane ~8,500 writes/s, two ~14,000, four ~19,500 ([examples/09-event](examples/09-event/README.md#the-ramp-how-far-one-mac-goes)). Set at start, no rebuild |
| `GC_BATCH_ROWS` | 1000 | how many tombstones the sweeper reaps per transaction |
| `GC_THRESHOLD_MS` | 604800000 (7 days) | the sweeper's age: a tombstone older than this is reaped (floor 60000). It is the longest a client may stay offline and still catch up through the chain, with its pending edits |
| `CDC_MAX_AGE_SECONDS` | 3 × cadence | how long a CDC stream keeps an event |
| `CDC_MAX_BYTES` | 1 GiB | a CDC stream's size cap, a disk valve |
| `CDC_MAX_MSGS` | 10,000,000 | a CDC stream's message cap, a disk valve |

Two inequalities hold them together, both checked by `bridge --diagnose` and at boot:

- `2 × GENERATION_CHECKPOINT_SECONDS < GC_THRESHOLD_MS / 1000` (with checkpoints off: `GENERATION_CHAIN_DEPTH × GENERATION_CADENCE_SECONDS`), or a tombstone is reaped inside the window the chain still ships;
- `CDC_MAX_AGE_SECONDS ≥ 2 × GENERATION_CADENCE_SECONDS`, plus a build and a seed of the largest table, or a returning client finds a chain the stream no longer overlaps. Changing the cadence moves the age with it unless you set the age yourself.

One more rule, for the write path: the writer role's `CONNECTION LIMIT` must hold one connection per ingress lane, one for the sweeper and four for enrollments, `ZB_INGRESS_LANES + 5`. The init template sets 20, enough for 8 lanes. At every boot the bridge reads the live limit, prints what it needs on its 🔌 line, and warns with the `ALTER ROLE` to run when the limit is short.

The manifests live in the `generations` KV bucket, keyed `{tenant}.{table}`; the objects in per-tenant `gen-{tenant}` object stores. Why the rules are what they are: [Catching up: the chain and the stream](#catching-up-the-chain-and-the-stream).

**4. Enrollment**: The enrollment variables are with the setup they belong to: [Set up, once per deployment](README.md#1-set-up-once-per-deployment). The main config is the JWT TTL: `ENROLL_JWT_TTL_SECONDS=86_400` (1 day).

**5. CDC configuration:**

- Batch size: `5000` events or `500ms` or `256KB` (whichever first)
- Subject: `cdc.<tenant>.<table>.<operation>` for a tenant's row, `cdc.<table>.<operation>` for a public table
- Message ID: `{lsn}-{table}-{operation}` per event, `batch-<first>-to-<last>` per published batch

**6. NATS configuration:**

- Max reconnect attempts: `-1` (infinite)
- Reconnect wait: `2000ms`
- Flush timeout: `10_000ms` (10 seconds)
- Status update to PostgreSQL: every `100 ms` or `1 MB` of data

**7. WAL monitoring:**

- Check interval: `30` seconds
- Warning threshold: `512MB`
- Critical threshold: `1GB`

**8. Fixed-size internal buffers:**

- Subject buffer: `128` bytes
- Message ID buffer: `64` bytes

The event ring itself (`BASE_BUF`, `RING_BUFFER_COUNT`, `MAX_COLUMNS`) is covered on its own below, since sizing it correctly matters far more than these two — see [Sizing the ring](#sizing-the-ring) below.

**9. Fleet observability**:  `FLEET_POLL_SECONDS`, `FLEET_TTL_SECONDS`, and `SLOT_INVENTORY_SECONDS` and a  per-client `heartbeatMs` option.

See `src/config.zig` for all tunables.

[⬆️](#table-of-contents)

---

## Sizing the ring

These values are not independent, and getting them wrong has a visible consequence.

⚠️ In particular, **`BASE_BUF` is a one-way door**: lowering it after rows have already been processed, meaning **already stored** (ie can be used freely by consumers), means the next write that touches such a row can 🚦 suspend the table, impacting every client.

❗️ The bridge checks this at boot and refuses to start (PROTOCOL.md §9).

The bridge pre-allocates the ring at startup, in **three parts**:

```txt
ring = ( 2^BASE_BUF  +  sizeof(CDCEvent)  +  MAX_COLUMNS × sizeof(ColumnView) )  ×  RING_BUFFER_COUNT
         ^ data:          ^ metadata:          ^ columns:                          ^ number of events
           max bytes        fixed, 328 B         8 B × MAX_COLUMNS — resolved        buffered ahead
           for ONE row      per event            at boot, not a compile constant     of NATS
```

`sizeof(CDCEvent)` is small and fixed regardless of table shape — `columns` is a _slice_ into a separate slab, not an inline array, so this term no longer grows with the widest table you might ever replicate.

Each knob answers a different question:

- **`BASE_BUF`** (log2 bytes, range 10–20) is _how large a single row may be_. Size it to your widest row: a `jsonb` document, a long `text` column, a big array.
- **`RING_BUFFER_COUNT`** (range 1024–1048576, **clamped** to the nearest bound if you go outside it) is _how many events can queue while NATS is unreachable_. 65536 slots ≈ 1 second at 60K events/s. It is also useful for long transactions, like an `ON DELETE CASCADE`. Below that, a NATS blip starts back-pressuring the WAL reader sooner.
- **`MAX_COLUMNS`** is _how many columns one event may carry_ — normally left to auto-detection; override it only to widen the ceiling ahead of a migration or to pin the value across instances. It is resolved **per instance, at boot**:

  - Unset (the default): **auto-detected** from the widest table actually in the publication, rounded up to the next multiple of 8 for migration headroom (a table with 6 columns → `MAX_COLUMNS=8`; see the "MAX_COLUMNS=…" line at boot).
  - `MAX_COLUMNS=<N>`: an explicit override, clamped to 8–1600, that skips auto-detection — set it if you replicate a genuinely wide table, or want to fix the value across instances rather than let each one detect its own.

🚦 A table past the resolved `MAX_COLUMNS` is refused with `TooManyColumns` — loudly, with its events dropped and its clients told why — rather than truncated.

🔔 Changing these values needs a **restart**. See [Restart Rules](#restart-rules)

**Examples**:

- default settings: payload 4 kB, 32 k evt/s, small tables (< 8 cols)

```txt
defaults
12 / 32_768, MAX_COLUMNS=8   =  134 MB data +  10 MB meta +  2 MB cols 
=  148 MB  ← 4 KB/row 
```

- You can increase the slot size if you expect large payloads (64 kB) across larger tables (< 32 cols) and more events (64 k evt/s):

```txt
16 / 65_536, MAX_COLUMNS=32   = 4.3 GB data +  21 MB meta +  17 MB cols 
= 4.3 GB  ← 64 KB/row
```

- if you expect many small events (< 2 kB), and possibly long cascade transactions, or lots of events:

```txt
11 / 131_072, MAX_COLUMNS=8  =  268 MB data +  43 MB meta + 8 MB cols 
=  320 MB  ← 2 kB/row, ~1s at 130K evt/s
```

- if you expect possibly large payload (1 MB) with low event count

```txt
20 /  256, MAX_COLUMNS=128 = 268 MB data + 84 kB meta +  < 1 MB cols
= 269 MB  ← 1 MB rows, wide table, minimum ring length
```

Because `MAX_COLUMNS` is resolved per instance, a static `BASE_BUF × RING_BUFFER_COUNT` table cannot show the true total in one number — the columns term depends on what _your_ publication's widest table looks like.
The bridge computes and logs the exact figure at startup instead, both knobs included:

```txt
info(bridge): MAX_COLUMNS=8 (auto-detected: widest monitored table has 6 columns, rounded up to a multiple of 8)
info(bridge): Event ring: 1048 MB of a 16384 MB limit (6%) — 1024 MB data + 20 MB metadata + 4 MB columns
```

That is the authoritative number for your deployment; the formula above is for back-of-envelope estimates before you have a running instance to read it from.
As a rule of thumb: with `MAX_COLUMNS` at its auto-detected default for a normal table (well under 64), the data slab (`2^BASE_BUF × RING_BUFFER_COUNT`) still dominates.

### Two things checked at startup, before a byte is allocated

Both failures are silent and late if left to runtime, so the bridge refuses to start:

| check | refuses when | why not a warning |
| --- | --- | --- |
| **ring vs memory** | `(2^BASE_BUF + sizeof(CDCEvent) + MAX_COLUMNS × sizeof(ColumnView)) × RING_BUFFER_COUNT` exceeds half the process's memory limit | the slab is pre-allocated, so the alternative is an OOM kill under load. In a container the **cgroup limit** is read, not the host's RAM — otherwise a 1 GB slab looks fine on a 64 GB host until the 512 MB cgroup kills it |
| **`BASE_BUF` vs `max_payload`** | `2^BASE_BUF + envelope` exceeds what the NATS server advertises | a row that size packs successfully and is then **rejected at publish time**, with nothing in the data path saying why. That is not a tuning choice, it is a configuration that cannot work |

```txt
🔴 The ring would be 15176 MB — 2048 MB of data (BASE_BUF=11 → 2 KB × RING_BUFFER_COUNT=
   1048576) plus 328 MB of per-event metadata (328 B each) plus 12800 MB of column
   descriptors (MAX_COLUMNS=1600 × 8 B × RING_BUFFER_COUNT) — against a 16384 MB memory
   limit. … Halve RING_BUFFER_COUNT for each step you raise BASE_BUF.

🔴 BASE_BUF=20 allows a 1024 KB row, but this NATS server accepts at most 1024 KB per
   message and the envelope needs roughly 16 KB more. … Lower BASE_BUF to 19 or raise
   max_payload in nats-server.conf.
```

✅ On a healthy start you get the same arithmetic as a fact:

```txt
info(bridge): MAX_COLUMNS=8 (auto-detected: widest monitored table has 6 columns, rounded up to a multiple of 8)
info(bridge): NATS max_payload: 1024 KB (server-advertised) → CDC per-event buffer: 16 KB (BASE_BUF=14, ceiling 20)
info(bridge): Event ring: 1048 MB of a 16384 MB limit (6%) — 1024 MB data + 20 MB metadata + 4 MB columns
```

### What happens when a row does not fit

🚦 The table is **suspended**, and you will see this in the log:

```txt
🔴 SUSPENDING 'orders': a row does not fit in the 4 KB per-event buffer (BASE_BUF=12).
    Fix: restart with a larger BASE_BUF (each +1 doubles it, max 20 = 1 MB) …
```

What this does **not** do is stop the bridge. Every other table keeps replicating. The metric `bridge_refused_tables` rises, and clients of that one table receive a suspension on `$KV.schemas.<table>` (`"reason": "row_too_large"`) telling them their copy is frozen at a known LSN.

➡️ Restart with a `BASE_BUF` that fits and the table resumes; its clients re-seed from the next generation chain.

⚠️ **This is why the metrics endpoint matters.** The event that overflows may arrive years after deployment — someone pastes a large JSON document into a text column — so this is not something you can verify once at install time.

🔔 Alert on `bridge_refused_tables > 0` (Prometheus) or on `SUSPENDING` in the logs (Loki).

**The ceiling is NATS, not the bridge**: `BASE_BUF=20` is 1 MB, which is also **nats-server's default** `max_payload`. A message also carries a subject, headers and MessagePack framing, so a row sized right up to the limit is still rejected at publish time. The bridge reads the server's advertised `max_payload` from its INFO line at connect and tells you where you stand:

```txt
info(bridge): NATS max_payload: 1024 KB (server-advertised) → CDC per-event buffer: 16 KB (BASE_BUF=14, ceiling 20)
```

and warns if the two cannot coexist.

Raising `max_payload` in `nats-server.conf` (up to 8 MB is safe) is possible but affects every client and every subject on that server. The cap also stops a client from writing a row wider than it. JetStream's memory use scales with it — so for genuinely large values, prefer **keeping the blob out of the replicated table** and replicating a reference to it (URL object storage).

[⬆️](#table-of-contents)

---

## NATS streams and buckets

ZeBridge uses two stream families and several KV buckets (schemas, tenants, generations, live) plus per-tenant object stores for the bidirectional flow ZeBridge ↔ NATS ↔ consumer.
The naming is **shared** and declared in [grammar.json](src/grammar.json).

A ZeBridge instance is started with one config. The DBA starts the NATS server with its own config. `grammar.json` is the static wire grammar shared between the two: stream names, subject prefixes, and KV bucket names, declared once (`streams`, `subjects`, `kv`, `cdc_streams`, `open_tenant`, `generations`). Which tables replicate, and how, lives in the database: one `zebridge_catalogue` row per table, written by `zebridge_enable(...)`. The bridge creates what is missing at boot: the MUTATIONS and VERDICTS streams, the `schemas`, `tenants`, `generations` and `live` buckets, and the CDC stream family, which it also reconciles to the catalogue. An existing MUTATIONS or VERDICTS stream keeps the limits it has.

The `live` bucket holds one heartbeat per connected client: its lag on each stream, sent every `heartbeatMs` (30 s by default) and dropped after `FLEET_TTL_SECONDS` of silence. The bridge's fleet monitor reads it for the `bridge_fleet_*` metrics: how many clients are live per tenant, and how far behind each one is.

**Three data flows**:

1. **Bootstrap** (READ): the consumer takes each table's _schema_ (from the `schemas` KV) and seeds it from the **generation chain** (objects + a manifest in the `generations` KV): PG → ZeBridge → NATS/JS.
2. **Real-time CDC** (CDC stream, READ): the consumer receives INSERT/UPDATE/DELETE events as they happen: PG → ZeBridge → NATS/JS.
3. **Real-time ingress** (MUTATIONS stream, WRITE): the consumer updates its local storage and sends the intended change to NATS/JS → ZeBridge → PostgreSQL.

| Stream | Purpose | Retention (default) | Consumer Pattern | Role |
| -------- | ------------------- | ------------- | ----------------------- | -- |
| **CDC** | Real-time egress changes | `CDC_MAX_AGE_SECONDS`, 3 × the generation cadence; `CDC_MAX_BYTES` 1 GiB and `CDC_MAX_MSGS` 10 M as disk valves | Continuous subscription | READ |
| **MUTATIONS** | Real-time ingress changes | 2 h, 1 GB, discard new, `MUTATION_BACKLOG_PER_PRINCIPAL` queued writes per principal: past it, that principal's writes are refused | Continuous subscription | WRITE |
| **VERDICTS** | Write verdicts and the revocation ban | 2 h, 1 GB, discard old: the window a missed verdict can be recovered in | Direct get by key | WRITE |

Besides the streams, the bridge maintains the seeding buckets: the **`generations` KV** holds one chain manifest per `<tenant>.<table>`, and a per-tenant **`gen-<tenant>` object store** holds the full and delta objects the manifest points to. The producer provisions the object stores at runtime, the same way the bridge provisions per-tenant CDC streams.

The CDC stream family is owned by the bridge: at boot it creates any missing `CDC_<TENANT>` stream with file storage, limits retention and s2 compression, and reconciles every existing one to the configured age, byte and message limits, naming each move in its log. The byte cap is deliberately modest, because JetStream `max_bytes` is a reservation against the server's storage budget. It also sets `CDC_PUBLIC`'s subjects authoritatively to `cdc.<tbl>.>` for every catalogue-public table plus `cdc.<open_tenant>.>`. MUTATIONS and VERDICTS are created once and never edited: their limits are the deployment's.

⚠️ **Retention is a correctness parameter.** A client offline past the stream's window re-seeds from the chain, and the chain's cut must still be in the stream: see [Catching up: the chain and the stream](#catching-up-the-chain-and-the-stream).

Consumers use these streams to interact with NATS; the exact names are declared in `grammar.json`.

[⬆️](#table-of-contents)

---

## Sweeper

The `bridge_sweeper` companion is run as a daemon that scans periodically PostgreSQL to prune rows marked for deletion.

It uses the `WRITER` role and reaps tombstones older than `GC_THRESHOLD_MS` (7 days by default; the setting sits with the chain's in [Configuration](#configuration), because the two clocks are coupled).

```sh
DATABASE_WRITER_URL=xxx bridge_sweeper [--once]
```

💡 `--once` runs a single pass and exits.

It reaps tombstone _families_ in order, children first, in batches of `GC_BATCH_ROWS` (1000), so a million expired tombstones is a thousand small deletes, never one transaction.

**Why this**? This garbage collector is standard practice and needed to keep Postgres in sync with replicas, because LOCAL deletes are HARD deletes, but they are **propagated back as soft-deletes** via an `UPDATE SET tombstone ...` by the daemon.
This update is echoed back to every connected client as a CDC event. On an **UPDATE with a tombstone**, the client hard-deletes the row in its replica.

The Sweeper captures this lifecycle to emit lightweight telemetry about the garbage-collected records.

[⬆️](#table-of-contents)

---

## Slots

See [Replication slot management](#replication-slot-management) for details about the CLI slot management tools `--view-slot(s)` and `--drop-slot`.

[⬆️](#table-of-contents)

---

## Architecture & Internals

Once the publication is created, a bridge runs one replication slot and processes the WAL in order.

> [!IMPORTANT]
> One instance of a bridge = one slot, one port

```sh
set -a; source .env.bridge; set +a &&\
bridge --pub my_pub --slot my_slot
```

With Postgres replication set to 'logical', we use a log-based Change Data Capture (CDC) with the native  `pgoutput` (v1) logical decoding plugin to stream WAL changes in _binary_ format.

We use `REPLICA IDENTITY DEFAULT` to limit the volume, thus increase the speed of the emitted data by `pgoutput`.

🔔 The price is, on every table, a _primary key_.

**The message format**:

- CDC events and chain rows travel as MessagePack — compact, type-safe, fast (it keeps the int/float/binary distinctions JSON loses).
- Schemas travel as JSON in two shapes (PostgreSQL and SQLite), so a client builds its local tables in either. Every write carries a message id, for idempotent at-least-once delivery.
- Snapshots (deltas) travel as Zstd compressed chunks to the consumer.

### The main loop PG/ZB/NATS

1. **Read the WAL.** One thread follows PostgreSQL's logical replication stream (`pgoutput`), in order.
2. **Decode into a fixed buffer.** Each change is decoded into a pre-allocated slot — memory is bounded at startup, not grown per event.
3. **Batch to NATS.** Decoded events are published to JetStream in batches, closed by a count, an age or a size (see [Configuration](#configuration)).
4. **Acknowledge.** Only after JetStream confirms does the bridge ACK that position to Postgres (no data loss).
5. **Reclaim**: Postgres can then reclaim WAL. If the bridge crashes, Postgres keeps the unpublished WAL — nothing is lost.

```txt
PostgreSQL WAL → Bridge → NATS JetStream
              ↑            ↓
              └─── ACK after JetStream confirms
```

Egress (Postgres → NATS) is a **push**: the bridge publishes as changes happen.

**Backpressure**: NATS slow/full → Bridge can't get JetStream ACK → Bridge stops ACK'ing PostgreSQL → WAL accumulates.

### The NATS/Consumer loop

Bootstrap and ingress (consumer ↔ NATS) are **pull**: the consumer pulls CDC and chain objects at its own pace and publishes its writes on its own subject.

The client keeps its own position: the last stream sequence it applied, stored in its replica. After a restart it reads on from there. The CDC streams use limits retention, so an event ages out after `CDC_MAX_AGE_SECONDS` whether anyone read it or not; a client that fell behind that window re-seeds from the chain.

**Backpressure**: a slow client only falls behind; neither the stream nor the bridge waits for it.

### Seeding

A client that missed part of the stream reloads from the generation chain, a snapshot per table and tenant the bridge cuts on a cadence; see [Catching up: the chain and the stream](#catching-up-the-chain-and-the-stream).

### Memory Management

The ring buffer is pre-allocated once at startup, in three parts: a fixed-size event slab, a data slab for row bytes, and a columns slab for column descriptors. Decoding a WAL message writes column values directly into that pre-allocated space — there is no per-column heap allocation on the hot path, and nothing to free afterward. The SPSC queue between the two threads carries only slot indices, not owned data; a slot is returned to the free pool once its batch is published, and the next event reuses the same memory.

**Arena allocator** (a separate, smaller one, for the encode/publish step):

- Reused once per flush, reset (not freed) between flushes.
- Backs the MessagePack value trees built for one batch.
- Avoids one malloc per column per event.

### Replication Slot Management

In short:

**On startup:**

1. Bridge creates replication slot (if not exists)
2. On a new slot, starts from the current LSN (no history); on an existing slot, from its confirmed position

**During operation:**

- Bridge sends status updates every 100ms or 1MB of data, whichever comes first
- PostgreSQL prunes WAL up to last ACK'd LSN

**On shutdown:**

- Bridge sends final ACK with last confirmed LSN
- Postgres' replication slot preserves position for restart

There is **no automatic slot cleanup** because an instance can be stopped and restarted on the fly, as a normal process.

> [!WARNING]
> If you retire an instance (say `--slot my_slot`), drop its slot: otherwise PostgreSQL keeps all WAL from the slot's last position, and it grows without limit.

In details: A bridge creates its replication slot at its first start and leaves it in place when it stops: the slot is the bridge's bookmark in the WAL, and a restart resumes from it without a client noticing. That bookmark is also a promise PostgreSQL keeps on the bridge's behalf. Every WAL segment written after the slot's `restart_lsn` is retained on disk until the slot confirms it, so a slot that nobody reads, an instance stopped for good or renamed, keeps the WAL growing at the database's full write rate while every dashboard of the bridge that matters stays green. Three settings decide how far that goes:

| setting | role | what it means for a slot |
| --- | --- | --- |
| `max_wal_size` | a checkpoint trigger, not a limit | PostgreSQL starts a checkpoint when WAL grows past it; a checkpoint recycles segments only if nothing still needs them. A slot needs them, so WAL grows past this value without a word |
| `wal_keep_size` | a floor | segments kept for standbys that stream **without** a slot; `0` by default and irrelevant to the bridge, which has one |
| `max_slot_wal_keep_size` | the hard limit | once a slot's retained WAL passes it, the next checkpoint discards those segments and marks the slot `lost`. The bridge refuses to start on a lost slot: drop it, start once with `ZB_FEED_RESTART=1`, and every client re-seeds from a fresh full |

`-1`, PostgreSQL's default for the hard limit, means "retain for ever", which is a full disk instead of a lost slot; a value like `10GB` turns the worst case from an outage of the database into a re-seed of the clients. PostgreSQL also states the runway itself: `pg_replication_slots.safe_wal_size` is how many more bytes can be written before the slot is invalidated, and `wal_status` moves from `reserved` through `extended` and `unreserved` to `lost` on the way. The bridge's inventory publishes the retained bytes per slot on `/metrics`, warns at boot when its own slot reads `unreserved`, and gives an operator the whole picture from a shell:

**The CLI commands**:

```sh
bridge --view-slots                                   # every slot: active, pid, LSNs, retained WAL
bridge --view-slot my_slot                            # one slot
ADMIN_DATABASE_URL=xxx bridge --drop-slot my_slot     # an INACTIVE slot only; frees its retained WAL
```

The views read with the bridge's own `DATABASE_READER_URL`. The drop needs an admin URL passed for the invocation, refuses an active slot (stop that bridge first), and says how much WAL it freed. An inactive slot whose retained figure grows is an abandoned instance.

💡 how is my slot going?

```sql
#psql>
SELECT slot_name, active,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
  FROM pg_replication_slots
  WHERE slot_name = 'my_slot';

slot_name | active |   lag
-----------+--------+----------
 my_slot   | t      | 56 bytes

SELECT slot_name, active,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
  FROM pg_replication_slots;

 slot_name | active |   lag
-----------+--------+----------
 zb_probe  | f      | 1289 MB
 my_slot   | t      | 56 bytes
```

💡 Drop one if unused:

```sql
#psql>
SELECT pg_drop_replication_slot('my_slot');
```

💡 Clean the WAL with:

```sql
#psql>
CHECKPOINT;
```

#### When the slot is lost

A slot is lost when PostgreSQL discarded WAL the bridge had not read yet: the bridge was stopped too long, or fell too far behind, past `max_slot_wal_keep_size`; a failover to a standby that did not carry the slot; or a drop. The changes in that window are gone from the feed, and nothing in the CDC streams' numbering shows the hole, so the bridge refuses to resume on a lost slot. Recover once:

```sh
ADMIN_DATABASE_URL=xxx bridge --drop-slot my_slot   # if the lost slot is still listed
ZB_FEED_RESTART=1 bridge                              # the first start only, then remove it
```

`ZB_FEED_RESTART=1` makes the bridge's start on its new slot a new feed: the CDC streams are recreated empty, the next tick builds a full for every table, and every client re-seeds from it, so the lost window is filled from the database. It is opt-in because a new slot can also be a second bridge next to a running one, whose streams it must not delete. `bridge --diagnose` says which case you are in.

A client that stays connected notices by itself: its stream has a new creation time, so the library logs `stream recreated`, resets its position, re-seeds the table from the new full and reads on. The app does nothing; the re-seeded tables come back in the poll report's `seeded` (libzb), or as an `onChange` with no event (TypeScript).

### Reconnection Handling

**PostgreSQL reconnection:**

- Connection lost → Bridge waits 5 seconds
- Reconnects and resumes from the slot's confirmed LSN
- Metrics track reconnection count

**NATS reconnection:**

- Automatic (handled by `nats.zig`), retrying for ever; the timings are in [Configuration](#configuration)

### Catching up: the chain and the stream

A table reaches a client by two paths. The **stream** carries every change as it happens, and a client that keeps reading never needs anything else. The **chain** is for what a client missed when away: a snapshot the bridge cuts on a cadence, once per table and tenant, that any number of clients reload from without touching PostgreSQL.

#### What the producer builds

Every `GENERATION_CADENCE_SECONDS` the bridge walks the published tables and, for each tenant of each table that moved, cuts a **delta**: the rows whose version moved since the last cut, tombstoned rows included, because a tombstone is how a delete travels.

Every `GENERATION_CHECKPOINT_SECONDS` a background lane cuts a **checkpoint**: every row that moved since the previous checkpoint, tombstones included. It is the middle level — one object that covers a whole window of deltas, so a client that was away for hours walks a handful of checkpoints instead of hundreds of deltas, and never reloads the table for being away.

A **base** — the live rows only, since a client applies it as a wipe and a reload — is cut when only a base can carry what happened (a hard delete on a table without the soft-delete guard, a truncate, a changed column shape, a re-seed asked with `zebridge_reseed`), when a delta would carry more than half the table (a burst re-stamping the same rows would otherwise put them in every delta), and, routinely, when the checkpoints above it weigh more than it does (`GENERATION_BASE_REBUILD_PERCENT`): past that bar a returning client pays more to walk them than to reload. `GENERATION_CHAIN_DEPTH` rotates a base in only when the lane has stalled and no checkpoint stands above the base. The objects are MessagePack compressed with zstd, streamed straight into the tenant's object store — the producer never holds a table in memory — and a manifest in the `generations` bucket names the chain.

A client that was away applies the deltas cut after its watermark when the oldest of them begins at or before it, so the chain continues from where the replica stands; when no kept delta reaches that far back, it applies the checkpoints that cover the absence and then those deltas; and it reloads the base only when nothing covers the absence, or when its watermark is older than the sweeper's `gc_watermark` — past that, a delete it missed may have had its tombstone reaped, and only a base states absence. Either way it then resumes the stream at the sequence recorded in the manifest.

The `gc_watermark` is the sweeper's marker (the one row of `zebridge_gc_watermark`): no tombstone older than it is guaranteed to still exist. The client reads it from the chain's manifest, since its own copy is as old as the client.

#### The one rule

That resume works only if the stream still holds the manifest's cut. The newest cut is at most one cadence old, so the stream must keep at least **two cadences** of events, plus the time a build takes and the time a client needs to apply it. `CDC_MAX_AGE_SECONDS` defaults to three cadences, and `bridge --diagnose` refuses less than two. The producer builds one table for one tenant at a time and walks every such pair in turn, so a tick costs the sum. Measured on a laptop over local TCP, one pair:

| step | time |
| --- | --- |
| the pair holds 75,000 rows (19 MB raw, 2.7 MB compressed): the full built, compressed, uploaded, manifest written | 0.21 s |
| the same table on a tenant with no rows to speak of: the fixed cost of any build (connection, snapshot, queries, upload, manifest) | 56 ms |
| a fresh libzb replica applies the 75,000-row full | 1.6 s |
| a fresh browser or Node replica applies it | 1.6 s |

So a build costs about 60 ms plus 2.3 µs per row, and 60 ms + 75,000 × 2.3 µs is the 0.21 s above; a delta of the same rows costs the same again. Every generation's log line carries its build time and its breakdown (query, encode, zstd, upload), so a deployment reads its own numbers and sums them per tick.

Two other limits end the stream's window before the age does: `CDC_MAX_BYTES` and `CDC_MAX_MSGS`. They are disk valves, and a burst of large rows can make them cut the window to seconds while the age still reads as three comfortable cadences. The fleet monitor measures the window each stream really holds (`bridge_cdc_window_seconds`) and flags one that is pruning under two cadences (`bridge_cdc_window_short`).

The producer does not wait for the cadence when a stream moves fast. The publisher marks a stream hot the second a burst starts, and the producer then re-cuts any table whose cut is about to fall off that stream, an empty delta if nothing moved, so a returning client always finds a cut it can splice on. The reaction time is a second; what it cannot beat is a burst that fills a whole cap inside that second, and a cap should therefore hold at least a few seconds of the worst burst you expect.

#### When the rule breaks

Nothing resumes past a hole. A client whose manifest's cut is older than the stream's oldest message says `chain predates the stream` and waits; the producer sees the same thing at its next tick and cuts a delta with a fresh cut, empty if the table did not move, so the wait is at most one cadence. A client that stayed connected but stopped reading for longer than the window, a phone in the background, a throttled tab, sees the jump in sequence numbers on its next message and reloads from the chain at once. If the stream prunes faster than a build, the producer retries the build three times and then says so; the next step, pausing publication for one build while the WAL absorbs the burst, is not built yet.

The sweeper's clock is coupled to the same chain: what the chain promises must stay under `GC_THRESHOLD_MS`, or a tombstone can be reaped inside the window a client still catches up through. With checkpoints on, the promise is `2 × GENERATION_CHECKPOINT_SECONDS`; with them off, `GENERATION_CHAIN_DEPTH × GENERATION_CADENCE_SECONDS`. `bridge --diagnose` refuses a configuration that breaks it and the boot warns. The knobs are together in [Configuration](#configuration) below.

[⬆️](#table-of-contents)

---
