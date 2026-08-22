# ZeBridge server PostgreSQL  ↔ NATS

<p align="center"><img  width="355" height="233" alt="Screenshot 2025-12-26 at 02 37 57" src="https://github.com/user-attachments/assets/b3701ef4-2d58-497a-be21-52ad1b970644" /></p>

![Zig support](https://img.shields.io/badge/Zig-0.16.0-color?logo=zig&color=%23f3ab20)

**What is it?**: ZeBridge is an opinionated bidirectional daemon connecting `PostgreSQL` to the message broker `NATS/JetStream` for Edge sync.

**What can it do?**: Allows mobile, WASM ([PGLite](https://github.com/electric-sql/pglite), [SQLite](https://sqlite.org/wasm/doc/trunk/index.md)) web apps or microservices to sync with a Postgres database via NATS/JS without ever reaching for Postgres.

**Design**: This tool is built to serve a large number of small to medium consumers via the message broker NATS/JS, so it is designed to be fast and safe. It projects PostgreSQL onto NATS/JS. Consumers connect to NATS and keep their local database in sync.

By pushing all consumer state, caching, and rate-limiting down into NATS/JS, the ZeBridge binary is stateless toward consumers.

**What is it not**: ZeBridge serves a large number of small to medium consumers via NATS. It is NOT for large and long tables or tables containing large objects: it is not a file transfer tool. A large blob (>1 MB) belongs in object storage: tables should only contain the reference to a blob.

**Can I read and write from edge?**: It can be used as a **READ tool only**, or **bidirectional** with WRITE capacities:

* **READ**: it streams Postgres CDC events and table snapshots to a NATS/JS server your consumer is connected to.
* **WRITE**: it propagates a consumer's local writes back to Postgres via NATS/JS, which then sends a CDC event so the consumer updates its local state with last-write-wins (LWW) conflict resolution.

**How is data access protected?**: each consumer must have its own unique identity and segregated by tenants. Access per stream / bucket is granted by NATS and access by tenant is enforced by Postgres.

* ZeBridge can only read the declared tables and columns in Postgres, with a dedicated user.
* Access is scoped by tenant.
* The read-only configuration is the base; add RLS on top to filter CDC events and snapshots by tenant.
* The read/write configuration comes in two flavors: single tenant with full Postgres control, or multi-tenant.

**Performance**: you can expect 200 kevt/s. See [An example of a measured throughput](#an-example-of-a-measured-throughput) for a real, reproducible number — run it yourself on your own hardware before trusting any figure quoted here.

**Security**: read [SECURITY.md](SECURITY.md)

**Status**: Dev stage — see [Roadmap](#roadmap) for the current version and what's next.

See the Table of Contents below for configuration, scaling, evaluation, and telemetry.

## Table of Contents

* [Overview](#overview)
* [How ZeBridge compares](#how-zebridge-compares)
* [Understanding the app](#understanding-the-app)
  * [Roles and privileges](#roles-and-privileges)
  * [The NATS streams and buckets](#the-nats-streams-and-buckets)
  * [The memory setting](#the-memory-setting)
* [Design overview](#design-overview)
  * [Bridge ACK Flow and NATS outages](#bridge-ack-flow-and-nats-outages)
* [Quick review of PG & NATS setup](#quick-review-of-pg--nats-setup)
* [Consumer Integration Guide](#consumer-integration-guide)
* [Writing a consumer — the short version](#writing-a-consumer--the-short-version)
* [Running the Bridge](#running-the-bridge)
* [Monitoring & Telemetry](#monitoring--telemetry)
* [Safety & Guarantees](#safety--guarantees)
* [Inside](#inside)
* [Local Build Instructions](#local-build-instructions)
* [Configuration](#configuration)
* [Testing](#testing)
* [Notes on nkeys](#notes-on-nkeys)
* [Dependencies & Licenses](#dependencies--licenses)
* [Roadmap](#roadmap)

---

## Overview

A consumer connects to a NATS/JS (v2.10+) server facing ZeBridge connected to PostgreSQL (v14+).

**Example of Architecture**:

```mermaid
flowchart LR
    subgraph VPS["VPS / Private Network"]
        PG[("Postgres <br> Master")]
        subgraph Localhost ["VPS - Localhost"]
        Bridge["ZeBridge <br> daemon"]
        NATS[("NATS")]
    end

    PG <-- "TCP <br> SSL (opt)" --> Bridge
  
    Bridge <--> |"TCP" <br> only| NATS
    end
    NATS <-- TLS/WSS --> App


    subgraph Consumers["Consumers"]
        App["App"] <--> LocalDB[("Local_DB")]
    end
```

> [!NOTE] v0.14: NATS and ZeBridge need to be colocated (same host) since the communication between them is plain text only — see [Roadmap](#roadmap).

NATS has client libraries for 40+ languages, so instead of an SDK, we propose [PROTOCOL.md](PROTOCOL.md). It details the workflows and rules to connect consumers to the NATS server and sync the local-first database, with worked examples for Flutter, webapps (WASM-SQLite + OPFS), and backend microservices in Node, Python & Elixir (_TODO_).

The current version, `v0.14`, is summarized under [Roadmap](#roadmap), along with what `v0.16` and `v1.0` change.

## How ZeBridge compares

ZeBridge is an inflow daemon: it connects PostgreSQL and NATS, and nothing else. It proposes a protocol — a set of rules and workflows — to connect a consumer to NATS. Reads are scoped by Postgres tenant. Writes are authorized by NATS subject grants together with the Postgres tenant, and resolved last-write-wins. No sync-rules DSL to write, no gatekeeper service to run — authorization and conflict resolution both live where the data already does.

| tool | what it is | how it operates |
| --- | --- | --- |
| **[PowerSync](https://github.com/powersync-ja)** | Solves "Postgres to local SQLite" for offline-first apps, with a bucketing system. | Own stateful sync engine; clients connect over WebSocket (HTTP streaming as fallback). Authorization is a real DSL: Sync Rules filter per bucket via parameter queries against trusted JWT claims, evaluated centrally. Ships a default last-write-wins-per-field conflict resolution, server-authoritative and customizable — e.g. routing conflicts to a table for manual resolution instead. |
| **[ElectricSQL](https://electric-sql.com/)** | An open-source Postgres sync engine: consumers subscribe to "shapes" of the data, synced into local SQLite/PGlite. | Own stateful sync/shape service; clients long-poll over plain HTTP with CDN-cacheable headers, so a standard CDN collapses concurrent requests for the same shape into one origin hit. Read-path only — no built-in write path or conflict resolution at all; writes go through your own separate backend API. Authorization isn't built in either: a separate proxy/gatekeeper service in front of Electric decides who gets which shape. |
| **[Debezium](https://github.com/debezium/debezium)** | The enterprise standard for CDC — feature-rich, reliable, server-to-server. | Runs on the JVM, streams into Kafka. Built for datacenter-to-datacenter pipelines — no edge-client protocol, device-facing snapshot, or mobile/web target at all. |
| **[pgstream](https://github.com/xataio/pgstream)** | A modern Go CDC tool from Xata: DDL changes, streaming to Kafka/OpenSearch/Webhooks. | Generic pipeline routing — database to database or search index. No client-facing state machine, local schema translation, or snapshot handoff to a device. |
| **[Bento](https://github.com/warpstreamlabs/bento)** | A high-performance stream processor, source to sink, with on-the-fly transforms. | A general building block, not a sync product: wiring it to Postgres CDC gets you the transform/routing layer only — the edge-sync protocol, snapshot logic, and schema transition handling are left to build. |

## Understanding the app

ZeBridge is designed as a companion to the NATS/JS broker to enable Local-first sync of a Postgres database.

### Roles and privileges

The DBA configures PostgreSQL to emit CDC, migrates the publication, and creates the roles, grants and triggers a ZeBridge instance needs.

ZeBridge uses two (Postgres) USER:

* `bridge_reader` (SELECT + REPLICATION, physically unable to write),
* `bridge_writer` (no table privileges until a table is opened one at a time).

📖 **[SECURITY.md](SECURITY.md) is the reference**: what each role holds, what the schema must satisfy to be writable or tenant-scoped, what to do after a migration, and what is _not_ protected — with a table of where every claim is tested.

### The NATS streams and buckets

ZeBridge authenticates to the NATS server using nkeys.

ZeBridge uses three streams and two KV buckets for the bidirectional flow ZeBridge ↔ NATS ↔ consumer.
The naming is **shared** and declared in [topology.json](topology.json).

A ZeBridge instance is started with one config. The DBA starts the NATS server with its own config. `topology.json` is the shared grammar between the two: it is where stream names, subject prefixes, and KV bucket names are declared once, and the NATS setup (`nats-init`) must be built to match it exactly — not the other way around. Both sides authenticate with nkeys.

**Three data flows**:

1. **Bootstrap** (INIT stream, READ): the consumer requests a table's _schema_ and _snapshot_; ZeBridge delivers them: PG → ZeBridge → NATS/JS.
2. **Real-time CDC** (CDC stream, READ): the consumer receives INSERT/UPDATE/DELETE events as they happen: PG → ZeBridge → NATS/JS.
3. **Real-time ingress** (MUTATIONS stream, WRITE): the consumer updates its local storage and sends the intended change to NATS/JS → ZeBridge → PostgreSQL.

| Stream | Purpose | Retention (default) | Consumer Pattern | Role |
| -------- | ------------------- | ------------- | ----------------------- | -- |
| **CDC** | Real-time egress changes | 8 days | Continuous subscription | READ |
| **INIT** | Bootstrap snapshots | 7 days | One-time replay | READ |
| **MUTATIONS** | Real-time ingress changes | 7 days | Continuous subscription | WRITE |

These are hardcoded in `nats-init`'s stream setup (`docker-compose.full.yml`), not currently an env var — a deployment that wants a different window edits that script. Changing it does **not** retroactively apply to an already-provisioned stream: `nats-init`'s update path for an existing stream only touches subjects, never limits (see the script's own comment) — an operator has to `nats stream edit <name> --max-age=… -f` by hand, or start from a clean stream.

⚠️ **CDC deliberately outlives INIT, by a day.** Snapshot generation isn't instant — it's a single sequential worker with no timeout, so "a small table can wait behind a large one" (see [Thread Model](#thread-model-8-threads)) can genuinely delay how long a snapshot takes to finish landing in INIT. If CDC and INIT expired on the same clock, a CDC event for a write that happened _while_ a snapshot was still queued could age out before that snapshot itself does — leaving a client that replays a still-valid snapshot with a gap right at the start of what it needs from CDC. The one-day margin exists to absorb that queueing delay; INIT's 7 days is what actually matters operationally: **the longest a consumer can be offline before it must re-seed from a fresh snapshot instead of just resuming CDC.**

Snapshot _requests_ (not the data itself) are separately throttled: the `REQUESTS` stream holds one request message per table at a time, for up to the `SNAP_RET` window (`SNAP_RET_SECONDS`) — a second request for that table inside the window is refused, so a client is expected to check the `snapshots` KV bucket first. `SNAP_RET`'s production default is intentionally close to INIT's own 7-day retention (minus a 30-minute margin), not a short debounce: a snapshot already in INIT is good for its whole retention window, so a fresh dump of the same table before then would just be redundant load on Postgres. The KV descriptor itself has no expiry, so a client always has something to check regardless of where `SNAP_RET` currently stands.
Consumers use these streams to interact with NATS; the exact names are declared in `topology.json`.

### The memory setting

NATS comes with a `max_payload=1M` default.

**Snapshot**: Zebridge will suspend a table whose rows are wider than the NATS message limit (<1 MB). The NATS cap also means a consumer cannot push a large row to NATS. Above this limit, we are in the domain of Object storage for large blobs, and URLs should be saved in the database instead.

**CDC**: ZeBridge is designed to be fast, with a **fixed memory** defined at runtime.

The `RING_BUFFER_COUNT` is designed to buffer the received events during potential NATS jitters. Its count depends naturally upon the emitting rate.
The `BASE_BUF` is the max payload size, capped at 1MB.
The `MAX_COLUMNS` is the maximum number of possible columns per table. Unset (the normal case), it is **auto-detected at boot** from the widest table in the publication, rounded
up for migration headroom — not a fixed compile-time guess. Set it explicitly only to
override that.

It caps the event size, suspends a table and drives the total memory used.

Ceiling is NATS/JS, the host capactiy, not ZeBridge.
The defaults (ring=65536, BASE_BUF=14 → 16 KB rows, MAX_COLUMNS auto-detected at 8 for a `users`-shaped table) land around 1 GB, dominated almost entirely by the data slab — see the worked formula below.

❇️ Read [Sizing BASE_BUF and RING_BUFFER_COUNT](#sizing-base_buf-and-ring_buffer_count)

❇️ Read [Bridge ACK Flow and NATS outages](#bridge-ack-flow-and-nats-outages)

A ZeBridge instance, in one line:

```txt
one bridge instance = one replication slot = sequential processing
```

Two ways to run several instances, depending on the isolation you need:

* **Multi-tenant instance**: one bridge, one slot, serving several tenants — cheaper, but every tenant's data flows through the same process.
* **Single-tenant instance**: one bridge per tenant, enforced at the PostgreSQL level — more processes, but a tenant's data never crosses another's.

Before starting a bridge:

* Postgres has run the needed migrations and has a `PUBLICATION` with WAL logging enabled.
* NATS has been configured with the streams and buckets.

The bridge is then started with:

* a memory budget: `BASE_BUF` (default 2^14 = 16 KB) and `RING_BUFFER_COUNT` (default 65536), sized to the tables this instance handles — see [Sizing BASE_BUF and RING_BUFFER_COUNT](#sizing-base_buf-and-ring_buffer_count). `MAX_COLUMNS` is usually left unset and auto-detected.
* a unique `--slot` — the WAL pointer PostgreSQL keeps for this instance. Each running instance needs its own.
* a unique `--port` for its telemetry webserver. Each running instance needs its own.
* `--top`, defaulting to `./topology.json` — the stream/bucket names shared between ZeBridge, NATS and consumers.
* the mandatory `NATS_NKEY_SEED` env var — the private half of the public nkey the NATS server was given.
* `DATABASE_URL`, `DATABASE_WRITER_URL`, `NATS_URL` — the connection strings.

For example, one instance on the publication `my_pub` (created by the DBA) with the slot `my_slot`:

```sh
BASE_BUF=10 \                     # default 14
RING_BUFFER_COUNT=4096 \          # default 65536
NATS_URL=nats://127.0.0.1:4222 \  # default value
BRIDGE_PORT=9090 \                # default port
NATS_NKEY_SEED=SU... \            # mandatory
DATABASE_URL=postgres://bridge_reader:bridge_password_changeme@127.0.0.1:55432/postgres \
DATABASE_WRITER_URL=postgres://bridge_writer:writer_password_changeme@127.0.0.1:55432/postgres \
./bridge --slot my_slot --pub my_pub --top topology.json
```

**Principal authentication** (the end user of a consumer app):

The principal is authenticated by the consumer app, and carried through NATS's JWT/operator model so the bridge can pass it to Postgres for RLS policies.

|  |  subscribe  |   publish  | needs an account |
|--|--|--|--|
| read-only consumer  | cdc.>, init.>, KV.schemas.>, KV.snapshots.> | snapshot.request.>       | no |
| read-write consumer | the same | + mutation.`<principal>`.> | yes |

## Design overview

It projects PostgreSQL onto NATS/JS, and never reads its own output back.
PostgreSQL is the source of truth of ZeBridge for anything about the catalogue.
NATS is the source of truth for consumers.
Lastly, consumer state always arrives through CDCs: no optimistic WRITE.

**Key features of the bridge**:

* PostgreSQL _proto-v1_ streams using logical replication (`pgoutput` binary format)
* Publishes schemas from the catalogue to NATS KV store on startup,
* Generates table snapshots on-demand, chunked by **bytes** to fit one NATS message (10 000 rows is only a ceiling), via NATS requests,
* tenants management,
* Triggers message to NATS on schema change via Postgres DDL event triggers,
* schemas are available in two formats: PostgreSQL(eg for PQLite) and SQLite,
* `MessagePack` default encoding for CDCs and snpashot,
* last-write-wins (LWW) strategy for writes,
* At-least-once delivery with idempotent message IDs,
* Graceful shutdown with LSN acknowledgment,
* telemetry via HTTP `/metrics` (Prometheus format) and structured logs on **stderr** (Grafana Loki)

**Key decisions**:

* `REPLICA IDENTITY DEFAULT`.
* Every table needs a **single-column primary key**. A composite key, or no key at all, fails preflight and the table is refused, loudly, rather than replicated unreliably. Migrate the table to add one if it is missing.
* **One WAL-reading thread per bridge.** PostgreSQL's WAL is itself sequential, so this keeps LSN acknowledgment simple — there is no cross-thread ordering to reconcile. (The bridge is not single-threaded overall — see [Thread Model](#thread-model-8-threads) for the full picture.) One replication slot can carry one or several tables.
* Tenant scoping via RLS policies.
* ➡️ Scale horizontally (multiple bridges) instead of vertically. The PostgreSQL admin creates the publication and scope (_pub_name_ for which tables). Run multiple bridge instances with different replication slots - limited to `max_replication_slots=XX` in master config - and reach PostgreSQL instances.
You can divide the workload per PostgreSQL instance, and attach bridge instances to differents scopes (specific tables). Each bridge instance can scale its builtin buffer taylored to the needs of the table(s) with `BASE_BUF` and `RING_BUFFER_COUNT`.
* No SDK but a _HOW TO SYNC_ workflow and _NAMING CONVENTIONS_ to follow client side: demanding schema, running migration, seeding from the snapshot, playing the CDCs. Snippets are proposed in JS, Elixir, Python, NodeJS, Go.

```bash
# Bridge 1: table "users" with publication "my_pub_1" on master
DATABASE_URL=postgres://bridge_reader:pw@localhost:5432/postgres \
  ./bridge --slot slot_1 --pub my_pub_1 --port 9090

# Bridge 2: table "orders" with publication "my_pub_2" on replica_1
DATABASE_URL=postgres://bridge_reader:pw@replica_1:5432/postgres \
  ./bridge --slot slot_2 --pub my_pub_2 --port 9091
```

* Encoding. The default is **MessagePack**: compact (~30% smaller than JSON), type-safe (it preserves int/float/binary distinctions, where JSON sends everything as strings or generic numbers), and fast to encode/decode. Libraries exist for most languages.

### Bridge ACK Flow and NATS outages

```txt
PostgreSQL WAL → Bridge → NATS JetStream
              ↑            ↓
              └─── ACK after JetStream confirms
```

1. Bridge receives WAL event from PostgreSQL
2. Bridge publishes to NATS JetStream (async)
3. **JetStream confirms** message is durably persisted (file storage)
4. Bridge ACKs that LSN to PostgreSQL
5. PostgreSQL can safely prune WAL up to that LSN

Bridge only ACKs after NATS has the data (no data loss). Then PostgreSQL can reclaim disk space safely.
If bridge crashes, PostgreSQL retains unpublished WAL

**Backpressure:**: NATS slow/full → Bridge can't get JetStream ACK → Bridge stops ACK'ing PostgreSQL → WAL accumulates

The NATS ACK flow remains "standard" outside of the bridge scope:

```txt
NATS JetStream → Consumer
       ↑              ↓
       └──── Consumer ACKs (or NAKs)
```

1. Consumer pulls messages from JetStream
2. Consumer processes message
3. Consumer ACKs to JetStream (or NAKs on error)
4. JetStream tracks consumer position (durable consumer)
5. JetStream can prune messages acknowledged by all consumers

The consumer controls replay (NAK → redeliver). Its durable name survives restarts, and multiple consumers can each track independent positions.

**Backpressure**: consumer slow → JetStream buffers → consumer catches up at its own pace. Retention policies prevent unbounded growth in the meantime.

Snapshots themselves — requesting one, replaying it, and the two-clock (`seq`/`lsn`) bookkeeping around it — are covered from the client's side in [Writing a consumer — the short version](#writing-a-consumer--the-short-version); this section is about the bridge's own guarantees, not the client protocol.

---

## Quick review of PG & NATS setup

This paragraph is only a rough overview of the operations needed to set up PostgreSQL and NATS/JetStream.
In short:

* you enable PG logical replication, run migrations to creates users, grants, and publication. The database itself is another separate migration from these admin setup,
* you configure NATS to run JetStream, setup the authentication based on NKEY between ZeBridge and NATS, create streams and buckets.

The exact setup is in [init.core.template.sql](init.core.template.sql), [init.write.template.sql](init.write.template.sql), [docker-compose.full.yml](docker-compose.full.yml), and [nats-server.conf.template](nats-server.conf.template).

**Enable PG Logical Replication**: on host, `postgresql.conf` or in Docker command:

```sh
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
max_slot_wal_keep_size = 10GB
wal_sender_timeout = 300s  # 5 minutes
```

Via `psql` (`psql -h localhost -p 55432 -U postgres -d postgres`) or a command in Docker:

**CREATE Publication**: the DBA creates a publication say `my_pub` on **specific tables** (or **all**).

```sql
CREATE PUBLICATION my_pub FOR TABLE users, orders;
```

Or for all tables:

```sql
-- Run as superuser
CREATE PUBLICATION my_pub FOR ALL TABLES;
```

**CREATE the two ZeBridge USER and GRANT**:

> [!NOTE] Zebridge has restricted USERs  "bridge_reader" and "bridge_writer" for security.

The Reader is restricted to SELECT on given tables + REPLICATION (least privilege).
The Writer has no table operation rights.

The DBA creates:

```sql
-- Run as superuser
CREATE USER bridge_reader WITH REPLICATION PASSWORD 'secure_password';
GRANT CONNECT, CREATE ON DATABASE postgres TO bridge_reader;
GRANT USAGE ON SCHEMA public TO bridge_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO bridge_reader;

-- Auto-grant for future tables
ALTER DEFAULT PRIVILEGES IN SCHEMA public
    GRANT SELECT ON TABLES TO bridge_reader;
```

See `init.{core,write}.template.sql` for a complete setup script.

**Enable NATS/JetStream**: The NATS admin must enable JetStream:

```sh
nats-server -js -m 8222

#via Docker:
docker run -p 4222:4222 -p 8222:8222 nats:latest -js -m 8222
```

**ADD Streams**: create streams eg 'INIT'

```sh
# INIT stream: Long retention for bootstrap data
nats stream add INIT \
  --subjects='init.>' \
  --storage=file \
  --retention=limits \
  --max-age=7d \
  --max-msgs=10000000 \
  --max-bytes=8G \
  --replicas=1
```

**ADD KV Bucket**: for example Schemas

```sh
nats kv add schemas --history=10 --replicas=1
```

**Configure Authentication NATS ↔ ZeBridge**: NATS and ZeBridge authenticate to each other with an nkey. One test pair is already provided; see [Notes on nkeys](#notes-on-nkeys) to generate your own.

```txt
# user public key:
NATS_BRIDGE_NKEY_PUB=DXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4
```

<details>
<summary>Example <code>nats.conf</code></summary>

```yml
port: 4222

jetstream {
    store_dir: "/data"
    max_memory_store: 1GB
    max_file_store: 10GB
}

accounts {
  BRIDGE: {
    jetstream: {
      max_memory: 1GB
      max_file: 10GB
      max_streams: 10
      max_consumers: 10
    }
    users: [
      { nkey: "${NATS_BRIDGE_NKEY_PUB}" }
    ]
  }
}
```

</details>

**Configure Authentication NATS ↔ Consumer**: Prefer the JWT/Operator mechanism. Transport only over TLS/WSS in production.

---

## Consumer Integration Guide

### Naming Conventions (The API Contract)

Because ZeBridge avoids a thick, opinionated SDK, these naming conventions _are_ the API contract. Clients must use these exact names and subjects to correctly synchronize with the backend.

Source: `topology.json`

#### 1. Schemas (NATS KV Store)

* **Bucket Name:** `schemas`

* **Key Convention:** `<table_name>` (e.g., `users`)
* **Client Action:** `kv.get('users')` to fetch the JSON schema definition.

#### 2. Snapshots (NATS JetStream)

* **Stream Name:** `INIT`

* **Subject Conventions:**
  * **Request:** `snapshot.request.<table_name>` (Client publishes empty payload here)
  * **Chunks:** `init.snap.<table_name>.<snapshot_id>.<chunk_index>` (Bridge publishes chunked data here)
  * **Metadata:** `init.meta.<table_name>` (Contains `Snapshot-LSN` and `Snapshot-Time` metadata)

#### 3. CDC Events (NATS JetStream)

* **Stream Name:** `CDC`

* **Subject Convention:** `cdc.<table_name>.<operation>`
  _(e.g., `cdc.test_types.insert`, `cdc.test_types.update`, `cdc.test_types.delete`)_
* **Client Action:** Subscribe to `cdc.<table_name>.>` starting from `opt_start_time = Snapshot-Time - 10s`. Filter out any messages where `msg.lsn <= Snapshot-LSN`.

#### 4. Local Client Mutations (NATS JetStream)

* **Stream Name:** `MUTATIONS`

* **Subject Convention:** `mutation.<table_name>.<operation>`
  _(e.g., `mutation.test_types.update`)_
* **Client Action:** The client publishes its local edge changes to this stream for the backend to ingest and resolve.

## Writing a consumer — the short version

`PROTOCOL.md` is the contract; this is the shape of it.

**Available examples**:

* a browser-based example: [App.tsx](/web-consumer/src/App.tsx)
* a [Flutter](/flutter) example
* [TODO] a `Python` microservice
* [TODO] a `Go` microservice
* [TODO] an `Elixir` microservice

**The startup flow, in order:**

```txt
1. Consumer starts, watches kv://schemas/  → local tables created/migrated
2. Compares stored seq against CDC first_seq
3. Gap or fresh → PUBLISH snapshot.request.<table>   (one per table per window)
4. Bridge serves requests sequentially, chunking by BYTES to fit one NATS message
   → init.snap.<table>.<snapshot_id>.<chunk>
5. Consumer replays chunks, sets table lsn = snapshot lsn
6. Consumer follows cdc.<table>.<op>, discarding events with lsn <= snapshot lsn
7. Consumer writes back on mutation.<principal>.<table>.<op>; the echo returns as CDC
```

In more detail, four things:

**Schemas arrive by subscription, not by asking**:  Watch the `schemas` KV bucket. The bridge writes every monitored table's schema at startup
and again on each DDL event, so the bucket is already current when you connect — a fresh
watch replays it. Create or migrate your local tables from what arrives; you never poll.

**Decide whether you need a snapshot — using `seq`, not `lsn`**: Persist **two** positions, because they are in different coordinate systems:

| stored | compared against | answers |
| --- | --- | --- |
| `seq` — JetStream stream sequence | the CDC stream's `state.first_seq` | _has the history I need fallen off the back of the stream?_ |
| `lsn` — PostgreSQL WAL position | a snapshot's `lsn`, per event | _have I already got this row?_ |

```js
const firstSeq = (await jsm.streams.info('CDC')).state.first_seq;
if (mySeq === 0 || (firstSeq > 0 && mySeq < firstSeq - 1)) {
  // fresh client, or my position expired → snapshot required
}
```

`first_seq` is JetStream's numbering, your LSN is PostgreSQL's; **there is no conversion
between them**, so the "did I fall behind?" question has to be asked in the stream's own
coordinates. (`- 1`, not `firstSeq`: if the oldest held is 100 and you hold 99, the next
message you need is 100 and it is still there.)

**If you need one: request, replay, then dedup by `lsn`**:

`PUBLISH snapshot.request.<table>` with an empty payload — the table is in the subject.
Then read the descriptor from the `snapshots` KV bucket and pull
`init.snap.<table>.<snapshot_id>.>`.

Two things to know:

* **One request per table per window.** The broker enforces it: a second request inside
  `SNAP_RET` is rejected with `503 … maximum messages per subject exceeded`. That is not an
  error to retry through — check the KV descriptor first and seed from the existing snapshot.
* **Snapshots are served one at a time**, so a request for a small table can wait behind a large one.

After replaying a snapshot taken at LSN X, set the table's `lsn = X` and **discard every CDC event with `event.lsn <= X`** — those rows are already in what you just seeded.

**Follow CDC, and gate on columns**: Resume the CDC consumer from your stored `seq`. Apply rows whose columns you know; **hold**
a row that carries a column your local table lacks until a newer schema arrives (§5 of
`PROTOCOL.md`). Gate on the _column set_, never on the LSN — most rows legitimately carry an
LSN newer than the last DDL, so an LSN gate stalls forever during quiet periods.

**Inspecting the local replica**: The client's SQLite lives in the browser's OPFS under a per-session name, so there is no
file for the `sqlite3` CLI. `web-consumer` exposes a console handle instead:

```js
await zb.q('SELECT id, name FROM users ORDER BY id')  // any SQL
await zb.count('users')                               // { n: 15 }
await zb.ids('users')                                 // [1,2,3,…] — diff against Postgres
zb.state()                                            // stored seq/lsn, per-table lsn, failed tables
```

`zb.ids()` is the one that matters when chasing a sync bug: compare it against
`SELECT id FROM users ORDER BY id` in psql. A row count alone will not find a **missing
middle** — and the LSN-boundary bug fixed on 2026-08-17 dropped exactly one row per
snapshot, which no count would have made obvious.

**Writing back (ingress)**:

Publish to `mutation.<principal>.<table>.<operation>`. Three things make this different from
a normal API call:

* **The principal is a subject token, not a payload field.** NATS authorises subjects, so a
  client granted `publish: ["mutation.alice.>"]` physically cannot write as anyone else. A
  `user_id` in the body would just be a claim.
* **Last write wins on a version column** — usually `updated_at`. Send the value you hold;
  the bridge applies the row only if yours is newer. A stale write is _rejected, silently by
  design_ — which is why §7.3's type table matters: a `timestamp` without time zone, or one
  with second precision, makes "newer" ambiguous and drops real edits.
* **Your write comes back as CDC**, like anyone else's. That echo is the confirmation.

⚠️ A mutation larger than the server's `max_payload` is rejected by NATS client-side, so the
write path is capped by construction — but the bridge's own per-event buffer (`BASE_BUF`,
16 KB by default) is _smaller_ than that ceiling. A 500 KB row can be accepted by Postgres
and then suspend the table when the CDC echo does not fit. Match `BASE_BUF` to what you
intend to write.

## Running the Bridge

The user of the bridge defines a `REPLICATION_SLOT` with the flag `--slot my_slot`. The bridge will create it.

### Command-Line Options

```txt
  --slot <NAME>     Mandatory: Replication slot name (default: cdc_slot)
  --pub <NAME>      Mandatory: use the Postgres `PUBLICATION`
  --port <PORT>     HTTP telemetry port (default: 9090)
  --help, -h        Show this help message
```

### Local Development (Bridge on Host Machine)

**Use case**: Development workflow where you run the bridge binary locally and connect to containerized PostgreSQL + NATS.

**Prerequisites**:

* PostgreSQL admin has created publication (e.g., `cdc_pub`) and bridge user (`bridge_reader`)
* NATS admin has created CDC/INIT streams, KV store, and credentials

**Step 1**: Start infrastructure containers:

```bash
docker compose \
  -f docker-compose.prod.yml \
  --env-file .env.prod up \
  postgres nats-server nats-init nats-config-gen -d
```

**Step 2**: Build the bridge locally:

```bash
zig build
```

**Step 3**: Run the bridge with environment variables:

```bash
DATABASE_URL=postgres://bridge_reader:bridge_password_changeme@localhost:5432/postgres \
NATS_URL=nats://bridge_user:bridge_secure_password@localhost:4222 \
./zig-out/bin/bridge --slot my_slot --pub cdc_pub

# With custom port and JSON encoding:
./zig-out/bin/bridge --slot my_slot --pub cdc_pub --port 9091 --json
```

**Environment variables reference** — `bridge --help` is the full list:

```bash
# PostgreSQL. URLs only: there is no PG_HOST/PG_USER fallback, on purpose (below).
DATABASE_URL=postgres://bridge_reader:pw@localhost:5432/postgres        # required
DATABASE_WRITER_URL=postgres://bridge_writer:pw@localhost:5432/postgres # unset = no ingress

# NATS. One address input; NATS_HOST is ignored with a warning.
NATS_URL=nats://[user:pass@]localhost:4222
NATS_NKEY_SEED=SU...

BRIDGE_PORT=9090          # HTTP telemetry, when --port is absent
```

#### Deployment requirement: the bridge sits with NATS

**v0.14** requires the bridge and `nats-server` on the same host, reached over loopback.

The vendored pure-Zig NATS client has no TLS, so a bridge→NATS hop across a network is plaintext; colocating removes the link rather than leaving it exposed.

`NATS_URL` accepts only `nats://` and rejects `tls://` for the same reason — a URL promising transport security the client cannot provide is worse than a refusal.

This constrains only that one hop. Clients still reach the broker from anywhere over its websocket port, and **Postgres may be remote**: `DATABASE_URL` carries `sslmode` in its query string and libpq does the work (untested as of 2026-08-16 — see `COPY_BINARY_PLAN.md`).

Lifting the constraint is **v1.0**: migrating to `nats-io/nats.zig`, which has
server-authenticated TLS and native nkey auth.

The minor version tracks the **PostgreSQL floor**: `v0.14` needs PG 14+ (where `pgoutput`'s
binary mode begins), `v0.16` needs PG 16+ (where logical decoding on a standby begins), and
`v1.0` is the NATS client swap. See `COPY_BINARY_PLAN.md` for what each buys.

#### Two families of credentials, two files

The variables above are the bridge's. A second family — `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DB` `POSTGRES_BRIDGE_*`, `POSTGRES_WRITER_*` — is the **admin** set: the superuser that `bridge-init` uses to run `init.sql` and the passwords it assigns to the roles it creates.
They live in `.env.admin` — together with `PG_PUBLISH_PORT`, `SNAP_RET_SECONDS` and
the nkey's public half, all of which are read by compose or by the init containers and

The bridge does not read any of them. The bridge reads `.env.bridge`. The bridge refuses to start without `DATABASE_URL`:

```bash
# the stack — .env.admin only: every service compose starts is an admin task
NATS_NKEY_SEED="SU..." docker compose -f docker-compose.full.yml \
  --env-file .env.admin up -d

# the bridge — .env.bridge only, so no superuser password enters this shell
set -a && source .env.bridge && set +a
NATS_NKEY_SEED="SU..." ./zig-out/bin/bridge --slot my_slot --pub my_pub
```

### Production Deployment (Full Container Stack)

**Use case**: Running everything in containers for production or production-like environments.

**What runs in containers**:

* PostgreSQL (with logical replication enabled)
* NATS server (with JetStream)
* Bridge binary (compiled and containerized)

**Command**:

```bash
# Start full stack (PostgreSQL + NATS + Bridge)
docker compose -f docker-compose.prod.yml --env-file .env.prod up --build -d
```

This uses `docker-compose.prod.yml` which includes the bridge container alongside PostgreSQL and NATS. See that file for complete configuration.

---

## Monitoring & Telemetry

The bridge provides telemetry through multiple channels:

```mermaid
flowchart LR
    B[Bridge Server<br/>Telemetry] -->|GET /metrics<br/>Prometheus format| Prometheus
    B -->|stderr<br/>Structured logfmt| Alloy/Loki
    B -->|GET /status<br/>JSON format| HTTP_Client
```

### Metrics or logs? Both — they answer different questions

They are not alternatives, and the suspension of a table shows why:

| | Prometheus (`/metrics`) | Loki (log lines) |
| --- | --- | --- |
| stores | numbers over time | text with labels |
| you get | `bridge_refused_tables 1` | `🔴 SUSPENDING 'orders': a row does not fit in the 4 KB per-event buffer (BASE_BUF=12)` |
| answers | **is** something wrong, since when, how often | **what** is wrong: which table, which LSN, what to change |
| good for | alerting, dashboards, trends | investigating after an alert fires |

A gauge is a float: Prometheus physically cannot hold the table name or the fix. Loki
can, but is poor at counting and alerting on rates. Alert on the metric, read the log
for the detail.

### Writing the log to a file

**Every log line goes to stderr** — including the periodic `METRICS` line and any panic
with its stack trace. Nothing is written to stdout, so `> logs.txt` captures an empty
file. Redirect with `2>`:

```bash
./zig-out/bin/bridge --slot my_slot --pub my_pub 2>> bridge.log
```

`LOG_LEVEL` (`debug|info|warn|err`, default `info`) decides what reaches the file. At
`info` the volume is small — the `METRICS` line every 15 s is ~5 700 lines/day — and
everything worth keeping is included. **Do not point a file sink at `debug`.**

⚠️ The level _names_ differ between input and output: you set `LOG_LEVEL=warn` but the
lines read `warning(scope):`. Both spellings are accepted for the variable; when
grepping or writing alert rules, match what the lines actually print:

```bash
grep -E '^(warning|error)\(' bridge.log
```

Rotate it, or it grows forever:

<details>
<summary><code>/etc/logrotate.d/zebridge</code></summary>

```conf
/path/to/bridge.log {
    daily
    rotate 14
    compress
    missingok
    copytruncate      # the bridge holds the fd open; it has no reopen-on-SIGHUP
}
```

</details>

Under systemd, skip the redirect entirely: journald captures stderr, and
`journalctl -u zebridge -p warning` gives the severity filter for free.

### 1. Prometheus Metrics Endpoint

**HTTP GET** `http://localhost:9090/metrics`, Prometheus text format, each metric with its own `# HELP`/`# TYPE` line.

<details>
<summary>Example output</summary>

```prometheus
bridge_uptime_seconds 331
bridge_wal_messages_received_total 1797
bridge_cdc_events_published_total 288
bridge_last_ack_lsn 25509096
bridge_connected 1
bridge_pg_reconnects_total 0
bridge_nats_reconnects_total 0
bridge_slot_active 1
bridge_wal_lag_bytes 51344
bridge_wal_confirmed_lag_bytes 2048
bridge_queue_usage_percent 0
bridge_cpu_seconds_total 7.600
bridge_max_rss_bytes 1526153216
bridge_refused_tables 0
bridge_refused_events_dropped_total 0
```

</details>

The four worth alerting on:

| metric | fires when | what it means |
| --- | --- | --- |
| `bridge_refused_tables` | `> 0` | a table is **suspended** — no primary key, an undecodable column type, or a row larger than the event buffer. The log line names which and why. |
| `bridge_refused_events_dropped_total` | `increase() > 0` | rows are being discarded right now for a suspended table |
| `bridge_wal_confirmed_lag_bytes` | rising steadily | **the bridge is behind**: WAL it has not confirmed yet. This is the backlog number. |
| `bridge_wal_lag_bytes` | large and growing across checkpoints | WAL PostgreSQL is _retaining_ on disk for the slot, until `max_slot_wal_keep_size` |
| `bridge_connected` | `== 0` | the replication stream is down |

⚠️ **The two lag metrics are not the same question.** `bridge_wal_lag_bytes` measures
from the slot's `restart_lsn`, which PostgreSQL only advances at checkpoints — so it
plateaus at a few MB on a perfectly healthy bridge and cannot tell you whether the
bridge is keeping up. `bridge_wal_confirmed_lag_bytes` measures from
`confirmed_flush_lsn`, which moves the moment the bridge ACKs. Alert on the confirmed
one for "the bridge is stuck", on the retained one for "the disk will fill".

`bridge_queue_usage_percent` is the ring buffer's fill level: sustained high values mean
NATS is not draining as fast as PostgreSQL produces, and the bridge is about to
back-pressure the WAL reader.

**How busy is the bridge?** `bridge_cpu_seconds_total` is a counter over all threads, so
`rate(bridge_cpu_seconds_total[1m])` gives cores used — `0.31` is a third of a core,
`1.0` is one core saturated, and a single-threaded reader that pins a whole core is
telling you it is the bottleneck. The same figure appears in the log as `cpu=31%` on
each `LOOP` line, which beats trying to isolate one process in `htop`.
`bridge_max_rss_bytes` is peak RSS: expect it to sit near
`2^BASE_BUF × RING_BUFFER_COUNT` plus ~400 MB of metadata, since the slab is
pre-allocated at startup rather than grown.

Configure Prometheus to scrape this endpoint:

```yaml
scrape_configs:
  - job_name: 'cdc_bridge'
    static_configs:
      - targets: ['localhost:9090']
```

### 2. JSON Status Endpoint

**HTTP GET** `http://localhost:9090/status` — the same data as `/metrics` (see the table above for what each field means), shaped as JSON for a human or a shell script rather than a scraper.

<details>
<summary>Example output</summary>

```json
{
  "status": "connected",
  "uptime_seconds": 331,
  "wal_messages_received": 1797,
  "cdc_events_published": 288,
  "current_lsn": "0/1832ce8",
  "is_connected": true,
  "pg_reconnect_count": 0,
  "nats_reconnect_count": 0,
  "slot_active": true,
  "wal_lag_bytes": 51344,
  "wal_lag_mb": 0,
  "wal_confirmed_lag_bytes": 2048,
  "cpu_seconds": 7.600,
  "max_rss_mb": 1455,
  "queue_usage_percent": 0,
  "refused_tables": 0,
  "refused_events_dropped": 0
}
```

</details>

### 3. Structured Log Metrics (for Grafana Alloy/Loki)

The bridge writes **every log line to stderr**, including the periodic metric line
below (every 15 seconds) and any panic with its stack trace. Nothing is written to
stdout — so redirect with `2>` or `2>&1`, not `>`:

```bash
./zig-out/bin/bridge --slot my_slot --pub my_pub 2>> /var/log/bridge/bridge.log
```

```log
info(bridge): METRICS uptime=376 wal_messages=67 cdc_events=6 lsn=0/217e280 connected=1 pg_reconnects=0 nats_reconnects=0 lag_bytes=17816 slot_active=1
```

The `LOOP` line next to `METRICS` every 15 s, which is the reader's profile:

```txt
LOOP iters=1407805 idle=10274 recv_ms=139 proc_ms=1494 cpu=31%
```

 field | reading |
| --- | --- |
| `iters` | WAL loop iterations in the interval |
| `idle` | iterations that found nothing and slept 1 ms — **high `idle` is good**: the bridge is waiting on PostgreSQL, not struggling |
| `recv_ms` | ms inside `receiveMessage` (libpq + framing) |
| `proc_ms` | ms decoding tuples and packing them into the ring buffer |
| `cpu` | process CPU over the interval, all threads |

⚠️ It is emitted from the WAL loop, so it starts once replication is running — not
during startup.

When you are ready to ship these to Loki, point Grafana Alloy at the file. Every line
is `level(scope): message`, and both halves make useful labels:

<details>
<summary>Grafana Alloy config</summary>

```hcl
loki.source.file "bridge" {
  targets    = [{__path__ = "/var/log/bridge/*.log", job = "zebridge"}]
  forward_to = [loki.process.bridge.receiver]
}

loki.process "bridge" {
  // "error(event_processor): 🔴 SUSPENDING 'orders': …"
  stage.regex {
    expression = "^(?P<level>debug|info|warning|error)\\((?P<scope>[a-z_]+)\\):"
  }
  stage.labels {
    values = { level = "", scope = "" }
  }
  // A panic is one event spread over ~10 lines; keep it as one entry.
  stage.multiline {
    firstline = "^(debug|info|warning|error)\\("
  }
  forward_to = [loki.write.default.receiver]
}
```

</details>

Then the queries that matter are `{job="zebridge", level="error"}` and
`{job="zebridge", scope="refused"}` — every suspension the bridge has ever declared.

⚠️ Do **not** use Alloy's `stage.metrics` to re-derive counters from the `METRICS` line.
Prometheus already scrapes those numbers from `/metrics`; a second, lossier copy that
only updates every 15 seconds is worse in every respect.

### 4. Health Check Endpoint

**HTTP GET** `http://localhost:9090/health`

Returns:

```json
{"status":"ok"}
```

Status: `200 OK` when bridge HTTP server is running.

Use for Docker health checks, Kubernetes probes, or load balancers.

### 5. Graceful Shutdown Endpoint

**HTTP POST** `http://localhost:9090/shutdown`

Triggers the same graceful shutdown as `SIGINT`/`SIGTERM` — see [Graceful Shutdown](#graceful-shutdown) for the exact sequence.

```bash
curl -X POST http://localhost:9090/shutdown
```

### 6. Stream Management Endpoints

**Get stream info:**

```bash
curl "http://localhost:9090/streams/info?stream=CDC" | jq
```

⚠️ **Known broken** (2026-08-15): this returns
`500 Failed to get stream info: error.JsonParseError`. The failure is in the vendored
NATS client's parse of JetStream's `STREAM.INFO` response, not in the endpoint itself.
Use the `nats` CLI meanwhile:

```bash
nats stream info CDC
nats stream subjects CDC
```

---

## Safety & Guarantees

### At-Least-Once Delivery

The full mechanism — bridge ACKs PostgreSQL only after JetStream confirms, what happens if the bridge crashes, what happens if NATS crashes — is covered in [Bridge ACK Flow and NATS outages](#bridge-ack-flow-and-nats-outages). The guarantee in one line: **no data loss between Postgres and NATS**, because the ACK to PostgreSQL only happens after JetStream has durably persisted the message, and JetStream's Msg-ID deduplication absorbs any retry.

### Zero-Consumer Protection & Storage Bounds

**What happens if PostgreSQL emits CDC events when no NATS clients are connected?** Nothing accumulates unbounded on either side. The bridge keeps ACKing PostgreSQL as normal — that only depends on JetStream, not on a consumer being present — so PostgreSQL's WAL stays bounded regardless. On the NATS side, the `CDC` stream's own retention policy (`--max-age`, `--max-bytes` — see [The NATS streams and buckets](#the-nats-streams-and-buckets)) purges old events even with zero subscribers. A client that reconnects after being offline just requests a fresh snapshot (`INIT` stream) and resumes from `CDC`.

### Idempotent Delivery

**Message ID pattern:** `{lsn}-{table}-{operation}`

* Example: `25cb3c8-users-insert`

**NATS JetStream deduplication:**

* Duplicate Msg-IDs are rejected
* Ensures exactly-once semantics even with retries

### Durability

**PostgreSQL side:**

* Logical replication slot preserves WAL
* `max_slot_wal_keep_size=10GB` prevents unbounded growth

**NATS JetStream side:**

* File storage (`.storage=file`) survives restarts
* Durable consumers track position across restarts

**Consumer side:**

* Durable consumer name persists progress
* Survives consumer restarts

### Schema Consistency

Each snapshot carries the LSN it was taken at, so a consumer can reconstruct exact table state at that point — and discard any CDC event at or before it as already included (see [Writing a consumer](#writing-a-consumer--the-short-version)).

Event ordering follows from [one WAL-reading thread per bridge](#design-overview): PostgreSQL's WAL is already sequential, the bridge doesn't reorder it, and JetStream delivers in the order it received.

### Graceful Shutdown

**Shutdown sequence:**

1. Signal handler (SIGINT/SIGTERM) sets stop flag
2. Main thread finishes processing current WAL message
3. Batch publisher drains internal queue
4. Bridge sends final ACK to PostgreSQL (last confirmed LSN)
5. All threads join cleanly

**Guarantees:**

* No in-flight events lost
* PostgreSQL knows exact resume point
* Clean restart from last ACK'd LSN

---

## Inside

### Thread Model (7 threads)

Every one is spawned once at startup and lives until shutdown. Nothing is spawned per
request, per table or per connection.

| # | thread | spawned at | does |
| --- | --- | --- | --- |
| 1 | **Main** | — | consumes the WAL stream, parses pgoutput, packs rows into the ring buffer. `EventProcessor` runs _inline_ here, so "the CDC path" and "the main thread" are the same thread |
| 2 | **Batch publisher** | `batch_publisher.zig:534` | drains the ring buffer, encodes MessagePack, publishes to the `CDC` stream |
| 3 | **WAL monitor** | `wal_monitor.zig:50` | replication-slot lag, every 30 s |
| 4 | **HTTP telemetry** | `http_server.zig:49` | `/metrics`, `/health`, `/status`, `/shutdown`. Accepts and serves inline — no thread per connection |
| 5 | **Snapshot supervisor** | `snapshot_listener.zig:710` | spawns 6, then sleeps until shutdown and joins it. Does no work of its own |
| 6 | **Snapshot requests** | `snapshot_listener.zig:745` | consumes `snapshot.request.>` and generates snapshots **inline** — one at a time, in arrival order |
| 7 | **Mutation listener** | `mutation_listener.zig:137` | consumes `mutation.>`, applies writes under the ingress role. Only started when `DATABASE_WRITER_URL` is set |

There used to be an eighth, a schema-request listener answering `init.schema` on its own
connection — removed along with the on-demand `init.schema.<table>` request/response
mechanism it served (PROTOCOL.md §1): an earlier design that predates the current push
model, where the bridge writes every table's schema straight into `$KV.schemas.<table>`
at boot and on every DDL change, and a client just watches that bucket.

Two things this table is meant to stop people assuming:

* **Snapshots are not concurrent.** Thread 6 calls `generateIncrementalSnapshot` inline, so
  a large table blocks every other table's request behind it in the stream. Requests are
  durable so nothing is lost, and the ack happens on receipt rather than on completion —
  but a small table waits for a big one. (`Config.Snapshot.max_concurrent_snapshots`
  states an intent that was never built; it is read by nothing.)
* **Preflight is not a thread.** It is an inline call in `main` (`bridge.zig:377`) that runs
  _before_ threads 5–7 exist, which is why it can write the refusal registry without
  synchronising with anyone.

#### Who touches the refusal registry

`refused_tables.Registry` is shared by the CDC and snapshot paths — one table must not be
refused by one and served by another. Three roles reach it:

| | role |
| --- | --- |
| main thread (1) | writes (DDL path) **and** reads (`shouldDrop` per event) |
| snapshot requests (6) | writes (a row too wide to publish) **and** reads (rejects requests) |
| HTTP telemetry (4) | reads only, for `/metrics` and `/status` |

Two concurrent _writers_ and a concurrent _reader_ is why the registry is an append-only
array with atomic publication rather than a hash map with a lock — see the header of
`src/refused_tables.zig`.

### Data Flow

```txt
PostgreSQL WAL
    ↓
Main Thread (parse pgoutput)
    ↓
SPSC Queue (lock-free) — producer: main thread, consumer: batch publisher, 65536 slots by default
    ↓
Batch Publisher Thread
    ↓ (batch: 5000 events OR 500ms OR 256KB)
MessagePack Encoding
    ↓
NATS JetStream (async publish)
    ↓ (JetStream ACK)
PostgreSQL LSN ACK
```

The SPSC queue serves two purposes: it decouples WAL reading from NATS publishing (so a slow flush never blocks the WAL reader directly), and it absorbs a NATS outage without losing anything — see [Bridge ACK Flow and NATS outages](#bridge-ack-flow-and-nats-outages) for what happens when it fills. Sized by `RING_BUFFER_COUNT`; roughly 1 second of buffer at 60K events/s with the defaults.

### Memory Management

The ring buffer is pre-allocated once at startup, in three parts: a fixed-size event slab, a data slab for row bytes, and a columns slab for column descriptors. Decoding a WAL message writes column values **directly into that pre-allocated space** — there is no per-column heap allocation on the hot path, and nothing to free afterward. The SPSC queue between the two threads carries only slot **indices**, not owned data; a slot is returned to the free pool once its batch is published, and the next event reuses the same memory.

**Arena allocator** (a separate, smaller one, for the encode/publish step):

* Reused once per flush, reset (not freed) between flushes.
* Backs the MessagePack value trees built for one batch.
* Avoids one malloc per column per event that a naive implementation would otherwise pay.

### Replication Slot Management

**On startup:**

1. Bridge creates replication slot (if not exists)
2. Gets current LSN to skip historical data
3. Starts streaming from current LSN

**During operation:**

* Bridge sends status updates every 100ms OR 1MB of data, whichever comes first
* PostgreSQL prunes WAL up to last ACK'd LSN

**On shutdown:**

* Bridge sends final ACK with last confirmed LSN
* Replication slot preserves position for restart

### Reconnection Handling

**PostgreSQL reconnection:**

* Connection lost → Bridge waits 5 seconds
* Gets latest LSN
* Reconnects and resumes streaming
* Metrics track reconnection count

**NATS reconnection:**

* Automatic (handled by the vendored NATS client in `/nats.zig`)
* Max attempts: -1 (infinite)
* Wait between attempts: 2 seconds
* Flush timeout: 10 seconds

## Local Build Instructions

### Quick first run

* Zig 0.16.0 or later
* Docker & Docker Compose (for PostgreSQL and NATS)
* `libpq` **14 or later** — the only C dependency, linked from the system
  (`brew install libpq` on macOS, `apk add postgresql-dev` / `apt install libpq-dev` on Linux). Override its location with `zig build -Dlibpq-prefix=/path`.

  ⚠️ The version floor is on **libpq at build time, not on the PostgreSQL server**.
  The write path uses pipeline mode (`PQenterPipelineMode`, libpq 14+), which the libpq documentation describes as _"client-side and compatible with any server supporting the v3 extended query protocol"_ — so the database itself can be older (Postgres ≧ 14).
  It buys a mutation costing **one** network round trip instead of four, which is the difference between ~20 ms and ~80 ms per row against a remote database and is invisible when PostgreSQL is on the same host.

There is no vendored-library build step: the NATS client is pure Zig and lives in `/nats.zig`.

**NKEY**: a test public key is already supplied in `.env.admin`, and its corresponding seed is used below. See [Notes on nkeys](#notes-on-nkeys) to generate your own.

**Start Docker Infrastructure**: (PG, NATS, Prometheus, Grafana)

```bash
# Start PostgreSQL with logical replication enabled
docker compose -f docker-compose.full.yml --env-file .env.admin up -d postgres
```

**Build and run ZeBridge on host**:

```bash
zig build (-Doptimize=ReleaseFast)
#output -> ./zig-out/bin/bridge
```

```sh
set -a && source .env.bridge && set +a && \
NATS_NKEY_SEED="SUAPSL67RKOUDZFREHHDWUXDXLYZKEHMWEXMIUC35Z4Z2LXWP55SWVJS4Q" \
LOG_LEVEL=info \
./zig-out/bin/bridge --slot my_slot --pub my_pub
```

**Run the test consumer webapp**:

```sh
cd web-consumer
pnpm run dev
# localhost:5173
```

**Monitoring**:

Open localhost:3000

( _admin|admin_, then set the password of your choice)

**Run Tests**:

```bash
zig build test
```

`Python` and `envsubt` on host to play _/scripts/scenarios_

---

## Configuration

All configuration constants are centralized in `src/config.zig` and `topology.json`.

### Key Settings

**Snapshot configuration:**

* Chunk size: `10_000` rows per batch
* Subject pattern: `init.snap.{table}.{snapshot_id}.{chunk}`
* Metadata subject: `init.meta.{table}`
* Request subject: `snapshot.request.{table}`

**CDC configuration:**

* Batch size: `5000` events OR `500ms` OR `256KB` (whichever first)
* Subject pattern: `cdc.{table}.{operation}`
* Message ID: `{lsn}-{table}-{operation}`

**NATS configuration:**

* Max reconnect attempts: `-1` (infinite)
* Reconnect wait: `2000ms`
* Flush timeout: `10_000ms` (10 seconds)
* Status update interval: `1` second OR `1MB` data

**WAL monitoring:**

* Check interval: `30` seconds
* Warning threshold: `512MB`
* Critical threshold: `1GB`

**Fixed-size internal buffers:**

* Subject buffer: `128` bytes
* Message ID buffer: `64` bytes

The event ring itself (`BASE_BUF`, `RING_BUFFER_COUNT`, `MAX_COLUMNS`) is covered on its own below, since sizing it correctly matters far more than these two — see [Sizing BASE_BUF and RING_BUFFER_COUNT](#sizing-base_buf-and-ring_buffer_count).

See `src/config.zig` for all tunables.

### Sizing `BASE_BUF` and `RING_BUFFER_COUNT`

⚠️ read this one

These are not independent, and getting them wrong has a visible consequence rather than a silent one. The bridge pre-allocates the ring at startup, in **three parts**:

```txt
ring = ( 2^BASE_BUF  +  sizeof(CDCEvent)  +  MAX_COLUMNS × sizeof(ColumnView) )  ×  RING_BUFFER_COUNT
         ^ data:          ^ metadata:          ^ columns:                          ^ number of events
           max bytes        fixed, 328 B         8 B × MAX_COLUMNS — resolved        buffered ahead
           for ONE row      per event            at boot, not a compile constant     of NATS

  14 / 65536, MAX_COLUMNS=8   = 1024 MB data +  20 MB meta +  4 MB cols = 1048 MB  ← defaults, `users`-shaped table
  12 / 65536, MAX_COLUMNS=8   =  256 MB data +  20 MB meta +  4 MB cols =  280 MB  ← 4 KB rows
  11 / 262144, MAX_COLUMNS=8  =  512 MB data +  82 MB meta + 16 MB cols =  610 MB  ← many small events, ~1s at 200K evt/s
  20 /  1024, MAX_COLUMNS=128 = 1024 MB data + <1 MB meta +  1 MB cols = 1025 MB  ← 1 MB rows, wide table, minimum ring
```

`sizeof(CDCEvent)` is small and fixed regardless of table shape — `columns` is a _slice_
into a separate slab, not an inline array, so this term no longer grows with the widest
table you might ever replicate. `MAX_COLUMNS` is what used to be a single fixed 128 baked
into every deployment; it is now resolved **per instance, at boot**:

* Unset (the default): **auto-detected** from the widest table actually in the
  publication, rounded up to the next multiple of 8 for migration headroom (a table with
  6 columns → `MAX_COLUMNS=8`; see the "MAX_COLUMNS=…" line at boot).
* `MAX_COLUMNS=<N>`: an explicit override, clamped to 8–1600, that skips auto-detection —
  set it if you replicate a genuinely wide table, or want to fix the value across
  instances rather than let each one detect its own.

A table past the resolved `MAX_COLUMNS` is refused with `TooManyColumns` — loudly, with
its events dropped and its clients told why — rather than truncated.

⚠️ **The metadata + columns terms do not shrink with `BASE_BUF`.** So the smaller you make
the event buffer, the more they dominate: at `BASE_BUF=11` with a wide table's
`MAX_COLUMNS`, they can be a meaningful fraction of the slab, and the temptation to buy
outage tolerance by lowering `BASE_BUF` and raising the ring costs more than the
arithmetic on the data slab alone suggests.

Each knob answers a different question:

* **`BASE_BUF`** (log2 bytes, range 10–20) is _how large a single row may be_. Size it
  to your widest row: a `jsonb` document, a long `text` column, a big array.
* **`RING_BUFFER_COUNT`** (range 1024–1048576, **clamped** to the nearest bound if you go outside it — an out-of-range value used to fall back to the _default_, so asking for 64 got you 65536) is _how many events can queue while NATS is unreachable_. 65536 slots ≈ 1 second at 60K events/s. Below that, a NATS blip starts back-pressuring the WAL reader sooner.
* **`MAX_COLUMNS`** is _how many columns one event may carry_ — normally left to
  auto-detection; override it only to widen the ceiling ahead of a migration or to pin the
  value across instances.

> Raising one and lowering the other keeps memory flat, at the cost of outage tolerance.

#### Total memory: read it from the boot log, not a table

Because `MAX_COLUMNS` is now resolved per instance rather than a single fixed constant, a
static `BASE_BUF × RING_BUFFER_COUNT` table can no longer show the true total in one
number — the columns term depends on what _your_ publication's widest table looks like.
The bridge computes and logs the exact figure at startup instead, both knobs included:

```txt
info(bridge): MAX_COLUMNS=8 (auto-detected: widest monitored table has 6 columns, rounded up to a multiple of 8)
info(bridge): Event ring: 1048 MB of a 16384 MB limit (6%) — 1024 MB data + 20 MB metadata + 4 MB columns
```

That is the authoritative number for your deployment; the formula above is for
back-of-envelope estimates before you have a running instance to read it from. As a rule
of thumb: with `MAX_COLUMNS` at its auto-detected default for a normal table (well under
64), the data slab (`2^BASE_BUF × RING_BUFFER_COUNT`) still dominates.

<details>
<summary>Data-slab size for every BASE_BUF × RING_BUFFER_COUNT combination</summary>

Bold cells exceed half of a 16 GB machine, which is where the startup check refuses to
start (it reads the **cgroup** limit in a container, not the host's RAM) — add your own
metadata+columns figure from the boot log to know exactly how close you are.

| `BASE_BUF` | row cap | ring 1,024 | ring 8,192 | ring 65,536 | ring 262,144 | ring 1,048,576 |
| --- | --- | --- | --- | --- | --- | --- |
| 11 | 2 KB | 2 MB | 16 MB | 128 MB | 512 MB | 2,048 MB |
| 12 | 4 KB | 4 MB | 32 MB | 256 MB | 1,024 MB | 4,096 MB |
| 14 | 16 KB | 16 MB | 128 MB | **1,024 MB** ← default | 4,096 MB | **16.0 GB** |
| 16 | 64 KB | 64 MB | 512 MB | 4,096 MB | **16.0 GB** | **64.0 GB** |
| 18 | 256 KB | 256 MB | 2,048 MB | **16.0 GB** | **64.0 GB** | **256.0 GB** |
| 19 | 512 KB | 512 MB | 4,096 MB | **32.0 GB** | **128.0 GB** | **512.0 GB** |
| 20 | 1 MB | 1,024 MB | **8.0 GB** | **64.0 GB** | **256.0 GB** | **1,024.0 GB** |

`2^BASE_BUF × RING_BUFFER_COUNT` — data only. Add `(328 + MAX_COLUMNS × 8) ×
RING_BUFFER_COUNT` for the metadata+columns term, or just read it off the boot log.

</details>

⚠️ **`BASE_BUF` is a one-way door.** Raising it is free. Lowering it below rows already
stored means the next write that touches such a row suspends the table for every client —
preflight checks this at boot and refuses to let it pass silently (PROTOCOL.md §9).

#### Two things checked at startup, before a byte is allocated

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

On a healthy start you get the same arithmetic as a fact:

```txt
info(bridge): MAX_COLUMNS=8 (auto-detected: widest monitored table has 6 columns, rounded up to a multiple of 8)
info(bridge): NATS max_payload: 1024 KB (server-advertised) → CDC per-event buffer: 16 KB (BASE_BUF=14, ceiling 20) | snapshot per-chunk buffer: 1008 KB (ceiling 2048)
info(bridge): Event ring: 1048 MB of a 16384 MB limit (6%) — 1024 MB data + 20 MB metadata + 4 MB columns
```

#### What happens when a row does not fit

The table is **suspended**, and you will see this in the log:

```txt
🔴 SUSPENDING 'orders': a row does not fit in the 4 KB per-event buffer (BASE_BUF=12).
    Fix: restart with a larger BASE_BUF (each +1 doubles it, max 20 = 1 MB) …
```

What this does **not** do is stop the bridge. Every other table keeps replicating,
`bridge_refused_tables` rises, and clients of that one table receive a suspension on `$KV.schemas.<table>` (`"reason": "row_too_large"`) telling them their copy is frozen at a known LSN.
➡️ Restart with a `BASE_BUF` that fits and the table resumes; its clients
re-seed from a fresh snapshot.

⚠️ **This is why the metrics endpoint matters.** The event that overflows may arrive years after deployment — someone pastes a large JSON document into a text column — so this is not something you can verify once at install time.
🔔 Alert on `bridge_refused_tables > 0` (Prometheus) or on `SUSPENDING` in the logs (Loki).

#### The ceiling is NATS, not the bridge

`BASE_BUF=20` is 1 MB, which is also **nats-server's default** `max_payload`. A message also carries a subject, headers and MessagePack framing, so a row sized right up to the limit is still rejected at publish time. The bridge reads the server's advertised `max_payload` from its INFO line at connect and tells you where you stand:

```txt
info(bridge): NATS max_payload: 1024 KB (server-advertised) → CDC per-event buffer: 16 KB (BASE_BUF=14, ceiling 20) | snapshot per-chunk buffer: 1008 KB (ceiling 2048)
```

and warns if the two cannot coexist.
Raising `max_payload` in `nats-server.conf` is possible but affects every client and every subject on that server. JetStream's memory use scales with it — so for genuinely large values, prefer **keeping the blob out of the replicated table** and replicating a reference to it (URL object storage).

---

## Testing

### An example of a measured throughput

Same machine (Apple M2 Pro, 10 cores), same load (2,000,000-row burst, detailed below), two environments:

| environment | end-to-end rate |
| --- | --- |
| PostgreSQL + NATS in Docker on the same host | **~100k events/s** |
| PostgreSQL + NATS native on the host (no container/VM virtualization) | **200k+ events/s** |

The gap is the virtualization layer, not the bridge: the same binary, same code, same load — only Docker's I/O virtualization differs. If you're chasing a specific throughput number on your own hardware, measure both before assuming the bridge itself is the ceiling.

⚠️ Treat either figure as a reference point, not a spec — absolute throughput moves with machine, build mode, PostgreSQL version, and host load (a background process competing for CPU cores measurably drops it). A rerun that differs is not automatically a regression; see below for the number that _is_ comparable across machines.

<details>
<summary>Full method, raw output, and how to read the <code>LOOP</code> line</summary>

**Method** (Docker environment):

| | |
| --- | --- |
| machine | Apple M2 Pro, 10 cores, macOS |
| build | `zig build -Doptimize=ReleaseFast` — a Debug build is several times slower |
| PostgreSQL | 18.4 in Docker on the same machine |
| NATS | JetStream, file storage, in Docker on the same machine, **no consumers attached** |
| bridge | one instance, `BASE_BUF=14` (16 KB/event), `RING_BUFFER_COUNT=65536`, MessagePack |
| table | `users` — 4 small columns, single-column PK, `REPLICA IDENTITY DEFAULT` |
| load | 2000 statements × a 1000-row `INSERT … SELECT … generate_series`, each its own transaction = **2,000,000 rows** |

```bash
python3 -c "
for i in range(2000):
    print(\"INSERT INTO public.users (name,email,inserted_at,updated_at) \"
          \"SELECT 'User-%d-'||i, 'u%d-'||i||'@e.com', now(), now() \"
          \"FROM generate_series(1,1000) i;\" % (i,i))
" > load.sql
docker exec -i postgres-primary psql -U postgres -q -f - < load.sql
```

`generate_series(1,1000)` is PostgreSQL's set-returning function: it produces a thousand
rows, and `INSERT … SELECT … FROM generate_series` inserts one row per value — so each
statement writes 1000 rows in **one** transaction rather than 1000 round trips. That is
deliberate: the point is to saturate the WAL, and a client sending 2,000,000 separate
`INSERT`s would be measuring the client, not the bridge. 2000 statements × 1000 rows is
also 2000 _transactions_, so the WAL carries 2000 BEGIN/COMMIT pairs — visible as the gap
between `wal_messages` and `cdc_events` below.

**How to measure it.** The `METRICS` line is a 15 s sampler with no timestamp, so it
cannot answer "how long did this take" — poll the counter instead, which updates the
moment the batch publisher acks:

```bash
# in another shell, before starting the load
while :; do
  printf '%s %s\n' "$(date +%s.%N)" \
    "$(curl -s localhost:9090/metrics | awk '/^bridge_cdc_events_published_total /{print $2}')"
  sleep 0.5
done | tee drain.log
```

End-to-end rate is `2000000 / (t_at_2M − t_at_start)`. PostgreSQL's own write time is the
wall clock of the `psql` command above (`time docker exec …`). CPU is
`bridge_cpu_seconds_total` sampled the same way — subtract the endpoints rather than
reading the `cpu=%` field, which is a per-interval average.

⚠️ **Detach every CDC consumer first.** The figures below were taken with none attached,
and a browser client replaying 2M events into OPFS changes both the number and, usually,
the browser. Check with `nats consumer ls CDC`.

⚠️ **Host load matters more than you'd expect.** A CPU-bound process (the bridge) loses far more wall-clock time to a busy host than an I/O-bound one (PostgreSQL) — PostgreSQL mostly waits on disk either way, so background CPU contention barely touches its write time, while the bridge needs a core continuously and pays for every scheduling delay. A single unrelated process pinning even a few cores can cut measured throughput by half or more. Close anything CPU-heavy before trusting a number.

**Result:**

```txt
LOOP iters=1407805 idle=10274 recv_ms=139 proc_ms=1494 cpu=31%
METRICS uptime=15 wal_messages=1443766 cdc_events=1440690 …
LOOP iters=618169  idle=11431 recv_ms=90  proc_ms=616  cpu=16%
METRICS uptime=30 wal_messages=2004279 cdc_events=2000000 …
```

| measure | value |
| --- | --- |
| PostgreSQL writing 2M rows | ~6 s |
| all 2M events in JetStream | under 20 s from bridge start |
| end-to-end, producer included | **~100k events/s** |
| WAL loop busy time (`15 − idle`) | ~7.6 s → **~260k events/s** while draining |
| time inside libpq (`recv_ms`) | 0.9% of the interval |
| time decoding + packing (`proc_ms`) | 7.9% |
| CPU while draining | **31% of one core** (`bridge_cpu_seconds_total` rose 7.6 s in total) |

The number that _is_ comparable across machines is `iters` for a fixed event count. The
WAL loop runs roughly one iteration per WAL message, so 2M events should cost ~2M
iterations wherever it runs; a rerun within a few percent means the hot path is unchanged
even when the wall-clock figures differ.

**This benchmark is producer-bound in Docker**: PostgreSQL needs 6 s to write what the bridge drains in 7.6 s of loop time, and the loop is idle ~72% of the first interval — the ceiling is higher than 260k. Running the same load natively removes Docker's I/O virtualization and reaches 200k+ events/s end-to-end; the CPU-bound-vs-I/O-bound asymmetry above is why that gap exists.

**What it does not measure**: wide rows, `jsonb`/array-heavy tables, `REPLICA IDENTITY
FULL` (which doubles tuple volume), a remote PostgreSQL or NATS, consumers reading concurrently, or a NATS server under back-pressure. Each of those moves the number.

**Reading the `LOOP` line:**

| field | meaning |
| --- | --- |
| `iters` | WAL loop iterations in the interval |
| `idle` | iterations that found nothing and slept 1 ms — high `idle` means the bridge is waiting for PostgreSQL, not struggling |
| `recv_ms` | milliseconds inside `receiveMessage` (libpq + framing) |
| `proc_ms` | milliseconds decoding tuples and packing them into the ring buffer |
| `cpu` | process CPU over the interval, all threads — `100%` is one core saturated |

`recv_ms` approaching the interval length with `idle=0` is the signature of a reader
that cannot drain its socket — the exact shape of a bug this project has hit once already, in `wal_stream.zig`.

</details>

### HTTP Endpoint Tests

```sh
# Health check
curl http://localhost:9090/health

# Bridge status
curl http://localhost:9090/status | jq

# Prometheus metrics
curl http://localhost:9090/metrics

# Stream management
curl http://localhost:9090/streams/info?stream=CDC | jq

# Graceful shutdown
curl -X POST http://localhost:9090/shutdown
```

### Monitoring Replication Slot

```bash
docker exec -it postgres psql -U postgres -c "
  SELECT slot_name, active,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
  FROM pg_replication_slots
  WHERE slot_name = 'cdc_slot';
"
```

### Clear the WAL

```sh
docker exec -it postgres psql -U postgres -c "CHECKPOINT;"
```

---

## Notes on nkeys

* The NATS client has **no TLS**: `protocol.zig` sends `"tls_required":false`, so [bridge ↔ nats-server] must stay on a private network.
  External clients reach nats-server over wss, which nats-server terminates itself.
* Authentication to NATS uses **nkey**, signed with `std.crypto.sign.Ed25519` — pure Zig, no OpenSSL linked.
* PostgreSQL logical replication requires `wal_level=logical`

**How to generate nkey for NATS**:

```sh
brew install nats-io/nats-tools/nats
nk -gen user -pubout

# private: SUAAF6OSYEICIIMANGOM5WCIRDEILIMKLLQWOXPKB4DOVEDZN22CPMWVVI
# public: UDIUGKDO52EGKF5VUPIZBJEMY2HY7W6PM4TP2D4FUMQ2XX74BZXMAHCV
```

In _nats-server.conf_, set the **public** key:

```diff
authorization {
  users: [
-    # { user: "${NATS_BRIDGE_USER}", password: "${NATS_BRIDGE_PASSWORD}" } <-- remove
+    { nkey: "UDIUGKDO52EGKF5VUPIZBJEMY2HY7W6PM4TP2D4FUMQ2XX74BZXMAHCV"}
  ]
}
```

Use the **private** nkey to start ZeBridge daemon:

```sh
NATS_NKEY_SEED=SUAAF6OSYEICIIMANGOM5WCIRDEILIMKLLQWOXPKB4DOVEDZN22CPMWVVI \
RING_BUFFER_COUNT=2048 \
BASE_BUF=12 \
DATABASE_URL=postgres://bridge_reader:bridge_password_changeme@localhost:55432/postgres \
NATS_URL=nats://localhost:4222 \
./zig-out/bin/bridge --slot my_slot --pub my_pub
```

---

## Dependencies & Licenses

**Managed via `build.zig.zon`:**

* [zig-msgpack](https://github.com/zigcc/zig-msgpack) - MessagePack encoding. License MIT

**Currently vendored (in `/`):**

* [nats.zig](https://github.com/lalinsky/nats.zig). License Apache 2.

**System:**

* [libpq](https://www.postgresql.org/docs/current/libpq.html) ≧ 14. License MIT

---

## Roadmap

**Current — v0.14:**

* **PG ≧ 14** (`pgoutput` binary mode starts).
* **`libpq` 14+ at build time** (pipeline mode). Only the _client_ needs this version — the server just has to speak the v3 extended query protocol, so PostgreSQL itself may be older.
* `NATS` 2.14+, colocated with the bridge on plain TCP only (no TLS between them yet — see `v1.0` below).
* Per-instance memory sizing (`BASE_BUF`, `RING_BUFFER_COUNT`, `MAX_COLUMNS`), tailored to the tables that instance handles.
* Preflight schema analysis before any thread starts.
* Authorized & writable tables: RLS, GRANT, `SYNC_RULES`, single-column PK, tombstone column, last-write-wins.
* Tenant-scoped reads, in two shapes: N tenants behind one bridge instance (cheaper), or N tenants behind N bridge instances (stronger isolation).
* Telemetry webserver: `/metrics` for Prometheus.

**Next:**

* [ ] **v0.16**: Split READ (CDC + bootstrap) onto a **standby replica** (PG ≧ 16, where logical decoding on a standby begins) from WRITE on the primary, and enable async snapshotting on the replica.
* [ ] **v1.0**: Remove the NATS colocation requirement — `ZeBridge ↔ NATS` over TLS, migrating to [nats-io/nats.zig](https://github.com/nats-io/nats.zig) for server-authenticated TLS and native nkey auth.

**Possible enhancements:**

* [ ] Metrics export to StatsD/InfluxDB

**Open questions:**

* The documented burst benchmark is producer-bound under Docker: PostgreSQL needs ~6s to write 2M rows the bridge drains in ~7.6s of loop time. Running the same benchmark natively (no Docker/VM virtualization) removes that ceiling and reaches 200k+ events/s — see [An example of a measured throughput](#an-example-of-a-measured-throughput). What still isn't pinned down precisely is the bridge's own per-event CPU cost floor, independent of host contention — measured indirectly (CPU-seconds stay near-constant across runs even when wall-clock time doesn't), never isolated directly.

**Contributions welcome!** If you find it useful (or find gaps), feedback is valuable.
