# ZeBridge server PostgreSQL  ↔ NATS

<img width="755" height="433" alt="Screenshot 2025-12-26 at 02 37 57" src="https://github.com/user-attachments/assets/b3701ef4-2d58-497a-be21-52ad1b970644" />

![Zig support](https://img.shields.io/badge/Zig-0.16.0-color?logo=zig&color=%23f3ab20)

ZeBridge is an opinionated bidirectional daemon connecting `PostgreSQL` (14+) to the message broker `NATS/JetStream` (2.14+) for Edge sync.

Allows mobile, WASM ([PGLite](https://github.com/electric-sql/pglite), [SQLite](https://sqlite.org/wasm/doc/trunk/index.md)) web apps or microservices to mirror locally Postgres tables via NATS/JS without ever reaching for Postgres.

It is bidirectional: it transferts Postgres CDCs and table snapshots to a NATS/JS server, and propagates Consumer local optimistic writes back to Postgres via NATS/JS.

By pushing all state, caching, and rate-limiting down into NATS/JS, the ZeBridge binary is stateless toward consumers.

This is **not** a file transfer tool. Large blobs (>1 MB) is the domain of object storage.

ZeBridge exposes a raw, standard protocol for NATS/JS rather than requiring a proprietary SDK. See [PROTOCOLE.md].

**Performance**: this single threaded binary drained 2,000,000 CDC events in under 20 s end to end (PG → NATS) in the burst benchmark below — ~100k events/s including the time PostgreSQL itself needed to write them, and ~260k events/s while the WAL loop was actually busy. See [An example of a measured throughput](#an-example-of-a-measured-throughput) for the method, and run it yourself before trusting it.

**Status**: Dev stage, draft notes.

## Table of Contents

* [Architecture](#architecture)
* [How ZeBridge compares](#how-zebridge-compares)
* [Understanding the app](#understanding-the-app)
  * [The PG roles](#the-pg-roles)
  * [The NATS streams and buckets](#the-nats-streams-and-buckets)
  * [The memory setting](#the-memory-setting)
  * [How to start an instance](#how-to-start-an-instance)
* [Design overview](#design-overview)
  * [Bridge ACK Flow and NATS outages](#bridge-ack-flow-and-nats-outages)
  * [Snapshots](#snapshots)
* [Local setup](#local-setup)
* [Consumer Integration Guide](#consumer-integration-guide)
* [Running the Bridge](#running-the-bridge)
* [Message Formats](#message-formats)
* [Monitoring & Telemetry](#monitoring--telemetry)
* [Safety & Guarantees](#safety--guarantees)
* [Architecture](#architecture)
* [Build Instructions](#build-instructions)
* [Configuration](#configuration)

---

## Architecture

We use `NATS/JetStream` to solve the problem of distributing `PostgreSQL`'s logical replication and snapshot tables.
Both solve the hard problems: ordering, durability, idempotency.

Zebridge connects PG ↔ NATS and stays as stateless as possible.

NATS/JS holds the state, and a consumer - any connected client to NATS/JS - just needs to handle a state machine.
NATS have 40+ clients, so instead of an SDK, we propose a  [PROTOCOLE.md](#protocole.md). It details the workflows to be used to connect consumers to the NATS server to use the local-first synced database.

**Worked examples** can be found in [PROTOCOLE.md](#protocole.md) for Flutter (_TODO_), webapps (WASM-SQLite + OPFS), backend microservice in Node, Python & Elixir (all _TODO_).

```mermaid
flowchart TD
    subgraph VPS["VPS / Private Network"]
        PG[("PostgreSQL <br> Master")]
        subgraph Localhost ["VPS - Localhost"]
          Bridge["ZeBridge <br> daemon"]
          NATS[("NATS <br> JetStream")]
        end
        
        Bridge -- Prometheus --> Grafana["Grafana <br> dashboard"]
        NATS -- Prometheus --> Grafana

        PG -- "SSL (opt)" --> Bridge
        Bridge -- "SSL (opt)" --> PG
        Bridge <--> |"TCP (Plain)" <br> only| NATS
    end

    NATS <--> |"TLS / WSS"| Consumers


    subgraph Consumers["Consumers"]
        NativeApp["Native App"]
        WebApp["WebApp"]
        Worker["Microservice"]
        NativeApp <--> Local_DB[("Local<br> SQLite <br> PGLite <br> PostgreSQL")]
        WebApp <--> Local_DB
        Worker <--> Local_DB
    end
```

Current **v 0.1.14**: uses PostgreSQL 14+, with `NATS` 2.14+ and `Zebridge` on localhost only (communicate via **TCP only**).

**Roadmap**:

* [ ] **v 0.1.16**: Separate READ [CDC+bootstrap] on a **standby replica** (PG ≧ 16, shared WAL stream), from WRITE on **master** and enable async snapshotting on the replica.
* [ ] **v 1.0**: Remove the colocation constraint with `Zebridge` ← TLS → `NATS`.

## How ZeBridge compares

ZeBridge differs significantly from established tools:

**[PowerSync](https://github.com/powersync-ja)**

* **What it is:** PowerSync elegantly solves the "Postgres to local SQLite" problem for offline-first apps and features a robust bucketing system.
* **The ZeBridge Difference:** PowerSync requires a stateful backend sync engine. ZeBridge does not require a companion state.

**[Debezium](https://github.com/debezium/debezium)**

* **What it is:** The enterprise standard for Change Data Capture. It is incredibly feature-rich and reliable for server-to-server data movement.
* **The ZeBridge Difference:** Debezium requires a JVM and Apache Kafka infrastructure. It is designed for datacenter pipelines, not for streaming directly to millions of end-user WebSockets.

**[pgstream](https://github.com/xataio/pgstream)**

* **What it is:** A modern CDC tool by Xata written in Go. It supports DDL schema changes and streaming Postgres data to Kafka, OpenSearch, and Webhooks.
* **The ZeBridge Difference:** `pgstream` is designed for generic pipeline routing (database to database). It lacks the specialized edge-client state machine, local SQLite schema translation, and Snapshot-to-CDC bridging required to seamlessly sync mobile and web applications.

**[Bento](https://github.com/warpstreamlabs/bento)** (formerly Benthos)

* **What it is:** A high-performance stream processor that connects almost any source to any sink with on-the-fly transformations.
* **The ZeBridge Difference:** While you could theoretically wire up a CDC pipeline using Bento, you would have to invent the entire edge-sync protocol, snapshot logic, and schema transition management yourself.

## Understanding the app

ZeBridge is designed as a companion to the NATS/JS broker to enable Local-first sync of a Postgres database.

### The PG roles

The DBA is responsible onfigured to emit CDCs, to migrate the PUBLICATION for the desired tables, the roles and triggers needed by a Zebridge instance.

**Two Postgres roles required by ZeBridge**:

* `bridge_reader` for the read path (SELECT + REPLICATION, no writes possible),
* `bridge_writer` for the write path (_no table privileges_, or a per-table minimum),
* and authenticated principals above that for row-level access.

### The NATS streams and buckets

ZeBridge uses the NKEY protocole to join the NATS server.

Zebridge uses three streams and two KV buckets that enables the bidirectional communication flow ZeBridge ← NATS → consumer. The naming is **shared** and declared in [topology.json].

A ZeBridge instance is started with one config.
The DBA starts the NATS server  with one config, but he can adjust the NATS server's streams and buckets catalogue on the fly. He uses the same NKEY protocole as ZeBridge.

**Three data flow**:

1. **Bootstrap** (INIT stream): READ - Consumer requests _schemas_ & table _snapshot_ and Zebridge delivers: PG->Zebridge->NATS
2. **Real-time CDC** (CDC stream): READ - consumer receives INSERT/UPDATE/DELETE events as they happen: PG -> Zebridge -> NATS/JetStream.
3. **Real-time Ingress flow** (MUTATION stream): WRITE - Consumer updates his local storage and sends intentions messages to NATS -> Zebridge -> PG_master.

| Stream | Purpose | Retention | Consumer Pattern | Role |
| -------- | ------------------- | ------------- | ----------------------- | -- |
| **CDC** | Real-time egress changes | Short (X min) | Continuous subscription | READ |
| **INIT** | Bootstrap snapshots | Long (X days) | One-time replay | READ |
| **MUTATION** | Real-time ingres changes | Short (X min) | Continuous subscription | WRITE |

Streams have defined retention policies. The snapshot request is protected by maximum demand on 1 during `SNAP_RET`.
The consumer will uses these streams to interact with the  NATS state (the names are defined in _topolgy.json_).

### The memory setting

NATS comes with a `max_payload=1M` default.

**Snapshot**: Zebridge will suspend a table whose rows are wider than the NATS message limit (<1 MB). The NATS cap also means a consumer cannot push a large row to NATS. Above this limit, we are in the domain of Object storage for large blobs, and URLs should be saved in the database instead.

**CDC**: Zebridge is designed to be fast, with a **fixed memory** which caps the event size, suspends a table and drives the total memory used.  
> Total Memory = 2 x
> (`BASE_BUF` = size/row, 16kB default ) x
> (`RING_BUFFER_COUNT` = nb_slots, default 8184 )

Zebridge  suspends a table when events size are larger than $2^{\mathtt{BASE\_BUF}}$ which itself is capped at `BASE_BUF=20` (1MB). This matches `max_payload=1M` used by NATS.

The `RING_BUFFER_COUNT` is designed to buffer the received events during a NATS restart ~1s. Its count depends naturally upon the emitting rate.

**Examples**: if you expect to follow a table(s) with row size below 100 KB with say 500 evt/s, increase to `BASE_BUF=17` (136 KB per row) and buffer less `RING_BUFFER_COUNT=1000` (total mem footprint: ~ 262 MB).
If you expect a table(s) with smaller rows  < 1 KB/row which emits 50k evt/s, decrease `BASE_BUF=10` (1kB) and increase the ring size to `RING_BUFFER_COUNT=60000` (mem footprint: ~ 130 MB).

Ceiling is NATS/JS, the host capactiy, not ZeBridge.
❇️ Read [Sizing BASE_BUF and RING_BUFFER_COUNT](#sizing-base_buf-and-ring_buffer_count)

❇️ Read [Bridge ACK Flow and NATS outages](#bridge-ack-flow-and-nats-outages)

### How to start an instance

```txt
One bridge instance 
= one replication slot 
= sequential processing
```

Once:

* Postgres has played the needed migrations and has a PUBLICATION and confiured with WAL enabled,
* NATS configured with the streams and buckets,

the ZeBridge can be started with the following settings:

* a fixed memory buffer definition: `BASE_BUF` (default 2^14=16KB) and `RING_BUFFER_COUNT` (default 65565) adjusted depending upon the tables the instance is supposed to handle. Remember the memory footprint: $$2^{\mathtt{BASE\_BUF}} \times \small{\text{RING\_BUFFER\_COUNT}}$$
* a unique `--slot` (the unique pointer to the WAL that PostgreSQL keeps for this PUBLICATION). If you have several instances running, each has its own `SLOT`.
* a unique `--port` (for the instance, for the telemetry, served by a webserver). If you have several instances running, each has its own `PORT`,
* the `--top` config, defaults to _topology.json_ (_./topology.json_), shared naming of streams and bucket between ZeBridge, NATS and consumers,
* the mandatory private seed `NATS_NKEY_SEED` env var corresponding to the public NKEY passed to the NATS server,
* the different paths DATABASE_URL, DATABASE_WRITER_URL, NATS_URL to connect to PostgreSQL and to NATS.

For example, one instance using the declared PUBLICATION 'my_pub' - created by the DBA -  with (unique) slot named 'my_slot' is run with the command:

```sh
# has defaults
BASE_BUF=10 \                     # <- default 14
RING_BUFFER_COUNT=4092 \          #<-- default 16K
NATS_URL=nats://127.0.0.1:4222 \  #<-- default value
PORT=9090 \                       #<-- default port
# ⚠️ mandatory -->
NATS_NKEY_SEED=SU... \            
DATABASE_URL=postgres://bridge_reader:bridge_password_changeme@127.0.0.1:55432/postgres \
DATABASE_WRITER_URL=postgres://bridge_writer:writer_password_changeme@127.0.0.1:55432/postgres \
./bridge --slot my_slot --pub my_pub --top topology.json
```

❌ Refuse startup on checks:

> TODO: #1 system_memory > 2^(BASE-1) x RING

> TODO: #2 BASE_BUF < MAX_BASE_BUF==max_payload (nats.conf)

**Principal authentication** (the user of a Consumer app):

The principal is authenticated at the Consumer level, and used in the JWT/Operator protocole by NATS so that the bridge can pass it for Postgres RLS policies.

|  |  subscribe  |   publish  | needs an account |
|--|--|--|--|
| read-only consumer  | cdc.>, init.>, KV.schemas.>, KV.snapshots.> | snapshot.request.>       | no |
| read-write consumer | the same | + mutation.<principal>.> | yes |

## Design overview

**Key features of the bridge**:

* It streams PostgreSQL _proto-v1_ changes using logical replication (`pgoutput` binary format)
* Publishes schemas from the catalogue to NATS KV store on startup,
* Generates table snapshots on-demand, chunked by **bytes** to fit one NATS message (10 000 rows is only a ceiling), via NATS requests,
* Triggers message to NATS on schema change via Postgres DDL event triggers,
* schemas are available in two formats: PostgreSQL(eg for PQLite) and SQLite,
* MessagePack default encoding or JSON available with `--json`,
* At-least-once delivery with idempotent message IDs,
* Graceful shutdown with LSN acknowledgment,
* telemetry via HTTP `/metrics` (Prometheus format) and structured logs on **stderr** (Grafana Loki)

**Key decisions**:

* `REPLICA IDENTITY DEFAULT | FULL`. Preflight warning. Use only tables with _single column primary key ID_ (no single column PK + DEFAULT => PG error on UPDATE). Solution: run a migration to set a unique column PK ID => `DELETE id='42'`,
* No SDK but a _HOW TO SYNC_ workflow and _NAMING CONVENTIONS_ to follow client side: demanding schema, running migration, seeding from the snapshot, playing the CDCs. Snippets are proposed in JS, Elixir, Python, NodeJS, Go.

* Single-Threaded per Bridge. The rationale is PostgreSQL WAL is inherently sequential. Simpler LSN acknowledgment logic. To a slot can be attached one or several tables.
* ➡️ Scale horizontally (multiple bridges) instead of vertically. The PostgreSQL admin creates the publication and scope (_pub_name_ for which tables). Run multiple bridge instances with different replication slots - limited to `max_replication_slots=XX` in master config - and reach PostgreSQL instances.
You can divide the workload per PostgreSQL instance, and attach bridge instances to differents scopes (specific tables). Each bridge instance can scale its builtin buffer taylored to the needs of the table(s) with `BASE_BUF` and `RING_BUFFER_COUNT`.

```bash
# Bridge 1: table "users" with publication "my_pub_1" on master
DATABASE_URL=postgres://bridge_reader:pw@localhost:5432/postgres \
  ./bridge --slot slot_1 --pub my_pub_1 --port 9090

# Bridge 2: table "orders" with publication "my_pub_2" on replica_1
DATABASE_URL=postgres://bridge_reader:pw@replica_1:5432/postgres \
  ./bridge --slot slot_2 --pub my_pub_2 --port 9091
```

* Encoding. The default is **MessagePack**. Compact (~30% smaller than JSON). Type-safe (_preserves_ int/float/binary distinctions) whilst JSON sends strings. Fast encoding/decoding and available on many plateforms and languages.

* Use flag `--json` to receive a JSON format. Slightly larger payload size and slower encoding/decoding.

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

The Consumer controls replay (NAK → redeliver).
Durable consumer name survives restarts.
Multiple consumers can track independent positions

**Backpressure:**: Consumer slow → JetStream buffers → Consumer catches up at own pace.
JetStream retention policies prevent unbounded growth
.

### Snapshots

1. Bridge starts → publishes schemas to KV store immediately
2. NATS pushes schemas when Consumer connects
3. ...
4. Consumer subscribes to CDC stream for updates

Non-blocking (bridge continues CDC while snapshotting)

---

## Local setup

### PostgreSQL Setup (by admin/DBA Task eg `postgres` user)

#### 1. Enable Logical Replication

Add to `postgresql.conf` or Docker command:

```sh
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
max_slot_wal_keep_size = 10GB
wal_sender_timeout = 300s  # 5 minutes
```

#### 2. Create Publication

The BDA create a publication say `my_pub` on **specific tables** or **all**.

```sql
CREATE PUBLICATION my_pub FOR TABLE users, orders;
```

Or for all tables:

```sql
-- Run as superuser
CREATE PUBLICATION my_pub FOR ALL TABLES;
```

#### 3. Create Bridge User

> [!NOTE] The bridge uses a restricted "bridge_reader" user for security.

It should be restricted to SELECT on given tables + REPLICATION (least privilege).

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

See `init.sh` for a complete setup script.

---

### NATS JetStream Setup (Admin Task)

The NATS admin must enable JetStream, create two streams 'CDC' and 'INIT' and a KV store 'schemas'.

#### 1. Enable JetStream

```sh
nats-server -js -m 8222
```

Or via Docker:

```bash
docker run -p 4222:4222 -p 8222:8222 nats:latest -js -m 8222
```

#### 2. Create Streams

```sh
# CDC stream: Short retention for real-time events
nats stream add CDC \
  --subjects='cdc.>' \
  --storage=file \
  --retention=limits \
  --max-age=1m \
  --max-msgs=1000000 \
  --max-bytes=1G \
  --replicas=1

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

#### 3. Create KV Bucket for Schemas

```sh
nats kv add schemas --history=10 --replicas=1
```

#### 4. Configure Authentication NATS <-> Zebridge

> The authentication between NATS and Zebridge uses NKEY. You must generate one.

**Generate nkey**: use `nk -gen user -pubout`

```txt
# seed: private key
UACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY

# user public key:
NATS_BRIDGE_NKEY_PUB=UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4
```

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

See `docker-compose.yml` for a complete Docker setup with PostgreSQL + NATS.

#### 5. configure Authentication NATS ↔ Consumer

Prefer the JWT mechanism. Transport over TLS/WSS.
The flow: [TODO]

---

## Consumer Integration Guide

### Naming Conventions (The API Contract)

Because ZeBridge avoids a thick, opinionated SDK, these naming conventions _are_ the API contract. Clients must use these exact names and subjects to correctly synchronize with the backend.

Source: `topology.json`

#### 1. Schemas (NATS KV Store)

* **Bucket Name:** `schemas`

* **Key Convention:** `<table_name>` (e.g., `users`)
* **Client Action:** `kv.get('users')` to fetch the MessagePack/JSON schema definition.

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

`PROTOCOL.md` is the contract; this is the shape of it. Four things, in order.

### 1. Schemas arrive by subscription, not by asking

Watch the `schemas` KV bucket. The bridge writes every monitored table's schema at startup
and again on each DDL event, so the bucket is already current when you connect — a fresh
watch replays it. Create or migrate your local tables from what arrives; you never poll.

### 2. Decide whether you need a snapshot — using `seq`, not `lsn`

Persist **two** positions, because they are in different coordinate systems:

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

### 3. If you need one: request, replay, then dedup by `lsn`

`PUBLISH snapshot.request.<table>` with an empty payload — the table is in the subject.
Then read the descriptor from the `snapshots` KV bucket and pull
`init.snap.<table>.<snapshot_id>.>`. Two things to know:

* **One request per table per window.** The broker enforces it: a second request inside
  `SNAP_RET` is rejected with `503 … maximum messages per subject exceeded`. That is not an
  error to retry through — check the KV descriptor first and seed from the existing snapshot.
* **Snapshots are served one at a time**, so a request for a small table can wait behind a
  large one.

After replaying a snapshot taken at LSN X, set the table's `lsn = X` and **discard every CDC
event with `event.lsn <= X`** — those rows are already in what you just seeded.

### 4. Follow CDC, and gate on columns

Resume the CDC consumer from your stored `seq`. Apply rows whose columns you know; **hold**
a row that carries a column your local table lacks until a newer schema arrives (§5 of
`PROTOCOL.md`). Gate on the _column set_, never on the LSN — most rows legitimately carry an
LSN newer than the last DDL, so an LSN gate stalls forever during quiet periods.

### Writing back (ingress)

Publish to `mutation.<principal>.<table>.<operation>`. Three things make this different from
a normal API call:

* **The principal is a subject token, not a payload field.** NATS authorises subjects, so a
  client granted `publish: ["mutation.alice.>"]` physically cannot write as anyone else. A
  `user_id` in the body would just be a claim.
* **Last write wins on a version column** — usually `updated_at`. Send the value you hold;
  the bridge applies the row only if yours is newer. A stale write is _rejected, silently by
  design_ — which is why §7.2's type table matters: a `timestamp` without time zone, or one
  with second precision, makes "newer" ambiguous and drops real edits.
* **Your write comes back as CDC**, like anyone else's. That echo is the confirmation.

⚠️ A mutation larger than the server's `max_payload` is rejected by NATS client-side, so the
write path is capped by construction — but the bridge's own per-event buffer (`BASE_BUF`,
16 KB by default) is _smaller_ than that ceiling. A 500 KB row can be accepted by Postgres
and then suspend the table when the CDC echo does not fit. Match `BASE_BUF` to what you
intend to write.

### The flow, end to end

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

## Running the Bridge

The user of the bridge defines a `REPLICATION_SLOT` with the flag `--slot my_slot`. The bridge will create it.

### Command-Line Options

```txt
  --slot <NAME>     Mandatory: Replication slot name (default: cdc_slot)
  --pub <NAME>      Mandatory: use the Postgres `PUBLICATION`
  --port <PORT>     HTTP telemetry port (default: 9090)
  --json            Encoder option: defaults to `MessagePack`
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

The variables above are the bridge's. A second family — `PG_HOST`, `PG_PORT`, `PG_USER`,
`PG_PASSWORD`, `PG_DB`, `POSTGRES_BRIDGE_*`, `POSTGRES_WRITER_*` — is the **admin** set:
the superuser that `bridge-init` uses to run `init.sql` and the passwords it assigns to
the roles it creates. The bridge does not read any of them.

They used to be a fallback for the bridge's own connection, which made a missing or
misspelled `DATABASE_URL` connect as `postgres` and look healthy doing it. That defeats
the whole point of the split: `bridge_reader` holds SELECT + REPLICATION and is
_physically_ unable to write, `bridge_writer` has no table privileges until a DBA opens
one — and neither guarantee survives a process that can connect as someone else.

So they live in `.env.admin` — together with `PG_PUBLISH_PORT`, `SNAP_RET_SECONDS` and
the nkey's public half, all of which are read by compose or by the init containers and
never by the bridge — while `.env.bridge` holds only what the bridge itself reads. The
bridge refuses to start without `DATABASE_URL`:

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

## Message Formats

Payloads use `MessagePack` binary encoding by default.

JSON can be enabled with the flag `--json`.

The examples below show the logical structure for illustration.

### Schema (KV Store: `schemas.{table}`)

Published at bridge startup. Consumers fetch before requesting snapshots.

```json
{
  "table": "users",
  "schema": "public.users",
  "timestamp": 1765201228,
  "columns": [
    {
      "name": "id",
      "position": 1,
      "data_type": "integer",
      "is_nullable": false,
      "column_default": "nextval('users_id_seq'::regclass)"
    },
    {
      "name": "name",
      "position": 2,
      "data_type": "text",
      "is_nullable": false,
      "column_default": null
    },
    {
      "name": "email",
      "position": 3,
      "data_type": "text",
      "is_nullable": true,
      "column_default": null
    }
  ]
}
```

### Snapshot Metadata (INIT Stream: `init.meta.{table}`)

Published after all chunks. Tells consumer how many chunks to expect.

```json
{
  "snapshot_id": "snap-1765208480",
  "lsn": "0/191BFD0",
  "timestamp": 1765208480,
  "batch_count": 4,
  "row_count": 4000,
  "table": "users"
}
```

### Snapshot Chunk (INIT Stream: `init.snap.{table}.{snapshot_id}.{chunk}`)

Chunked by **bytes**, not rows: the bridge asks Postgres for the longest prefix of rows
whose cumulative size fits one NATS message (`max_payload` minus envelope). `chunk_size`
in `config.zig` is only a row _ceiling_ — a narrow table hits it, a table of 256 KiB rows
gets three rows a chunk. A row too large to publish at all suspends the table rather than
being split.

```json
{
  "table": "users",
  "operation": "snapshot",
  "snapshot_id": "snap-1765208480",
  "chunk": 3,
  "lsn": "0/191BFD0",
  "data": [
    {
      "id": "3001",
      "name": "User-3001",
      "email": "user3001@example.com",
      "created_at": "2025-12-08 13:45:21.719719+00"
    },
    {
      "id": "3002",
      "name": "User-3002",
      "email": "user3002@example.com",
      "created_at": "2025-12-08 13:45:22.123456+00"
    }
    // ... as many rows as fit one message
  ]
}
```

### CDC Event (CDC Stream: `cdc.{table}.{operation}.{batch}`)

Real-time INSERT/UPDATE/DELETE events.

**Subject pattern:** `cdc.{table}.{operation}` with the test table 'test_types':

* `cdc.test_types.insert`
* `cdc.test_types.update`
* `cdc.test_types.delete`

**Message ID (for deduplication):** `{lsn}-{table}-{operation}`

Example: `"1851208-test_types-insert"`

**INSERT event:**

```json
{
  "data" => {
    "age" => 30,
    "created_at" => "2025-12-12T12:00:34.338547Z",
    "id" => 12,
    "is_true" => true,
    "matrix" => "{{1,2},{3,4}}",
    "metadata" => {
      "key_1" => "value_1",
      "key_2" => [[1, 2], [3, 4], [5, 6]],
      "key_3" => {"key_4" => "value_4", "key_5" => "value_5"}
    },
    "price" => "123.4500",
    "some_text" => "Sample text",
    "tags" => "{\"tag1\",\"tag2\"}",
    "temperature" => 36.6,
    "uid" => "f4b0611f-7258-47f8-bceb-0eba9ac5195a"
  },
  "msg_id" => "1851208-test_types-insert",
  "operation" => "INSERT",
  "relation_id" => 16392,
  "subject" => "cdc.test_types.insert",
  "table" => "test_types"
}
```

**UPDATE event:**

```json
{
  "data" => {
    "age" => 31,
    "created_at" => "2025-12-12T12:00:34.338547Z",
    "id" => 12,
    "is_true" => false,
    "matrix" => "{{1,2},{3,4}}",
    "metadata" => {
      "key_1" => "value_1",
      "key_2" => [[1, 2], [3, 4], [5, 6]],
      "key_3" => {"key_4" => "value_4", "key_5" => "value_5"}
    },
    "price" => "122.9905",
    "some_text" => "Sample text",
    "tags" => "{\"tag1\",\"tag2\"}",
    "temperature" => 37,
    "uid" => "f4b0611f-7258-47f8-bceb-0eba9ac5195a"
  },
  "msg_id" => "18513a8-test_types-update",
  "operation" => "UPDATE",
  "relation_id" => 16392,
  "subject" => "cdc.test_types.update",
  "table" => "test_types"
}
```

**DELETE event:**

```json
{
  "data" => {
    "age" => nil,
    "created_at" => nil,
    "id" => 12,
    "is_true" => nil,
    "matrix" => nil,
    "metadata" => nil,
    "price" => nil,
    "some_text" => nil,
    "tags" => nil,
    "temperature" => nil,
    "uid" => nil
  },
  "msg_id" => "1851518-test_types-delete",
  "operation" => "DELETE",
  "relation_id" => 16392,
  "subject" => "cdc.test_types.delete",
  "table" => "test_types"
}
```

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

```conf
# /etc/logrotate.d/zebridge
/path/to/bridge.log {
    daily
    rotate 14
    compress
    missingok
    copytruncate      # the bridge holds the fd open; it has no reopen-on-SIGHUP
}
```

Under systemd, skip the redirect entirely: journald captures stderr, and
`journalctl -u zebridge -p warning` gives the severity filter for free.

### 1. Prometheus Metrics Endpoint

**HTTP GET** `http://localhost:9090/metrics`

Returns metrics in Prometheus text format:

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

Each is served with its own `# HELP` and `# TYPE` line. The four worth alerting on:

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

**HTTP GET** `http://localhost:9090/status`

Returns bridge status as JSON:

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

The same numbers as `/metrics`, for a human or a shell script rather than a scraper.

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

⚠️ It is emitted from the WAL loop, so it starts once replication is running — not
during startup.

When you are ready to ship these to Loki, point Grafana Alloy at the file. Every line
is `level(scope): message`, and both halves make useful labels:

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

Initiates graceful shutdown:

```bash
curl -X POST http://localhost:9090/shutdown
```

Shutdown sequence:

1. Sets global shutdown flag
2. Drains internal event queue
3. Sends final ACK to PostgreSQL
4. Closes connections cleanly
5. Exits with summary statistics

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

**The guarantee:**

* Bridge only ACKs to PostgreSQL **after** NATS JetStream confirms receipt
* PostgreSQL can safely prune WAL after ACK
* No data loss between Postgres and NATS

**If bridge crashes:**

* PostgreSQL retains WAL from last ACK'd LSN
* Bridge restarts from last ACK'd position
* JetStream deduplication (Msg-ID) prevents duplicates

**If NATS crashes:**

* Bridge stops ACK'ing to PostgreSQL
* WAL accumulates (up to `max_slot_wal_keep_size=10GB`)
* NATS recovers → Bridge resumes publishing

### Zero-Consumer Protection & Storage Bounds

**What happens if PostgreSQL emits CDC events when NO NATS clients are connected?**

ZeBridge uses a **NATS JetStream Limits Retention** policy (`--max-age=1m`, `--max-bytes=1G`) for the `CDC` stream to protect both storage systems:

1. **PostgreSQL Disk Protection**: ZeBridge continues receiving WAL events from PostgreSQL and publishing them to NATS JetStream. Once JetStream confirms file persistence, ZeBridge ACKs the LSN back to PostgreSQL. PostgreSQL safely prunes its disk WAL, preventing PostgreSQL disk bloat.
2. **NATS JetStream Storage Bounds**: Even if zero clients are connected, JetStream automatically purges `CDC` stream events older than 1 minute (or exceeding 1GB).

> [!NOTE]
> This strategy ensures that neither PostgreSQL nor NATS JetStream run out of disk space when consumers are offline. Clients that connect after an outage fetch a fresh table snapshot (`INIT` stream) and resume real-time updates from the `CDC` stream.

* No data loss (WAL preserved)

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

**Snapshot consistency:**

* Each snapshot includes LSN for consistency point
* Consumer can reconstruct table state at that LSN

**CDC event ordering:**

* PostgreSQL WAL is sequential
* Bridge preserves order (single-threaded SPSC queue)
* NATS JetStream delivers in order

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

## Architecture

### Thread Model (8 threads)

Every one is spawned once at startup and lives until shutdown. Nothing is spawned per
request, per table or per connection.

| # | thread | spawned at | does |
| --- | --- | --- | --- |
| 1 | **Main** | — | consumes the WAL stream, parses pgoutput, packs rows into the ring buffer. `EventProcessor` runs _inline_ here, so "the CDC path" and "the main thread" are the same thread |
| 2 | **Batch publisher** | `batch_publisher.zig:534` | drains the ring buffer, encodes MessagePack, publishes to the `CDC` stream |
| 3 | **WAL monitor** | `wal_monitor.zig:50` | replication-slot lag, every 30 s |
| 4 | **HTTP telemetry** | `http_server.zig:49` | `/metrics`, `/health`, `/status`, `/shutdown`. Accepts and serves inline — no thread per connection |
| 5 | **Snapshot supervisor** | `snapshot_listener.zig:710` | spawns 6 and 7, then sleeps until shutdown and joins them. Does no work of its own |
| 6 | **Schema requests** | `snapshot_listener.zig:731` | answers `init.schema` on its own connection, so a schema request never queues behind a multi-minute `COPY` |
| 7 | **Snapshot requests** | `snapshot_listener.zig:745` | consumes `snapshot.request.>` and generates snapshots **inline** — one at a time, in arrival order |
| 8 | **Mutation listener** | `mutation_listener.zig:137` | consumes `mutation.>`, applies writes under the ingress role. Only started when `DATABASE_WRITER_URL` is set |

Two things this table is meant to stop people assuming:

* **Snapshots are not concurrent.** Thread 7 calls `generateIncrementalSnapshot` inline, so
  a large table blocks every other table's request behind it in the stream. Requests are
  durable so nothing is lost, and the ack happens on receipt rather than on completion —
  but a small table waits for a big one. (`Config.Snapshot.max_concurrent_snapshots`
  states an intent that was never built; it is read by nothing.)
* **Preflight is not a thread.** It is an inline call in `main` (`bridge.zig:377`) that runs
  _before_ threads 5–8 exist, which is why it can write the refusal registry without
  synchronising with anyone.

#### Who touches the refusal registry

`refused_tables.Registry` is shared by the CDC, snapshot and schema paths — one table must
not be refused by one and served by another. Three roles reach it:

| | role |
| --- | --- |
| main thread (1) | writes (DDL path) **and** reads (`shouldDrop` per event) |
| snapshot requests (7) | writes (a row too wide to publish) **and** reads (rejects requests) |
| HTTP telemetry (4) | reads only, for `/metrics` and `/status` |

Two concurrent _writers_ and a concurrent _reader_ is why the registry is an append-only
array with atomic publication rather than a hash map with a lock — see the header of
`src/refused_tables.zig`.

### Practical Performance of SPSC

**Producer-Consumer pattern:**

* **Producer**: Main thread (reading WAL)
* **Consumer**: Batch publisher thread
* **Queue**: 65536 slots (2^16), ~4MB memory

**Dual purpose:**

1. **Thread separation** (primary): Decouple WAL reading from NATS publishing
2. **Resilience buffer** (critical): Absorb WAL events during NATS reconnection

**How it handles NATS outages:**

```txt
NATS goes down at T=0
├─ Main thread continues reading WAL → pushes to queue
├─ Flush thread can't publish → queue fills up
├─ Queue fills (65536 slots) → ~1s buffer at 60K events/s (see Measured throughput)
├─ Queue full → Main thread backs off (sleeps 1ms per attempt)
├─ PostgreSQL WAL starts accumulating (controlled)
│
NATS reconnects at T=1000ms+ (reconnect_wait covered by queue buffer)
├─ Flush thread resumes publishing
├─ Queue drains rapidly (~1s of buffered events)
└─ Bridge catches up, resumes ACK'ing PostgreSQL
```

**Backpressure cascade:**

```txt
NATS outage → Queue fills → Main thread slows → PostgreSQL WAL accumulates
                                                         ↓
                                           (up to max_slot_wal_keep_size=10GB)
```

**Graceful degradation:**

* Queue absorbs microsecond-scale jitter (lock-free, wait-free)
* PostgreSQL WAL absorbs second-scale outages (up to 1s queue buffer)
* `max_slot_wal_keep_size=10GB` absorbs minute-scale outages
* Beyond that → alerts fire (intentional)

### Data Flow

```txt
PostgreSQL WAL
    ↓
Main Thread (parse pgoutput)
    ↓
SPSC Queue (lock-free)
    ↓
Batch Publisher Thread
    ↓ (batch: 500 events OR 100ms OR 256KB)
MessagePack Encoding
    ↓
NATS JetStream (async publish)
    ↓ (JetStream ACK)
PostgreSQL LSN ACK
```

### Memory Management

**Arena allocator:**

* Reused for each WAL message
* Retains capacity across messages
* Avoids allocator churn at high throughput

**Ownership transfer:**

* Decoded column values transferred via SPSC queue
* Batch publisher thread frees after publishing
* No shared state between threads

### Replication Slot Management

**On startup:**

1. Bridge creates replication slot (if not exists)
2. Gets current LSN to skip historical data
3. Starts streaming from current LSN

**During operation:**

* Bridge sends status updates every 1 second OR 1MB data
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

* Automatic (handled by the vendored Zig client in `src/nats_vendor/`)
* Max attempts: -1 (infinite)
* Wait between attempts: 2 seconds
* Flush timeout: 10 seconds

## Build Instructions

### Prerequisites

* Zig 0.16.0 or later
* Docker & Docker Compose (for PostgreSQL and NATS)
* `libpq` — the only C dependency, linked from the system
  (`brew install libpq` on macOS, `apk add postgresql-dev` / `apt install libpq-dev`
  on Linux). Override its location with `zig build -Dlibpq-prefix=/path`.

There is no vendored-library build step: the NATS client is pure Zig and lives in
`src/nats_vendor/`.

### 1. Start Infrastructure

```bash
# Start PostgreSQL with logical replication enabled
docker compose up -d postgres

# Start NATS server with JetStream enabled
docker compose up -d nats-server nats-config-gen nats-init
```

### 2. Build the Bridge

```bash
zig build
```

Output: `./zig-out/bin/bridge`

### 3. Run Tests

```bash
zig build test
```

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

* Batch size: `500` events OR `100ms` OR `256KB` (whichever first)
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

**Buffer sizes:**

* SPSC queue: `65536` slots (2^16, ~1092ms buffer at 60K events/s)
* Subject buffer: `128` bytes
* Message ID buffer: `128` bytes

See `src/config.zig` for all tunables.

### Sizing `BASE_BUF` and `RING_BUFFER_COUNT`

⚠️ read this one

These two are not independent, and getting them wrong has a visible consequence rather than a silent one. The bridge pre-allocates one fixed-size buffer per event slot:

```txt
data slab = 2^BASE_BUF  ×  RING_BUFFER_COUNT
             ^ max bytes    ^ number of events
               for ONE row    buffered ahead of NATS

  12 / 65536 = 256 MB   ← defaults: 4 KB rows, ~1s of outage tolerance at 60K evt/s
  14 / 65536 =   1 GB   ← 16 KB rows
  20 /  1024 =   1 GB   ← 1 MB rows, minimum ring (least outage tolerance)
```

Each one answers a different question:

* **`BASE_BUF`** (log2 bytes, range 10–20) is _how large a single row may be_. Size it
  to your widest row: a `jsonb` document, a long `text` column, a big array.
* **`RING_BUFFER_COUNT`** (range 1024–1048576) is _how many events can queue while NATS is unreachable_. 65536 slots ≈ 1 second at 60K events/s. Below that, a NATS blip starts back-pressuring the WAL reader sooner.

Raising one and lowering the other keeps memory flat, at the cost of outage tolerance.

#### Two things checked at startup, before a byte is allocated

Both failures are silent and late if left to runtime, so the bridge refuses to start:

| check | refuses when | why not a warning |
| --- | --- | --- |
| **slab vs memory** | `2^BASE_BUF × RING_BUFFER_COUNT` exceeds half the process's memory limit | the slab is pre-allocated, so the alternative is an OOM kill under load. In a container the **cgroup limit** is read, not the host's RAM — otherwise a 1 GB slab looks fine on a 64 GB host until the 512 MB cgroup kills it |
| **`BASE_BUF` vs `max_payload`** | `2^BASE_BUF + envelope` exceeds what the NATS server advertises | a row that size packs successfully and is then **rejected at publish time**, with nothing in the data path saying why. That is not a tuning choice, it is a configuration that cannot work |

```txt
🔴 The event slab would be 65536 MB (BASE_BUF=20 → 1024 KB × RING_BUFFER_COUNT=65536),
   against a 16384 MB memory limit. … Halve RING_BUFFER_COUNT for each step you raise
   BASE_BUF.

🔴 BASE_BUF=20 allows a 1024 KB row, but this NATS server accepts at most 1024 KB per
   message and the envelope needs roughly 16 KB more. … Lower BASE_BUF to 19 or raise
   max_payload in nats-server.conf.
```

On a healthy start you get the same arithmetic as a fact rather than a complaint:

```txt
NATS max_payload: 1024 KB (server-advertised); per-event buffer: 16 KB (BASE_BUF=14)
Event slab: 1024 MB of a 16384 MB limit (6%)
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

`BASE_BUF=20` is 1 MB, which is also **nats-server's default `max_payload`**. A message also carries a subject, headers and MessagePack framing, so a row sized right up to the limit is still rejected at publish time. The bridge reads the server's advertised `max_payload` from its INFO line at connect and tells you where you stand:

```txt
info(bridge): NATS max_payload: 1024 KB (server-advertised); per-event buffer: 16 KB (BASE_BUF=14)
```

and warns if the two cannot coexist.
Raising `max_payload` in `nats-server.conf` is possible but affects every client and every subject on that server. JetStream's memory use scales with it — so for genuinely large values, prefer keeping the blob out of the replicated table and replicating a reference to it.

---

## Dependencies

**Managed via `build.zig.zon`:**

* [zig-msgpack](https://github.com/zigcc/zig-msgpack) - MessagePack encoding

**Vendored (in `src/`):**

* [g41797/nats](https://github.com/g41797/nats) — pure-Zig NATS client, patched for
  Zig 0.16 and extended with nkey authentication. `src/nats_vendor/` is the only
  0.16 port of it, so its own test suite runs here: `zig build test-nats`.
* [g41797/mailbox](https://github.com/g41797/mailbox) — `src/mailbox_vendor/`

**System:**

* [libpq](https://www.postgresql.org/docs/current/libpq.html) — dynamically linked, so the distro's own CVE fixes apply

---

## Testing

### An example of a measured throughput

**Method**:

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

**Result**:

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

**This benchmark is producer-bound**: PostgreSQL needs 6 s to write what the bridge drains in 7.6 s of loop time, and the loop is idle ~72% of the first interval. The
ceiling is higher than 260k; finding it needs a producer that is not the bottleneck.

**What it does not measure**: wide rows, `jsonb`/array-heavy tables, `REPLICA IDENTITY
FULL` (which doubles tuple volume), a remote PostgreSQL or NATS, consumers reading concurrently, or a NATS server under back-pressure. Each of those moves the number.

**Reading the `LOOP` line.** It is emitted next to `METRICS` every 15 s and is what
makes this diagnosable at all:

| field | meaning |
| --- | --- |
| `iters` | WAL loop iterations in the interval |
| `idle` | iterations that found nothing and slept 1 ms — high `idle` means the bridge is waiting for PostgreSQL, not struggling |
| `recv_ms` | milliseconds inside `receiveMessage` (libpq + framing) |
| `proc_ms` | milliseconds decoding tuples and packing them into the ring buffer |
| `cpu` | process CPU over the interval, all threads — `100%` is one core saturated |

`recv_ms` approaching the interval length with `idle=0` is the signature of a reader
that cannot drain its socket — the exact shape of the bug fixed in `wal_stream.zig`.

### End-to-End CDC Pipeline Test

**Terminal 1 - Start the bridge**:

Start Postgres and NATS as containers:

```sh
docker compose -f docker-compose.prod.yml --env-file .env.prod up
```

Start the bridge:

```sh
./zig-out/bin/bridge --slot my_slot --pub cdc_slot
```

**Terminal 2 - Use th Elixir Admin app**:

To migrate (on startup):

```sh
cd consumer && iex -S mix
```

and generate events:

```sh
# Generate 100 INSERT events
iex> Producer.bulk(10)

# Parallel load test: 100 batches of 10 events each every 5ms
iex> Producer.stream(5, 100, 10, "teset_types")
```

**Terminal 3 - Use the Browser client app**:

Start consumer (auto-connects to NATS server)

```sh
cd web-consumer && pnpm dev
```

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

## Roadmap

**Possible enhancements:**:

* [ ] Metrics export to StatsD/InfluxDB
* [ ] Integrate MySQL connector option?

**Open questions:**:

* The burst benchmark is producer-bound: PostgreSQL needs ~6 s to write the 2M rows the
  bridge then drains in ~7.6 s of loop time. Where is the ceiling when the producer is
  not the limit — a pre-filled WAL, or several emitters?

**Contributions welcome!** If you find it useful (or find gaps), feedback is valuable.

---

## License

This project uses:

* [zig-msgpack](https://github.com/zigcc/zig-msgpack) (MIT)

---

## Notes

* The vendored NATS client has **no TLS**: `protocol.zig` sends
  `"tls_required":false`, so bridge ↔ nats-server must stay on a private network.
  External clients reach nats-server over wss, which nats-server terminates itself.
* Authentication to NATS uses **nkey**, signed with `std.crypto.sign.Ed25519` — pure
  Zig, no OpenSSL linked.
* PostgreSQL logical replication requires `wal_level=logical`

**Example how to generate nkey for NATS**:

```sh
brew install nats-io/nats-tools/nats
nk -gen user -pubout

# private: SUAAF6OSYEICIIMANGOM5WCIRDEILIMKLLQWOXPKB4DOVEDZN22CPMWVVI
# public: UDIUGKDO52EGKF5VUPIZBJEMY2HY7W6PM4TP2D4FUMQ2XX74BZXMAHCV
```

In _nats-server.conf_, set:

```diff
authorization {
  users: [
-    # { user: "${NATS_BRIDGE_USER}", password: "${NATS_BRIDGE_PASSWORD}" } <-- remove
+    { nkey: "UDIUGKDO52EGKF5VUPIZBJEMY2HY7W6PM4TP2D4FUMQ2XX74BZXMAHCV"}
  ]
}
```

Start the bridge daemon with the private nkey:

```sh
NATS_NKEY_SEED=SUAAF6OSYEICIIMANGOM5WCIRDEILIMKLLQWOXPKB4DOVEDZN22CPMWVVI \
RING_BUFFER_COUNT=2048 \
BASE_BUF=12 \
DATABASE_URL=postgres://bridge_reader:bridge_password_changeme@localhost:55432/postgres \
NATS_URL=nats://localhost:4222 \
./zig-out/bin/bridge --slot my_slot --pub my_pub --port 9090
```
