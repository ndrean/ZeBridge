# ZeBridge: replicate and sync PostgreSQL

<p align="center"><img  width="355" height="233" alt="Screenshot 2025-12-26 at 02 37 57" src="https://github.com/user-attachments/assets/b3701ef4-2d58-497a-be21-52ad1b970644" /></p>

![Zig support](https://img.shields.io/badge/Zig-0.16.0-color?logo=zig&color=%23f3ab20)

**What is it?**:  An opinionated, bidirectional bridge to synchronize a single PostgreSQL (14+) database with a local replica via NATS/JetStream (2.10+).
It is **one bridge with two components** you build on:

```txt
PG  ←→  zebridge (daemon)  ←→  NATS  ←→  libzb(.js) (library)  ←→  consumer
```

The daemon `zebridge` streams PostgreSQL changes onto NATS/JetStream and applies writes coming back.
The client library `libzb` keeps a local SQLite replica on the edge — browsers, phones, services — and is the only way the consumer reads and writes.
Consumers query their replica offline  - dashboard, search index, analytics...and optimistic writes are pushed using `libzb` through three verbs (INS, DEL, UP), resolved **last-write-wins**.

NATS/JS is the transport, the fan-out/in, and the tenant boundary.

**Design**: This tool is built to serve a large number of small to medium consumers via the message broker NATS/JS, so it is designed to be fast and safe.
It is NOT for large and long tables or tables containing large objects: it is not a file transfer tool. A large blob (> 1 MB) belongs in object storage. Tables should only contain the reference to a blob.

**Performance**: you can expect up to 250-300.000 evt/s on your local computer. It essentially depends entirely on Postgres `pgoutput` rate. ZeBridge and NATS just absorb whatever's given. Once deployed, you have the unavoidable network latency.
See [An example of a measured throughput](#an-example-of-a-measured-throughput) for a real, reproducible number — run it yourself on your own hardware before trusting any figure quoted here.

**Status**: Dev stage.

See the Table of Contents below for configuration, scaling, evaluation, and telemetry.

## Table of Contents

* [Overview](#overview)
* [How ZeBridge compares](#how-zebridge-compares)
* [Opinionated](#opinionated)
* [Server setup side](#server-setup-side)
  * [Roles and privileges](#roles-and-privileges)
  * [NATS streams and buckets](#nats-streams-and-buckets)
  * [The memory setting](#the-memory-setting)
* [Design overview](#design-overview)
  * [Bridge ACK Flow and NATS outages](#bridge-ack-flow-and-nats-outages)
* [Quick review of PG & NATS setup](#quick-review-of-pg--nats-setup)
* [The consumer side — use the library](#the-consumer-side--use-the-library)
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

 The `ZeBridge` daemon connects the Postgres database and a NATS/JS server.
A consumer uses the library `libzb` to connect to the NATS/JS  server  - or leaf node - and to synchronize Postgres with its local SQLite database - or PGLITE or Postgres.
The consumer can query his database and mutate through the `libzb` API.

**Example of Architecture**:

```mermaid
flowchart LR
     subgraph VPN["VPN"]
        PG[("Postgres<br>Master")]
        subgraph Localhost ["VPS localhost"]
            Bridge(("ZeBridge <br> daemon"))
            NATS[("NATS <br>hub")]
        end
        PG <-- "TCP<br>SSL (opt)" --> Bridge
        Bridge <--> |"TCP"| NATS
    end

    NATS <-- "TLS" --> NATS_L
    NATS <-- "TLS / WSS" --> Lib


     subgraph Edge["Server-side consumers (per tenant)"]
        NATS_L["NATS Leaf<br>(tenant-scoped creds)"]
        NATS_L <--> Svc["microservice<br>(libzb via FFI<br>or wasm eater + pump)"]
    end

    subgraph Mobile["Mobile consumer"]
        Lib["libzb<br>applier + SQLite"]
        Lib -- "query · mutate · onChange<br>(C ABI, read-only handle)" --> App["App"]
    end

    style Bridge fill:#f59e0b,stroke:#d97706,color:#000
    style Lib fill:#fbbf24,stroke:#f59e0b,color:#000
    style NATS fill:#10b981,stroke:#059669,color:#000
    style NATS_L fill:#b8f8e3,stroke:#059669,stroke-dasharray:5 5,color:#000
```

| artifact | what it is | who uses it |
| -- | -- | -- |
| zebridge | executable (daemon) | next to PG ← ZeBridge →  NATS |
| libzb | native library, C ABI | mobile apps, desktop apps, microservices via FFI |
| zb-client-ts | npm package (self-contained TypeScript) | any JS runtime: browsers, Node, Electron, Deno, Bun |

We have worked examples for Flutter, webapps (WASM-SQLite + OPFS), and backend microservices in Node, Go,Python & Elixir.

## How ZeBridge compares

> ZeBridge is an inflow daemon that connects PostgreSQL and NATS.JS, and nothing else.
It proposes a protocol — a set of rules and workflows — to connect a consumer to NATS. The library `libzeb` implements the protocole in the conusmer code.
Reads are scoped by Postgres tenant.
Writes are authorized by NATS subject grants together with the Postgres tenant, and resolved last-write-wins.
No sync-rules DSL to write, no gatekeeper service to run — authorization and conflict resolution both live where the data already does.

| tool | what it is | how it operates |
| --- | --- | --- |
| **[PowerSync](https://github.com/powersync-ja)** | Solves "Postgres to local SQLite" for offline-first apps, with a bucketing system. | Own stateful sync engine; clients connect over WebSocket (HTTP streaming as fallback). Authorization is a real DSL: Sync Rules filter per bucket via parameter queries against trusted JWT claims, evaluated centrally. Ships a default last-write-wins-per-field conflict resolution, server-authoritative and customizable — e.g. routing conflicts to a table for manual resolution instead. |
| **[ElectricSQL](https://electric-sql.com/)** | An open-source Postgres sync engine: consumers subscribe to "shapes" of the data, synced into local SQLite/PGlite. | Own stateful sync/shape service; clients long-poll over plain HTTP with CDN-cacheable headers, so a standard CDN collapses concurrent requests for the same shape into one origin hit. Read-path only — no built-in write path or conflict resolution at all; writes go through your own separate backend API. Authorization isn't built in either: a separate proxy/gatekeeper service in front of Electric decides who gets which shape. |
| **[Debezium](https://github.com/debezium/debezium)** | The enterprise standard for CDC — feature-rich, reliable, server-to-server. | Runs on the JVM, streams into Kafka. Built for datacenter-to-datacenter pipelines — no edge-client protocol, device-facing snapshot, or mobile/web target at all. |
| **[pgstream](https://github.com/xataio/pgstream)** | A modern Go CDC tool from Xata: DDL changes, streaming to Kafka/OpenSearch/Webhooks. | Generic pipeline routing — database to database or search index. No client-facing state machine, local schema translation, or snapshot handoff to a device. |
| **[Bento](https://github.com/warpstreamlabs/bento)** | A high-performance stream processor, source to sink, with on-the-fly transforms. | A general building block, not a sync product: wiring it to Postgres CDC gets you the transform/routing layer only — the edge-sync protocol, snapshot logic, and schema transition handling are left to build. |

## Opinionated

ZeBridge makes choices a general sync engine leaves to you. They are constraints, and that is the point — each one buys a guarantee. Where ElectricSQL or PowerSync stay neutral (write what you like, the change flows back, you observe the result), ZeBridge takes positions. Here they are, so you can judge the fit before adopting it.

**Every consumer is an identity in a tenant.** A consumer connects as a *principal* — a stable, unique name — that belongs to exactly one tenant. Reads are scoped to that tenant by PostgreSQL RLS; writes are confined to that principal by NATS subject grants. There is no anonymous consumer and no cross-tenant read. Enrollment is the only way in: a JWT minted under a scoped signing key, plus a `zebridge_user_tenants` row — one identity, written once, in two projections.

**Tables must qualify — the schema carries the contract.**

* A replicated table needs a **primary key**. Because a client mints its own keys offline, a *writable* table's key must be **client-generable** — a `uuid`, not a `bigserial` the database hands out (an edge write to a sequence key would collide with the server's next insert, so the bridge refuses it).
* A writable table needs a **version column**, a `timestamptz` — never a naive `timestamp`. The timestamp guard refuses one at `CREATE`/`ALTER`, because "newer" has to be an absolute instant or last-write-wins is meaningless. Optional companions: a **tombstone** column (a delete becomes a soft-delete so an offline client cannot resurrect a removed row) and a **tiebreak** column (resolves equal versions instead of refusing both).
* **No large objects.** NATS caps the message and the bridge runs on a fixed buffer, so a row too wide for the change feed is refused *at write time*, both from the edge and from `psql`. A blob (> ~1 MB) belongs in object storage; the table holds the reference. ZeBridge moves rows, not files.

**Writes are resolved, not merely accepted — last-write-wins.** A write carries the version the client holds; the bridge applies it only if it is newer than what Postgres has, and rejects a stale one. This is a deliberate opinion, and the honest contrast: Electric and PowerSync do *not* arbitrate — every write lands and you observe whatever results. ZeBridge arbitrates at ingest, so a slow or offline client cannot silently clobber a newer edit and a stale queued write cannot undo a delete. The cost is that LWW is the only resolution offered today. A future build may add a neutral "write-through, observe the echo" mode for apps that prefer the Electric/PowerSync shape; until then LWW is enforced, and the version column is how you get it right.

**The library owns the write path — reads are open, writes go through `mutate()`.** The opinion is firm: the consumer reads its local database freely — any SQL, joins, aggregates, offline — but changes it only through the library, so every write gets the outbox, the version stamp and the LWW echo. A write that skips the library is a bug you should not be able to make by accident. *How* that is enforced depends on the local engine, and this is honest about it:

* **Browser SQLite (one OPFS connection):** the library owns the single connection and hands the app a **read-only** handle — a direct write is simply unreachable. Enforced today.
* **PGlite:** the library also carries a Postgres schema, so PGlite is a valid local engine; whether its setup can lock the write path the same way is **not yet tested**.
* **Mobile and microservice SQLite:** SQLite is the only mobile engine, and there the library does not own the connection the same way — so the lock moves into the schema: an **initial migration** makes the app-facing tables read-only (views + triggers) and routes writes through the library's own path. Enforced by the schema, not the handle.
* **Local Postgres (microservice):** the same choice as PGlite, schema-enforced.

The rule is the same everywhere; the *mechanism* that guarantees it is engine-specific, and the PGlite case is still open. It is why the library — not a set of naming conventions — is the API.

**Authorization lives where the data does — no gatekeeper, no DSL.** NATS grants (a scoped JWT signing key) decide which subjects a principal may touch; PostgreSQL RLS and the tenant guard decide which rows it may read and write. There is no sync-rules language to author and no separate authorization service to run and keep in sync — the two systems that already hold the data hold the rules, and the principal is a subject token the broker vouches for, never a claim in a payload.

**The daemon is stateless; state has one home each.** The bridge holds no certificates and does no NATS-side lookup — everything it needs (the catalogue, the rules, the tenants) comes from Postgres, and it never reads its own output back from NATS. What lives in the process is a small, self-invalidating cache, nothing durable. So a bridge is cheap to start, cheap to restart, cheap to colocate, and never itself a source of truth: ZeBridge's state is in Postgres, the consumer's is its local replica plus its NATS stream position, and neither side is the other's cache.

These opinions aim in one direction: many small consumers that read freely, write safely, and never cross the tenant line. That is what the two components are for — `zebridge` next to Postgres, `libzb` next to the consumer — and together they make the guarantees above. It is a bridge with two ends you build on, not a bare pipe.

## Server setup side

ZeBridge runs in three contexts, and they are not the same:

* **The test suite** runs Postgres and NATS natively on the host — fast to iterate, and what the scenarios drive. Development only.
* **Evaluation** — trying ZeBridge out — is best as a **docker compose** stack: Postgres, NATS and the bridge in one file, so a fresh machine comes up identically every time (fully Infrastructure-as-Code). That is what compose is for.
* **Production** is your own topology, and here compose is *not* a recommendation. Postgres may be a managed or remote instance; NATS may be remote too. What actually matters is **colocation**: the bridge must sit next to `nats-server` — their hop is plain TCP for speed, so it must not cross a network — and the strong setup puts **Postgres + zebridge + nats-server + a reverse proxy** (nginx or Caddy) together behind one boundary. The reverse proxy fronts the bridge's HTTP surface over TLS — **Prometheus scraping** (`/metrics`) and the **JWT enrollment dance** (`/enroll`) — with a domain and whatever auth you put in front. The bridge holds no certificates of its own. (Browser `wss` to NATS is served by `nats-server`'s own websocket port — behind the same proxy if you like.)

The DBA has two jobs, and they are **separable**:

* **Read-only** — run `init.core.sql`. It creates the reader role, the publication, the catalogue, the schema/DDL triggers and the read-side guards. The bridge now streams changes and consumers read them; nothing can be written from the edge. This is the base, and for many uses it is the whole thing.
* **Read/write** — also run `init.write.sql`. It adds the writer role, the per-table write guards (version stamping, soft-delete, tenant guard), RLS scoping and the enrollment table. Now a consumer can push writes, resolved last-write-wins.

A read-only deployment never touches `init.write.sql`; a read/write one runs both, in order. Everything else is settings with sensible defaults — the one dimension you usually set by hand is the memory buffer ([below](#the-memory-setting)).

### The setup sequence

On a fresh VPS, in order. Drive it with whatever you like — a shell script, your migration tool, Ansible; the steps are the same, and the compose stack simply does them for you. The SQL and config templates carry `${VAR}` placeholders, so they are rendered with `envsubst` from the admin environment first.

```sh
set -a && source .env.admin && set +a
```

1. **Create the bridge's database objects.** Read side, then write side (write side only if you want edge writes):

   ```sh
   envsubst < init.core.template.sql  | psql "$ADMIN_URL" -v ON_ERROR_STOP=1 -f -
   envsubst < init.write.template.sql | psql "$ADMIN_URL" -v ON_ERROR_STOP=1 -f -   # read/write only
   ```

2. **Declare your tables** — the important step. Run your own migrations to create the application tables, and call `zebridge_enable(...)` for each table you want replicated. One call writes the `zebridge_catalogue` row, installs the guards, scopes RLS and adds the table to the publication, atomically:

   ```sql
   SELECT zebridge_enable('public.notes', writable => true,
       version_col => 'updated_at', tenant_col => 'tenant_id', dry_run => false);
   ```

3. **Place the wire grammar.** Copy `grammar.json` where the bridge and the NATS setup can read it — it holds the static names both sides share.

4. **Configure and start NATS with JetStream.** Render the server config from its template, then start it:

   ```sh
   envsubst < nats-server.conf.template > nats-server.conf
   nats-server -js -c nats-server.conf
   ```

   For the operator/JWT world (consumers), run `scripts/native/jwt-bootstrap.sh` once to build the operator, accounts and scoped signing keys, and start NATS with the operator config it emits instead.

5. **Configure the bridge.** Fill in `.env.bridge`: the connection strings (`DATABASE_URL`, `DATABASE_WRITER_URL`, `NATS_URL`), the credential (`NATS_CREDS` for JWT, or `NATS_NKEY_SEED`), and `BASE_BUF` if the default is too small for your widest table.

6. **Start the bridge.**

   ```sh
   set -a && source .env.bridge && set +a
   ./bridge --slot my_slot --pub my_pub --port 9090
   ```

   At boot it reads the catalogue, reconciles the CDC/INIT streams, publishes every table's schema, and begins streaming.

7. **Verify the wiring** — ⚠️ *TODO: a first-class post-boot checker.* The pieces exist: [`scripts/scenarios/check.py`](scripts/scenarios/check.py) compares what is *declared* against what is *actually live* (catalogue vs bound stream subjects, tenant columns vs schema, orphan replication slots, connection budgets), and the `zebridge_audit_publications()` / `zebridge_audit_write_guards()` / `zebridge_audit_sweeper()` SQL functions report unscoped publications, missing write guards, and unreachable sweeper tenants. What is still wanted is **one command, run after the bridge is up, that gives a single green/red verdict on the whole wiring**. Until it exists, run `check.py` and read its findings.

### Roles and privileges

ZeBridge uses two PostgreSQL roles:

* `bridge_reader` — SELECT + REPLICATION, physically unable to write (the read side, `init.core.sql`).
* `bridge_writer` — no table privilege until a table is opened one at a time (the write side, `init.write.sql`).

The superuser credentials create these roles **once**, at the init step; the bridge never connects as the superuser at runtime. Each role carries a connection-limit budget, so the bridge cannot exhaust the database.

📖 **[SECURITY.md](SECURITY.md) is the reference**: what each role holds, what the schema must satisfy to be writable or tenant-scoped, what to do after a migration, and what is *not* protected — with a table of where every claim is tested.

### Credentials & trust boundaries

Four separate keys, four separate boundaries. Plain words:

| boundary | credential | who holds it |
| --- | --- | --- |
| create the roles (once) | PostgreSQL **superuser** | the DBA / the init step — never the bridge at runtime |
| bridge → PostgreSQL | the two **role passwords** (`bridge_reader`, `bridge_writer`) | the bridge, in `.env.bridge` |
| bridge → NATS | an **nkey** (or a service-role JWT) | the bridge. Plain TCP on the colocated hop, by design |
| consumer → NATS | a **JWT** (scoped signing key) + nkey seed | every consumer, from enrollment |

The consumer boundary is one model for **every** consumer type — webapp, mobile, or microservice. The JWT and its check are the same everywhere; only the transport (WebSocket for the browser, TLS-TCP for native), where the credential is stored, and the client runtime differ. The permissions live once on the signing key's template (`mutation.{{name()}}.>`, `cdc.{{tag(tenant)}}.>`); a new consumer is one minted JWT plus one `zebridge_user_tenants` row, no server-config edit. On the PostgreSQL side, RLS and the tenant guard bound the rows a principal may read and write. See [SECURITY.md](SECURITY.md) for depth.

### NATS streams and buckets

ZeBridge authenticates to the NATS server using nkeys.

ZeBridge uses three streams and several KV buckets (schemas, snapshots, tenants, generations) plus per-tenant object stores for the bidirectional flow ZeBridge ↔ NATS ↔ consumer.
The naming is **shared** and declared in [grammar.json](grammar.json).

A ZeBridge instance is started with one config. The DBA starts the NATS server with its own config. `grammar.json` is the static wire grammar shared between the two: stream names, subject prefixes, and KV bucket names, declared once (`streams`, `subjects`, `kv`, `cdc_streams`, `init_streams`, `open_tenant`, `generations`). Which tables replicate, and how, lives in the database: one `zebridge_catalogue` row per table, written by `zebridge_enable(...)`. `nats-init` creates only the MUTATIONS and REQUESTS streams and the KV buckets; the bridge creates and reconciles the CDC/INIT stream family itself at boot, from the catalogue. Both sides authenticate with nkeys.

**Three data flows**:

1. **Bootstrap** (READ): the consumer takes each table's *schema* (from the `schemas` KV) and seeds it — from the **generation chain** (objects + a manifest in the `generations` KV) normally, or from a **snapshot** (INIT stream) when no chain exists yet: PG → ZeBridge → NATS/JS.
2. **Real-time CDC** (CDC stream, READ): the consumer receives INSERT/UPDATE/DELETE events as they happen: PG → ZeBridge → NATS/JS.
3. **Real-time ingress** (MUTATIONS stream, WRITE): the consumer updates its local storage and sends the intended change to NATS/JS → ZeBridge → PostgreSQL.

| Stream | Purpose | Retention (default) | Consumer Pattern | Role |
| -------- | ------------------- | ------------- | ----------------------- | -- |
| **CDC** | Real-time egress changes | 8 days | Continuous subscription | READ |
| **INIT** | Bootstrap snapshots | 7 days | One-time replay | READ |
| **MUTATIONS** | Real-time ingress changes | 7 days | Continuous subscription | WRITE |

Besides the three streams, the bridge maintains the seeding buckets: the **`generations` KV** holds one chain manifest per `<tenant>.<table>`, and a per-tenant **`gen-<tenant>` object store** holds the full and delta objects the manifest points to. These are the normal seed source; the `INIT` stream is the snapshot fallback, and the `snapshots` KV holds a descriptor per dump. The producer provisions the object stores at runtime, the same way it provisions per-tenant CDC streams.

The CDC/INIT stream family is owned by the bridge: at boot it creates any missing `CDC_<TENANT>`/`INIT_<TENANT>` stream with file storage, limits retention, s2 compression, these max-ages and a 1 GB max-bytes cap — deliberately modest, because JetStream `max_bytes` is a reservation against the server's storage budget. It also sets `CDC_PUBLIC`'s subjects authoritatively to `cdc.<tbl>.>` for every catalogue-public table plus `cdc.<open_tenant>.>`, and ensures `INIT_PUBLIC` exists. MUTATIONS and REQUESTS keep the limits `nats-init` gives them.

⚠️ **CDC deliberately outlives INIT, by a day.** Snapshot generation isn't instant — it's a single sequential worker with no timeout, so "a small table can wait behind a large one" (see [Thread Model](#thread-model-7-threads)) can genuinely delay how long a snapshot takes to finish landing in INIT. If CDC and INIT expired on the same clock, a CDC event for a write that happened *while* a snapshot was still queued could age out before that snapshot itself does — leaving a client that replays a still-valid snapshot with a gap right at the start of what it needs from CDC. The one-day margin exists to absorb that queueing delay; INIT's 7 days is what actually matters operationally: **the longest a consumer can be offline before it must re-seed from a fresh snapshot instead of just resuming CDC.**

Snapshot *requests* (not the data itself) are separately throttled: the `REQUESTS` stream holds one request message per table at a time, for up to the `SNAP_RET` window (`SNAP_RET_SECONDS`) — a second request for that table inside the window is refused, so a client is expected to check the `snapshots` KV bucket first. `SNAP_RET`'s production default is intentionally close to INIT's own 7-day retention (minus a 30-minute margin), not a short debounce: a snapshot already in INIT is good for its whole retention window, so a fresh dump of the same table before then would just be redundant load on Postgres. The KV descriptor itself has no expiry, so a client always has something to check regardless of where `SNAP_RET` currently stands.
Consumers use these streams to interact with NATS; the exact names are declared in `grammar.json`.

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
The defaults (ring=32768, BASE_BUF=14 → 16 KB rows, MAX_COLUMNS auto-detected at 8 for a `users`-shaped table) land around 1 GB, dominated almost entirely by the data slab — see the worked formula below.

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
* NATS has the MUTATIONS and REQUESTS streams and the KV buckets (the bridge creates the CDC/INIT streams itself at boot).

The bridge is then started with:

* a memory budget: `BASE_BUF` (default 2^14 = 16 KB) and `RING_BUFFER_COUNT` (default 32768), sized to the tables this instance handles — see [Sizing BASE_BUF and RING_BUFFER_COUNT](#sizing-base_buf-and-ring_buffer_count). `MAX_COLUMNS` is usually left unset and auto-detected.
* a unique `--slot` — the WAL pointer PostgreSQL keeps for this instance. Each running instance needs its own.
* a unique `--port` for its telemetry webserver. Each running instance needs its own.
* `--top`, defaulting to `./grammar.json` — the stream/bucket names shared between ZeBridge, NATS and consumers.
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
./bridge --slot my_slot --pub my_pub --top grammar.json
```

**Principal authentication** (the end user of a consumer app):

The principal is authenticated by the consumer app, and carried through NATS's JWT/operator model so the bridge can pass it to Postgres for RLS policies.

|  |  subscribe  |   publish  | needs an account |
|--|--|--|--|
| read-only consumer  | cdc.>, init.>, KV.schemas.>, KV.snapshots.> | snapshot.request.>       | no |
| read-write consumer | the same | + mutation.`<principal>`.> | yes |

## Design overview

ZeBridge projects PostgreSQL onto NATS and never reads its own output back. **Postgres is the source of truth for the bridge; NATS is the source of truth for consumers.** A consumer's state only ever arrives through the change feed — there is no optimistic path around it.

**The main loop, four steps:**

1. **Read the WAL.** One thread follows PostgreSQL's logical replication stream (`pgoutput`), in order.
2. **Decode into a fixed buffer.** Each change is decoded into a pre-allocated slot — memory is bounded at startup, not grown per event.
3. **Batch to NATS.** Decoded events are published to JetStream in batches.
4. **Acknowledge.** Only after JetStream confirms does the bridge ACK that position to Postgres, which can then reclaim WAL. If the bridge crashes, Postgres keeps the unpublished WAL — nothing is lost.

**Two sides, two patterns.** Egress (Postgres → NATS) is a **push**: the bridge publishes as changes happen. Bootstrap and ingress (consumer ↔ NATS) are **pull**: the consumer pulls CDC and snapshot chunks at its own pace and pushes its writes on its own subject. The consumer controls replay.

**Seeding: generations first, snapshot as fallback.** A fresh or fallen-behind consumer needs a starting point, and the normal path is a **generation chain**. The bridge periodically builds a *full* plus a series of *deltas* per table, stored as objects with a small manifest in KV. The consumer reads the manifest, applies the full, then only the deltas newer than its watermark — cheap and incremental, so a returning consumer usually just needs the latest delta. Only when there is no chain yet — a brand-new tenant or table — does it fall back to a **snapshot**: a one-shot dump the bridge serves from Postgres, one table at a time, chunked by bytes to fit one NATS message. Either way the consumer lands on one cutoff and follows CDC from there.

**Why generations** (and why snapshots are being retired):

* **No connection storm on Postgres.** A snapshot is served *per request*, one at a time, from Postgres — so a fleet reconnecting at once queues against the database. A generation is built once by a background job (the producer, on a cadence) and pushed to object storage; any number of consumers then pull the same almost-fresh chain from storage, never touching Postgres. The storm hits object storage, which fans out cheaply.
* **One copy, partitioned — not N dumps.** The chain slices the database by tenant and table, so storage holds a single partitioned copy, not a fresh full dump per consumer request. The data lives once.
* **CDC stays small.** Because a consumer always seeds from a recent generation and only needs CDC back to the last cutoff, the CDC stream retains at most about two generation windows — bounding its size instead of letting it grow with the offline window.

Snapshots remain today only as the cold-start fallback for a table with no chain yet, and will be **pruned** once generations cover that case too.

**Telemetry is a first-class tool.** Every instance exposes `/metrics` (Prometheus), `/status` (JSON), `/health`, and structured logs on stderr (Loki). Metrics and logs answer different questions and both matter — see [Monitoring & Telemetry](#monitoring--telemetry).

**The message format.** CDC events and snapshot rows travel as **MessagePack** — compact, type-safe, fast (it keeps the int/float/binary distinctions JSON loses). Schemas travel as JSON in two shapes (PostgreSQL and SQLite), so a client builds its local tables in either. Every write carries a message id, for idempotent at-least-once delivery.

**One instance = one slot = sequential.** A bridge runs one replication slot and processes it in order. Scale by running more instances on more slots/publications, each with its own buffer — not by making one instance bigger.

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

Snapshots — requesting one, replaying it, the two-clock bookkeeping around it — are the library's job, not the app's ([the consumer side](#the-consumer-side--use-the-library)). This section is about the bridge's own guarantees.

---

## Quick review of PG & NATS setup

This paragraph is only a rough overview of the operations needed to set up PostgreSQL and NATS/JetStream.
In short:

* you enable PG logical replication, run migrations to creates users, grants, and publication. The database itself is another separate migration from these admin setup,
* you configure NATS to run JetStream. The bridge authenticates to NATS with an **nkey**; consumers authenticate with **JWT/operator** credentials (a scoped signing key holds their grants). `nats-init` creates the MUTATIONS/REQUESTS streams and the KV buckets; the bridge creates the CDC/INIT streams itself at boot.

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

## The consumer side — use the library

You do not talk to NATS by hand. The library does all of it: it watches the schema, seeds the local database (from the generation chain, or a snapshot when there is no chain yet), follows the change feed, applies rows last-write-wins, and sends your writes. It owns the local SQLite, so a write can only go through the library.

Two builds of the same core:

* **`libzebridge.js`** — a self-contained TypeScript package that runs **as-is** in any JS runtime: browsers, Node, Electron, Deno, Bun. No wasm and no native library needed — a JavaScript host just uses this.
* **`libzebridge`** — a native library with a C ABI for mobile apps, desktop apps and microservices (via FFI).

The API is small — three verbs and a doorbell:

* **`query(sql)`** — read your local database directly. Any SQL: joins, aggregates, offline. The replica *is* the API.
* **`mutate(table, key, values)`** — one write, three verbs (insert, update, delete), resolved last-write-wins. This is the only way to change data.
* **`onChange(table, cb)`** — a doorbell. When the change feed touches a table, re-run your query.

That is the whole contract for an app author. The wire format — the NATS subjects, the KV buckets, the snapshot chunking, the tenant scoping — is the library's own business, not yours. It is written down in [PROTOCOL.md](PROTOCOL.md) for one reason: someone writing a **new** language binding. Building an app, you never read it.

**Getting a consumer connected** is enrollment: the app authenticates to your backend (or the bridge's mint endpoint), receives a JWT credential, and connects. The principal comes back *inside* the credential — the consumer never types it. The same model works for every consumer type; see [Credentials & trust boundaries](#credentials--trust-boundaries).

**Examples**: [App.tsx](/web-consumer/src/App.tsx) (browser, live), a [Flutter](/flutter) example, and planned Node, Go, Python and Elixir microservices.

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
* NATS admin has created the MUTATIONS/REQUESTS streams, KV buckets, and credentials — the bridge creates the CDC/INIT streams at boot

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

⚠️ The level *names* differ between input and output: you set `LOG_LEVEL=warn` but the
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
| `bridge_wal_lag_bytes` | large and growing across checkpoints | WAL PostgreSQL is *retaining* on disk for the slot, until `max_slot_wal_keep_size` |
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
  "last_ack_lsn": "0/1832ce8",
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

**What happens if PostgreSQL emits CDC events when no NATS clients are connected?** Nothing accumulates unbounded on either side. The bridge keeps ACKing PostgreSQL as normal — that only depends on JetStream, not on a consumer being present — so PostgreSQL's WAL stays bounded regardless. On the NATS side, the `CDC` stream's own retention policy (`--max-age`, `--max-bytes` — see [The NATS streams and buckets](#the-nats-streams-and-buckets)) purges old events even with zero subscribers. A client that reconnects after being offline re-seeds from the latest **generation chain** (or a snapshot, if it has fallen past the chain's retention) and resumes from `CDC`.

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

Each snapshot carries the LSN it was taken at, so a consumer can reconstruct exact table state at that point — and discard any CDC event at or before it as already included (the library handles this — [the consumer side](#the-consumer-side--use-the-library)).

Event ordering follows from [one WAL-reading thread per bridge](#design-overview): PostgreSQL hands the bridge a strict total order — transactions in **commit** order, and each transaction's changes in execution order — and the bridge reads it sequentially.

⚠️ **Order is preserved WITHIN a table, not ACROSS tables.** A flush is grouped by subject before publishing, which collapses interleaving: when a run begins mid-pair, a child's batch can be published before the batch carrying its parents. Measured. Most consumers never notice — rows are applied by primary key, last-write-wins, idempotent — but a consumer holding a constraint that spans tables (a foreign key) must be order-tolerant, which is why `libzb` holds a row whose parent has not arrived and retries it after the next batch.

⚠️ **Do not sort by `lsn` to "restore" order.** LSNs are not monotonic in delivery order: a transaction that begins earlier but commits later arrives later carrying a *lower* LSN (measured: `lsn=8700486880` delivered before `lsn=8700486488`). Delivery order is the truth; `lsn` is a watermark for "was this already in my snapshot?", nothing more.

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

The SPSC queue serves two purposes: it decouples WAL reading from NATS publishing (so a slow flush never blocks the WAL reader directly), and it absorbs a NATS outage without losing anything — see [Bridge ACK Flow and NATS outages](#bridge-ack-flow-and-nats-outages) for what happens when it fills.

❇️ Sized by `RING_BUFFER_COUNT`; roughly 1 second of buffer at 60K events/s with the defaults.

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
* Replication slot preserves position for restart <- ??

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
  The write path uses pipeline mode (`PQenterPipelineMode`, libpq 14+), which the libpq documentation describes as *"client-side and compatible with any server supporting the v3 extended query protocol"* — so the database itself can be older (Postgres ≧ 14).
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

( *admin|admin*, then set the password of your choice)

**Run Tests**:

```bash
zig build test
```

`Python` and `envsubt` on host to play */scripts/scenarios*

---

## Configuration

All configuration constants are centralized in `src/config.zig` and `grammar.json`. Per-table replication rules (tenant column, LWW columns, tombstone) live in `zebridge_catalogue`.

### Key Settings

**Snapshot configuration:**

* Chunk size: `10_000` rows per batch
* Subject pattern: `init.snap.{table}.{snapshot_id}.{chunk}`
* Metadata subject: `init.snap.{tenant}.meta.{table}` (tenant-scoped, like every snapshot subject)
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

`sizeof(CDCEvent)` is small and fixed regardless of table shape — `columns` is a *slice*
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

* **`BASE_BUF`** (log2 bytes, range 10–20) is *how large a single row may be*. Size it
  to your widest row: a `jsonb` document, a long `text` column, a big array.
* **`RING_BUFFER_COUNT`** (range 1024–1048576, **clamped** to the nearest bound if you go outside it — an out-of-range value used to fall back to the *default*, so asking for 64 got you 65536) is *how many events can queue while NATS is unreachable*. 65536 slots ≈ 1 second at 60K events/s. Below that, a NATS blip starts back-pressuring the WAL reader sooner.
* **`MAX_COLUMNS`** is *how many columns one event may carry* — normally left to
  auto-detection; override it only to widen the ceiling ahead of a migration or to pin the
  value across instances.

> Raising one and lowering the other keeps memory flat, at the cost of outage tolerance.

#### Total memory: read it from the boot log, not a table

Because `MAX_COLUMNS` is now resolved per instance rather than a single fixed constant, a
static `BASE_BUF × RING_BUFFER_COUNT` table can no longer show the true total in one
number — the columns term depends on what *your* publication's widest table looks like.
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

⚠️ Treat either figure as a reference point, not a spec — absolute throughput moves with machine, build mode, PostgreSQL version, and host load (a background process competing for CPU cores measurably drops it). A rerun that differs is not automatically a regression; see below for the number that *is* comparable across machines.

<details>
<summary>Full method, raw output, and how to read the <code>LOOP</code> line</summary>

**Method** (Docker environment):

| | |
| --- | --- |
| machine | Apple M2 Pro, 10 cores, macOS |
| build | `zig build -Doptimize=ReleaseFast` — a Debug build is several times slower |
| PostgreSQL | 18.4 in Docker on the same machine |
| NATS | JetStream, file storage, in Docker on the same machine, **no consumers attached** |
| bridge | one instance, `BASE_BUF=14` (16 KB/event), `RING_BUFFER_COUNT=32768`, MessagePack |
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
also 2000 *transactions*, so the WAL carries 2000 BEGIN/COMMIT pairs — visible as the gap
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

The number that *is* comparable across machines is `iters` for a fixed event count. The
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

## Requirements, Dependencies & Licenses

**Managed via `build.zig.zon`:**

* [zig-msgpack](https://github.com/zigcc/zig-msgpack) - MessagePack encoding. License MIT

**Currently vendored (in `/`):**

* [nats.zig](https://github.com/lalinsky/nats.zig). License Apache 2.

**System:**

* [libpq](https://www.postgresql.org/docs/current/libpq.html) ≧ 14  at build time (pipeline mode). License MIT

* `PostgreSQL` ≧ 14 (`pgoutput` **binary** mode),
* `Nats/JetStream` 2.10+

---

## Roadmap

**Current — v0.14:**

* [x] **The catalogue is the config.** One `zebridge_catalogue` row per table, written by `zebridge_enable(...)`, is the single source the bridge, sweeper and producer read; env vars are optional overrides. `grammar.json` holds only the static wire names; the bridge reconciles its NATS streams itself at boot.
* [x] **Operator/JWT auth for consumers**, with a scoped signing key holding the grant template — a new consumer is one minted JWT + one mapping row, no server-config edit. An enrollment/mint endpoint on the bridge issues credentials; the bridge signs user JWTs in pure Zig.
* **The client library**, step one: `libzebridge.js` (TypeScript pump + applier) drives the browser consumer today.
* Read-only or read/write, separable (`init.core.sql` / `init.write.sql`); tenant-scoped reads via RLS, one-or-N bridges per tenant; last-write-wins ingress with tombstone + tiebreak.
* Delta **generations** (full+delta chains in object storage) for fast seeding of hot tables.
* Per-instance memory sizing, preflight schema analysis, a PG connection budget enforced in the roles.
* Telemetry (`/metrics`, `/status`, `/health`) and a robustness suite: chaos, adversarial-input, and memory-leak scenarios (both build modes).

**Next:**

* [ ] **`libzebridge` native + the shared Zig eater** — the §10 vision: one sans-I/O applier core in Zig, compiled native (`.so`/`.dylib`/`.dll`, C ABI) for FFI hosts — mobile (Swift/Kotlin/Dart), native microservices, Windows (.NET/C++) — and to `applier.wasm` for **non-JS** hosts that want that same core without native FFI (Go via `wazero`, Python/Ruby/Elixir via wasmtime). JavaScript hosts do not need it: `libzebridge.js` already *is* the core, in TypeScript.
* [ ] **Auth callout** — the login rides the NATS connect (`$SYS.REQ.USER.AUTH`), so even the mint endpoint disappears; the bridge is the responder.
* [ ] Split READ (CDC + bootstrap) onto a **standby replica** (PG ≧ 16) from WRITE on the primary, with async snapshotting on the replica.
* [ ] TLS on the NATS↔leaf and PG↔bridge links for cross-network deployments.
* [ ] **Windows Server for the bridge daemon** — Zig targets `x86_64-windows`, but the daemon has Unix-isms to port first: POSIX signal handling for graceful shutdown (→ `SetConsoleCtrlHandler`) and the `poll()` WAL loop (→ `WSAPoll`), then the scenario suite re-run on Windows. Colocation with NATS is usually Linux, so this is for Windows-only shops. **Separate from the consumer**, which already runs on Windows today — native `libzebridge.dll` via P/Invoke, or `libzebridge.js` in a Node/Electron service, no wasm needed.
* [ ] **Prove the write-path lock on every local engine** — enforced today for browser SQLite (single owned connection); still to verify for PGlite, and to implement/verify the schema-migration lock (views + triggers) for mobile/microservice SQLite and local Postgres.
* [ ] **Retire the snapshot path.** Generations are the seed mechanism; snapshots survive only as the cold-start fallback. Cover that case with generations (a chain built on first sight of a tenant/table) and remove the snapshot request/serve path — one fewer moving part, and Postgres is never queried per consumer.
* [ ] **A post-boot wiring checker** — one command, run once the bridge is up, that verifies the whole chain (catalogue ↔ streams ↔ grants ↔ RLS) and returns a single verdict. Today the pieces are `check.py` and the `zebridge_audit_*()` functions; the goal is to unify them into one green/red gate.
* [ ] Metrics export to StatsD/InfluxDB

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

In *nats-server.conf*, set the **public** key:

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

**Contributions welcome!** If you find it useful (or find gaps), feedback is valuable.
