# Sync PostgreSQL locally

<p align="center"><img  width="355" height="233" alt="Screenshot 2025-12-26 at 02 37 57" src="https://github.com/user-attachments/assets/b3701ef4-2d58-497a-be21-52ad1b970644" /></p>

![Zig support](https://img.shields.io/badge/Zig-0.16.0-color?logo=zig&color=%23f3ab20)

**What is it?**:  An opinionated, bidirectional bridge to synchronize a single PostgreSQL database with a _local replica_ via the message broker NATS/JetStream.

A two-pillar architecture where consumers build upon the client library.

```mermaid
flowchart LR
     subgraph VPN["VPN"]
        PG[("Postgres")]
        subgraph Localhost ["VPS localhost"]
            Bridge(("ZeBridge"))
            NATS[("NATS")]
        end
        PG <--> Bridge
        Bridge <--> |TCP| NATS
    end

    NATS <--> |WSS| Lib


    subgraph Edge["Consumer"]
        Lib(("libzb"))
        SQL[("Local")]
        App["mobile<br>browser<br>service"]
        Lib <-->SQL
        Lib <-->App

    end

    style Bridge fill:#f59e0b,stroke:#d97706,color:#000
    style Lib fill:#fbbf24,stroke:#f59e0b,color:#000
    style NATS fill:#10b981,stroke:#059669,color:#000
```

**Local_DB supported flavours**: standard SQLite, PGlite or PostgreSQL.

**How does it work?**: The bridge architecture is split into two core components: a daemon and a client library that makes syncing a breeze.

* the daemon `ZeBridge` (ZB): a Zig executable that connects to PostgreSQL (PG) and to NATS/JetStream (NATS). It streams schemas, seeds by chunks and sends PG changes onto NATS. It applies writes coming back from the consumer to the primary database.
This is a lightweight process that can be started / stopped gracefully on the fly.
* a client library: it abstracts all the NATS connection and the storage into local database. The consumer gets offline-first by default with an optimistic write. When the connection is on, the final result comes back naturally, echoed. No retry, almost nothing to do. The API of the library is tiny and comes in two flavours: a TypeScript library `zb-client-ts` and a native dynamic Zig library `libzb` with a  C ABI  FFI-compatible. The `TS` library uses a push model for reactivity whilst the Zig C ABI library uses a pull model as the host owns the library and polls on every tick. 

**Consumers**: The library can be integrated across a wide range of runtime environments.

* Mobile Native Apps: Utilizing native SQLite with file system storage.
* Browsers and Webapps: Leveraging OPFS support for sqlite-wasm or PGlite.
* Backend Services: Running PGlite, native SQLite, or standard PostgreSQL.

**Design**: This tool is built to keep synchronized replicas of a large volume of small to medium consumers via the NATS message broker with small to medium Postgres databases.
The daemon is engineered to be light (~4 MB executable), fast, secure, stateless with near instant startup.

* **Performance**: While Postgres is I/O bound, the daemon is CPU bound with minimal memory allocation. You can expect the flow PG → NATS to reach >200k evt/s, and a sustained  >20k mut/s flow NATS → PG, boundary scoped.
The consumer's local database ingress/egress depends a lot upon your device. Values around 15 +/- 5 k evt/s can be reached.
Trust is earned. Test first. See [SPEED_TEST.md](#speed_test.md)
* **Multiple instances**: run several instances of ZeBridge on the same Postgres publication, each with its own slot (and port). This enables you to follow large slow moving tables independently from small tables with heavy changes.
* **Mobile-First Synchronization**: to optimize mobile bandwidth and reliability, we use a delta-chain process with aggressive compression for seeding and reseeding. This mitigates the need for long, expensive unitary CDC catchups.
* **Geographic or Tenant division**: with NATS leaf nodes, you can choose to use nodes by tenant, or geographically distributed, when it makes sense.
* **Strict authentication**: because NATS is exposed to the internet and contains data, users are strictly tenant scoped and access grants are encoded in a JWT, immediately revokable by the DBA.
* **Standby Read Replica ready**: you can use a dedicated Postgres standby replica for all the reads.
* **Schema translation**: the replicas are built using schemas transcriptions. For PGlite, it is a native transcription. For SQLite, primary key NOT NULL (even of composite type), foreign key references (`PRAGMA foreign_key='on'), on delete cascade/no action, and (multi-column) unique index are transcribed into SQLite schemas. 
  
**Opinionated**: Because our goal is to sync Postgres databases locally with strict predictability by preventing unexpected concurrent writes, we have a few rules that we stamped 💡 _good practices_: strict memory boundaries, safety enforced by tenant, enrollment by tenant and JWT, enforced schemas, foreign key cascade mitigation, conflict resolution via last-writer-win (LWW) enforced in the schema, table suspension, and controlled local writes propagation.

Although they might seem rigorous, these rules are mostly standard and well known, almost mechanical, schemas.
See [The daemon](#the-daemon) for more details.


**Configuration**: Because the engine uses a pre-allocated ring buffer with zero allocation on the hot path, the primary runtime configuration of the engine is the **fixed-size buffer**. After this, the daemon takes a publication and a slot, and uses its default port. The rest are env variables: 

❗️ Read [Sizing the ring](#️--sizing-the-ring).

Depending on the write volume, the schema sizes of your published tables, and if you have lengthy cascading transactions, the total buffer allocation can be configured anywhere from 16 MB to 6+ GB.

Any change in the buffer, for special live migrations that could enable larger tables than the current setting, or for tables with numerous columns exceeding the max column expected to be larger than the current setting, needs a daemon restart: see [Restart Rules](#restart-rules).

Defaults are `BASE_BUF=12` (4 KB/row), `RING_BUFFER_COUNT=32768`and `MAX_COLUMNS=128`, consuming around 150MB.

**Observability**: production-ready out of the box. It exposes standard Prometheus metrics for performance tracking and structured logs optimized for Loki and Grafana dashboards. The metrics are all owned by the daemon, meaning metrics from its Postgres catalogue and self reflecting metrics.

See the detailed file [TELEMETRY.md](#telemetry.md)

**Status**: More than an experiment. Chaos and live tested, early adoption stage but not battle tested.

---

#### Limitations

We expose them directly so you can judge.


##### Type limitation

PGlite will accept the native Postgres schema.
  
SQLite has a limited number of storage class, `INTEGER`, `TEXT`, `REAL`, `BLOB`. We transcribe Postgres schemas into an SQLite schema using `STRICT` to enforce type casting (not by affinity).

Postgres `NUMERIC` type is casted into `TEXT` to distinguish  2.3 from 2.30000.

JSON/JSONB datatype from Postgres are stored as `TEXT` in SQLite.

We don't support spatial data EWKB encoded because the client would need to install SpatiaLite, or use PostGis with PGlite.

🚦 CDC events that contain unsupported columns **suspends** the table.
See [Suspended tables](#suspended-tables).

##### Size limitation

`ZeBridge` is not designed for massive databases or tables storing large objects (BLOB) or an extra large number of columns. Firstly because NATS restricts payloads (< $2^{20}$=1 MB by default, safe up to 8 MB). This default limit of 1 MB is already very large for text - 200.000 words, 400 pages, or a huge JSON. On the other side, you have to adjust the fixed-size buffer used by ZeBridge to support such payloads.

🚦Because of our memory model, tables can get **suspended** _at runtime_ for `row_too_large`.
See [Suspended tables](#suspended-tables).

> 💡 _Good pratice_: large payloads belong to object storage: database tables should exclusively contain metadata or an external reference (e.g., an S3 bucket URL) to the blob data; unless small, not PDF nor base64 encoded images for instance.

  
##### Cross-tenant ordering limitation

If fields are referenced across tenant without a foreign key, the result can look wrong for a moment. For example, an order scoped to the tenant `accounting` referencing a document that arrives as public data can, for a moment, arrive first and look wrong before the document lands. It is not related to LWW.

>💡 _Good pratice_: the solution is a choice made when designing tables. Only a **foreign key** guarantees that order, because the bridge then knows the schema constraint so that a child must be hold until the parent lands when a foreign is declared. Without this, impossible to know. 

---

## Table of Contents

* [Overview](#overview)
  * [Two pillars](#two-pillars)
  * [The backend and the frontend in short](#the-backend-and-the-front-end-in-short)
  * [Example of a deployed system](#example-of-a-deployed-system)
* [The daemon](#the-daemon)
  * [Schemas & Migrations](#schemas--migrations)
  * [Suspended tables](#suspended-tables)
  * [Restart rules](#restart-rules)
  * [Troubleshooting](#troubleshooting)
* [ZeBridge CLI](#zebridge-cli)
* [The consumer side](#the-consumer-side)
  * [The TypeScript API](#the-typescript-api)
  * [The C ABI library](#the-c-abi-library)
* [Architecture & Internals](#architecture--internals)
  * [Design overview](#design-overview)
  * [Inside](#inside)
  * [Safety & Guarantees](#safety--guarantees)
* [Configuration & Tuning](#configuration--tuning)
  * [Key settings](#key-settings)
  * [Sizing the ring](#️--sizing-the-ring)
* [Deployment & Setup](#deployment--setup)
  * [Server setup side](#server-setup-side)
  * [Quick review of PG & NATS setup](#quick-review-of-pg--nats-setup)
  * [Running the Bridge](#running-the-bridge)
  * [Local Build Instructions](#local-build-instructions)
  * [Notes on nkeys](#notes-on-nkeys)
* [Requirements, Dependencies, Licenses & Sources](#requirements-dependencies-licenses--sources)
* [Roadmap](#roadmap)

---

## Overview

### Two pillars

| artifact | what it is | who uses it |
| -- | -- | -- |
| zebridge |  POSIX based executable <br>- Linux, FreeBSD, OSX | daemon running  next to Postgres and  NATS |
|||
| libzb | native library, C ABI | FFI Consumers: mobile apps, desktop apps, microservices via FFI |
| zb-client | npm package <br>(self-contained TS) | JS Consumers: browsers, Node, Electron, Deno, Bun |


`ZeBridge` projects Postgres into NATS and back. It defines a protocol—a set of rules and workflows—for a consumer to connect to NATS and to the local replica.

The C-ABI `libzb` library - for FFI users - and the `zb-client-ts` library (no WASM) - for any JavaScript-based consumers - implements this protocol.

The library abstracts away the complex choreography required to manage NATS streams, KV buckets, data decompression and deserialization, and local tables which contains the state of a client.

The client library shrinks this orchestration down to a few primitives: `connect()`, `close()`, `newVersion()`, `query()`, `mutate()` and `onChange()`.

### The backend and the frontend in short

* **Postgres**:
  * the DBA defines two Postgres USERs,  READER and WRITER, and uses them to install the Postgres functions and triggers neede by ZeBridge,
  * the DBA migrates the database, runs a diagnose to ensure the schemas follow the _good pratice rules_, and removes any deviation.
  * the DBA runs `SELECT zb_enable()` on each table attached to the desired publication,
  🔔 These three steps guarantees the sync of the engine.
* **NATS**:
  * the DBA generates an NKEY pair for authentication: `bridge --gen-nkey`.
  * the DBA starts NATS with the NKEY seed in the config, and enables JetStream,
* **ZeBridge**: 
  * the DBA sources the `.env.bridge` file that contains the READER and the WRITER and the public NKEY part, 
  * starts with `bridge --pub my_pub -slot my_slot`.
* **Client Authentication**: since NATS is exposed to the internet, users are strictly authenticated.
  * the bridge is OAuth agnostic; the DBA assigns the user identity returned by the OAuth in a tenant in the table `'public.zb_user_tenants'`.
  * the JWT setup: see []
*  **Frontend**: the dev builds on top of the library to interact with the storage, no NATS incantations.
   *  he sets up the `libzb` or `zb-client-ts` with the storage / database flavour (SQLite, PGlite) and the domain to reach NATS,
   *  he invokes the few primitives to interact with the database: `connect`,  `query`, `mutate` and `onChange`. 

### Example of a deployed system

<details><summary>The diagram</summary>

```mermaid

graph TD
    %% Styling and Definitions
    classDef external fill:#f9f,stroke:#333,stroke-width:2px;
    classDef proxy fill:#bbf,stroke:#333,stroke-width:2px;
    classDef internal fill:#dfd,stroke:#333,stroke-width:1px;
    classDef secure fill:#fdd,stroke:#333,stroke-width:1px;
    classDef telemetry fill:#fff2cc,stroke:#d6b656,stroke-width:1px;
    classDef bridge fill:##bdf3ff,stroke:#ac0100,stroke-with:2px;
    classDef maybe_external fill:#dfd,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5;

    %% External Clients
    User([mobile / browser <br> ---libzb --- <br>SQLite / PGlite]):::external
    LocalDB[(localDB <br>PG/lite<br>SQLite)]:::secure
    LocalDB@{shape: lin-cyl}
    RemoteLeaf[NATS<br>Leaf Node]:::internal
    RemoteLeaf2[NATS<br>Leaf Node]:::maybe_external
    Consumer([Service <br> --libbz--]):::external
    Grafana([Grafana]):::telemetry
    Grafana@{ shape: cloud}


    %% Cloudflare Edge
    %% subgraph Cloudflare_Network  [Cloudflare Edge Proxy]
        CF([https://my-domain]):::proxy
        CF@{ shape: cloud}
    %% end 
    subgraph VPS-2 [Optional VPS-2]
      PGREP[(Postgres<br>Replica <br>hot_standby = on)]:::secure
    end

    %% VPS Boundary
    subgraph VPS [Your VPS Server]
        direction TB
        
        HA([HAProxy<br>:8090]):::proxy
        %% Internal Apps
        Prom[(Prometheus<br> :9090)]:::telemetry
        NatsExp([NATS Exporter<br>:7777]):::telemetry
        Bridge[[ZeBridge-1<br> :27434]]:::bridge
        Bridge@{shape: st-rect}
        PG[(Postgres<br>Primary <br> :5432)]:::secure
        NATS[NATS Server<br> TPC :4222 <br> wss :8080]:::internal
        NATS@{shape: data-store}
        
        %% Telemetry & Monitoring Stack
        
        Sweeper[[Sweeper]]:::bridge
    end

    subgraph Consumer Service
      Consumer
      LocalDB
      RemoteLeaf
    end
    %% External Connections to Cloudflare
    PG -->|hot standby| PGREP
    User <==>RemoteLeaf2
    RemoteLeaf2 <==>CF
    RemoteLeaf <==>|wss| CF
    Consumer <==>|Connects Local| RemoteLeaf
    Consumer -->LocalDB

    %% Cloudflare to HAProxy Subdomain Routing
    CF <==> HA


    %% Internal Component Dependencies
    PGREP ==>|R|Bridge
    Bridge ==>|W| PG
    Bridge <==>|Pub Sub  <br> TCP:4222| NATS
    
    %% HAProxy Internal Layer 7 Routing
    HA -.->|:27434/enroll| Bridge
    HA <==>|wss://localhost:8080| NATS

    %% Telemetry Data Flow
    Prom -.-> |remote_writer| HA
    Prom -.->|Scrapes:7777| NatsExp
    Prom -.->|Scrapes<br>:27434/metrics| Bridge
    NatsExp -.->|Monitors| NATS
```
</details>
<br>

Performance is I/O driven. You can test on a VPS with for example 6-vCPU, 24 GB RAM and 200GB NVMe SSD, and run comfortably the following stack: 

* a master Postgres (≥16 if you want to launch a standby replica on another VPS),
* a NATS server (≥ 2.10) and his companion NATS-exporter for telemetry,
* two daemon ZeBridge, one slot for small tables 2kB-128.000 evt/s, ~ 300 MB and another slot for larger tables 128kB-4.000 evt/s, ~ 600 MB,
* a TSDB Prometheus (scraping telemetry from ZB and nats-exporter and pushing to a cloud Grafana),
* the reverse-proxy HAProxy for TLS termination of the internal ZeBridge endpoint '/enroll', and let Prometheus push to a Grafana cloud, and let NATS websockets pass-through.

## The daemon

Once the DBA has migrated the Zebridge functions into Postgres, checked for the compliance of the database, configured NATS with the needed streams and buckets, you are ready to run ZeBridge, the first pillar of the architecture, a long running background process, connected to Postgres and to NATS.

In this paragraph, we explain that this "compliance" means to follow "good pratices" of a general sync engine, as the engine makes choices usually left to you. This aims in one direction: many small consumers that read freely, write safely, and never cross the tenant line.

These good pratices act as constraints—though mostly mechanical—and that is the point: each one buys a specific guarantee.

Literature-1: <https://hatchet.run/blog/postgres-survival-guide>
Literature-2: <https://www.digitalocean.com/community/tutorials/database-normalization>


Here they are, so you can judge the fit before adopting it. 

* **Strict Memory Boundaries**: Because ZB uses a **fixed pre-allocated buffer**, its memory footprint must be defined at runtime. 
🚦 Overflows are detected, wether originated from a consumer write, or directly loaded within Postgres, or after a schema migration. They are rolled back and the table is suspended. See [Suspended tables](#suspended-tables)
* **💡 Good practices on Schemas**:  Because we are syncing databases, the schema rules are enforced, not suggested: a primary key, `uuid` keys and `timestamptz` columns on writable tables, and a deliberate choice about deletes across a foreign key.
We have added tools in Postgres to diagnose tables as `SELECT zebridge_enable(dry_run => true)`. This reports every rule before touching anything, and `bridge --diagnose` checks a whole database, the cascade rule included. See [diagnose](#diagnose).
* **Suspension**: 🚦 When a table stops meeting a rule while the bridge runs, the bridge **suspends** it and keeps everything else flowing. Fix the table and most suspensions lift by themselves; two cases need a restart, [Suspended tables](#suspended-tables) and [Restart rules](#restart-rules).
* **Soft-delete Cascade Transaction Mitigation with Sweeper**: when consumers apply _soft-deletion_ this can lead to bloated databases. Soft deletion is enforced by using a `tombstone` column in the schema.
The client library always applies a local HARD DELETE, but a _soft delete_ (via the `tombstone` timestampz) is sent to Postgres. ZeBridge solves the Postgres bloat with a companion garbage collector, the `bridge_sweeper` daemon which runs with a WRITER privilege.
The sweeper: reaps old tombstones _families_, in order, children first, in batches of `GC_BATCH_ROWS` (1000), so a million expired tombstones is a thousand small deletes and never one transaction.

* **A cascade storm, when PostgreSQL does cascade**: if a table is loaded with a schema declared with a physical cascade on a cascade-declared family, it is one transaction, and the bridge takes it as one: its events land in the ring buffer, `RING_BUFFER_COUNT` slots (32,768 by default), and are published in order as batches. A transaction larger than the ring is published as several batches, in order, and the clients apply each as one unit and hold what crosses a batch boundary until its parent arrives;
proven with a 1,500-row family against a 1,024-slot ring.
* **Ring buffer**: the ring exists to buffer possible large transactions and naturally for the broker's ordinary jitter. It is not for a NATS outage: a publish that fails is retried five times with a backoff from 100 ms doubling to 5 s, and if the broker is still gone the bridge stops rather than acknowledge WAL it never delivered, and resumes from the slot when the broker returns.
* **Attach tables to a publication**: when running a migration, the DBA must attach each table to a publication with `SELECT zb_enable;`. See [Good pratices](#good-pratices)
* **Writable is not automatic**: client-side writable tables are enabled `SELECT zebridge_enable(writable => true);` when running the database migration. See [Good pratices](#good-pratices)
* **Writes with Conflict Resolution policy-LWW**: ZeBridge makes decisions for you that other sync engines leave you to : writes are not merely accepted, but enforced with Last-Write-Win (LWW) strategy server-side _per row_.
Postgres judges a write by its version, per row: an edit stamped below the row's version is refused as `stale`. The client then looks at which columns the winner changed. If the refused edit touched none of them, it is resubmitted with a fresh stamp and lands; if both edited the same column, the edit is dropped and the loss is surfaced. So an edit is lost only on a column that was genuinely contested.
Clock skew is absorbed the same way: a slow clock is judged stale, and its edit is rebased onto the row it was made on, stamped above what the client has seen (a hybrid logical clock). The client implements a Hybrid Logical Clock (HLC) to neutralize the clock drift problem.
See [Good pratices](#good-pratices) and [Understanding the LWW rules at a glance](#understanding-the-lww-rules-at-a-glance).

* **Controlled Local Writes**: On the consumer side, we expect a standard SQLite or PGlite engine. While clients are free to read from their local database, all writes **must** route through the `libzb` library to ensure tracking. Enforcement depends upon the local engine.
* **Safety enforced by Tenant isolation**: we enforce a strict tenant model in Postgres: every principal -consumer- operates within a designated tenant boundary.
Access control - grants-  and permissions within  NATS are cryptographically secured and mapped via NATS JWT tokens tied to each tenant.
For these tasks, you can run `ZeBridge` as a CLI with a dedicated helpers  (`--init-nats`, `--gen-nkey`, `--revoke`).
This tenant structure makes sense for B2B services, less for B2C operations because clients have basically all the same rights. By dividing the database by tenants, you can use advantageously NATS leaf nodes and assign a tenant per node; the main benefit is that NATS will contains only one full copy of the database.
If you face clients, you have in pratice one tenant. Thanks to NATS leafs nodes, you can distribute geographically the load where each leaf nodes will contain a copy of the database close to the consumers.
* **Delta-chain** generation: a snapshot of a table is not on-demand nor a full table per tenant. This would crush Postgres if thousands of consumers connect. Instead, a "generation" thread produces full/deltas in a time window with a max chain length and these are dictionary based Zstd compressed and pushed into NATS. The client library cherry picks whatever its needs on connection, and complements with the few remaining CDCs up to its watermark.




### Good pratices

> [!WARNING] Every table must be attached to a publication with `zb_enable`.

#### Schemas and zebridge_enable

The schema rules below are the ones needed in terms of column types and mandatory columns.

Tables are either public or private/tenant-scoped:

| Action | column | type | note |
| --  |  --    | --   | --   |
|||||
| public table | no column, but a public reason is declared | text | ✚ `zebridge_enable(public_reason => 'this table is public')` for example|
| private table| `tenant_id` | text | ✚ `zebridge_enable(tenant_col => 'tenant_id')` |

Tables are either read-only or writable with a LWW conflict resolution policy.

| Action | column | type | note |
| --  |  --    | --   | --   |
|  Read  | id    | **bigint** or **uuid** | composite pk possible (2)|
|  Write  | id    | **uuid** (1)| composite pk possible (2)|
|||||
| Write  | updated_at | **timestamptz**  (3)| no bigserial ✚ `zebridge_enable(version => 'updated_at')` or with your prefered field identifier|
| Write  | delete_at | **timestamptz** | soft-deleted ✚ `zebridge_enable(tombstone_col => 'deleted_at')` or with your prefered field identifier |
| Write | last_writer | text | `zebridge_enable(tiebreak_col => 'last_writer')` or with your prefered field identifier|

(1) _in a writable table, a client mints its own keys offline, so a writable table's key must be **client-generable** — a `uuid`, NOT a `bigserial` that the database hands out (an edge write to a sequence key would collide with the server's next insert, so the bridge refuses it)_.

(2) _without a PK, the replica doesn't know which row to apply a mutation from the backend and we use `REPLICA IDENTITY DEFAULT` for performance_.

For example:

  ```txt
  postgres=# \d counter_tenant;
                            Table "public.counter_tenant"

    Column   |     Type   | Collation | Nullable |      Default
  -----------+------------+-----------+----------+------------------
   uid       | uuid       |           | not null | gen_random_uuid()
  ```

For example, two public tables and a tenant scoped one:

  ```txt
  postgres=# select * from zebridge_catalogue;
                          my_db

            tbl          | tenant_col |                           public_reason                           | version_col | tombstone_col | tiebreak_col |
  -----------------------+------------+-------------------------------------------------------------------+-------------+---------------+--------------+
   users                 |            | no tenant column, readable by every consumer         | updated_at  |               |
   counter_public        |            | demo — identical content for every tenant                 | updated_at  |               |
   counter_tenant        | tenant_id  |                                                                   | updated_at  |               | 
  ```
(3) See [Conflict resolution](#conflict-resolution)

#### Schemas - guards and suspension


| a table needs | why | if not |
| --- | --- | --- |
| a primary key | rows are addressed by it on every replica | ❗️ suspended: `no_primary_key` |
| `zebridge_enable(...)` | the catalogue row is the only thing that makes the bridge route it | ❗️ suspended: `no_cdc_subject` |
| a tenant column (private) or `public_reason` (public) | rows route to `cdc.<tenant>.*` or to the public stream | 🚦 refused by `zebridge_enable` |
| the tenant column inside the replica identity | a DELETE carries only the replica identity, and must still route | ❗️ suspended: `tenant_not_in_replica_identity` |
| a `timestamptz` version column (writable tables) | last-writer-wins needs one clock per row | 🚦 refused by `zebridge_enable` |
| a `timestamptz` tombstone column, or `allow_physical_deletes` | a hard delete cannot be replayed to a client that was offline | 🚦 refused by `zebridge_enable` |
| no `ON DELETE CASCADE` into a table that keeps tombstones | a cascade removes rows physically, and a physical delete on a tombstone table is never forwarded | 🚦 refused by `zebridge_enable` and by the DDL guard |
| children deleted before their parent (tombstone tables) | **a tombstone is a HARD DELETE on every Replica**, and a replica cannot delete a parent whose children are still there | the parent's tombstone is `rejected` while a live child references it |
| a `uuid` primary key (writable tables) | a client mints keys without asking the server | 🚦 refused by `zebridge_enable` |
| column types the decoder knows | an unknown binary type would be passed through as garbage | ❗️ suspended: `unsupported_column_type` |
| rows under `2^BASE_BUF` bytes, columns under `MAX_COLUMNS` | one event holds one row; both sizes are fixed at boot | ❗️ suspended: `row_too_large` / `too_many_columns` |

🔔 See [Suspended table](#suspended-tables) to see how to **revoke** a suspension.

#### Checks

Can I check if my schemas will be accepted?

✅  use `zebridge_enable(..., dry_run => true)`; this reports every rule before touching anything.

✅ On a live database, the `bridge --diagnose` tool.


🔔 The table `zebridge_catalogue` gives you the state of each table with reference to the constraints:

```txt
postgres=# select * from zebridge_catalogue;
                        my_db

tbl        | tenant_col | public_reason | version_col | tombstone_col | tiebreak_col |
-----------+------------+---------------+-------------+---------------+--------------+
test_types | tenant_id  |               | updated_at  | deleted_at    | last_writer
```

#### Scoped by tenant, authorized by grants

**Every consumer is an identity in a tenant.** A consumer connects as a _principal_ — a stable, unique name — that belongs to exactly one tenant.

Reads are scoped to that tenant by Postgres' RLS. Postgres RLS and the tenant guard decide which rows it may read and write.

NATS grants - a scoped JWT signing key - decide which subjects a principal may touch. Writes are confined to that principal by NATS subject grants. 

The two systems that already hold the data hold the rules, and the principal is a subject token the broker vouches for, never a claim in a payload.

There is _no anonymous consumer_ and no cross-tenant read, so:

> [!NOTE] Enrollment is a new row in `zebridge_user_tenants` ✚ a JWT minted under a scoped signing key.

```sql
postgres=# \d zebridge_user_tenants
       Table "public.zebridge_user_tenants"
  Column   | Type | Collation | Nullable | Default
-----------+------+-----------+----------+---------
 principal | text |           | not null |
 tenant_id | text |           | not null |

postgres=# 
INSERT INTO zebridge_user_tenants (principal, tenant_id) VALUES ('alice', 'acme');
```


#### Payload limits are not flexbile

**Payload size are limited** because NATS caps the message payload with an already generous default 1 MB, and because the bridge runs on a fixed buffer.

❗️ A row too wide for the change feed is **suspended** _at write time_, both from the edge and from `psql`.



See [Suspended tables](#suspended-tables)

#### Conflict resolution

* A writable table needs a **version column**, `updated_at`, which should be a `timestamptz` — ⚠️ never a naive `timestamp`.

🚦 The timestamp guard refuses one at `CREATE`/`ALTER`, because "newer" must be an absolute instant, otherwise last-write-wins is meaningless.
* A writable table needs a **tombstone_col** column for _SOFT-DELETE_, with a column `deleted_at` (a delete becomes a soft-delete so an offline client cannot resurrect a removed row) and an optional **tiebreak_col** column `last_writer` (resolves equal versions instead of refusing both).
➡ If there is no tombstone column, a delete becomes a HARD DELETE in Postgres.
  


* Writes are **resolved, not merely accepted — last-write-wins**: A write carries the version the client holds; the bridge applies it **only if it is newer** than what Postgres has, and rejects a stale one.
The writes use three verbs (`INSERT`, `DELETE`, `UPDATE`), resolved via **last-write-wins** (LWW).

* Furthermore, against **clock drift**: the client uses a Hybrib Logical Clock (HLC) algorithm to neutralize the clock dirft / synchronization problems and disallow silent data overwrite issues.


This is a deliberate design choice, otherwise you observe whatever results. ZeBridge arbitrates at ingest, so a slow or offline client cannot silently clobber a newer edit, and a stale queued write cannot undo a delete.
The cost is that LWW is the only resolution offered today.

#### Cascade rules

You must run the Postgres function `zebridge_enable` on each table to build th attachment  all this and needs inputs reflecting the table.

Two designs are allowed, per key:

* `ON DELETE NO ACTION` with a tombstone column, where children are deleted before their parent and the parent's tombstone is refused while a child still lives;
*  or `ON DELETE CASCADE` with no tombstone, where PostgreSQL deletes the family and every delete reaches the replicas as a CDC event.

Mixing the two on one key is **refused**, at `zebridge_enable` and at the migration that would introduce it, because a cascade's deletes never reach a replica of a tombstone table. 

💡 Cascades stay small by design: what a client may delete is a row without children.

### Diagnose

We have tools to **diagnose** the database and tables; 

Run against your already migrated database to check if it they are ready and get feedback.

```sh
bridge --diagnose
```

### Sweeper

The `bridge_sweeper` companion is run as a daemon that scans periodically PostgreSQL to prune rows marked for deletion.

It uses the `WRITER` USER privilege and its periode is `GC_THRESHOLD_MS=3600000` by default.

```sh
DATABASE_WRITER_URL=xxx bridge_sweeper
```

It also accepts an immediate action with the flag `--once`.

This is needed to keep Postgres in sync with replicas, because LOCAL deletes are HARD deletes, but they are **propagated back as soft-deletes** via an `UPDATE SET tombstone ...` by the daemon.
This operation naturally echoed back to all connected clients via the CDC event. Upon reception of  an **UPDATE with a tombstone**, the client will apply the hard delete on their replica.

The Sweeper captures this lifecycle to emit lightweight telemetry about the garbage-collected records.


## State

**The daemon is stateless**: The bridge holds no certificates, does no NATS-side lookup — everything it needs comes from Postgres (the catalogue, the rules, the tenants).

What lives in the process is a small, self-invalidating cache, nothing durable.

So a bridge is cheap to start, cheap to restart, cheap to colocate, and never itself a source of truth: in other words, ZB's state is in Postgres. All it does is mirroring PG into NATS, in and out.

On the other side, the consumer's state is its local replica plus its NATS stream position, and everything the consumer needs comes from NATS streams, buckets and object storage, managed by the client library.

## Migrations

The rules a table must meet, what each migration does to the replicas, and the one lever for what a migration cannot carry. The full per-shape table is [MIGRATIONS.md](MIGRATIONS.md).


### What a migration does

Every schema change reaches every replica live, through the descriptor the DDL trigger publishes. No client restart, no bridge restart.
One exception: too large number of columns, 

| you run | the replicas | rows |
| --- | --- | --- |
| `ADD COLUMN`, with or without a constant default | add the column, old rows take the default | kept |
| `ADD COLUMN ... DEFAULT now()` or any expression | add the column, old rows hold NULL | kept, diverged: run `zebridge_reseed('t')` |
| `RENAME COLUMN` | rename | kept |
| `DROP COLUMN` | drop | kept |
| `CREATE INDEX`, `DROP INDEX`, add or drop a foreign key | apply the same | kept |
| change a column's type | alter in place or rebuild the table | kept, then re-seeded automatically |
| change the primary key's type or columns | rebuild the table empty | replaced by a fresh full, automatically, tables referencing it included |
| `DROP TABLE` | drop the local table | gone |

Two things only PostgreSQL knows, so the replica must be re-seeded: values an expression default wrote into old rows, and rows whose identity moved. The lever is one call:

```sql
SELECT * FROM zebridge_reseed('orders');   -- bumps orders and every table referencing it
```

The DDL trigger pulls it by itself on a key change or a type change. After a key change, re-run `zebridge_enable` for the table: the old key took the replica identity with it. Every replica of the table downloads one full generation, so plan a re-key like a downtime, not like an `ALTER`.

### Examples

Two main rules:

- ❗️ Any table must have a **primary key**. This is because clients create locally tables from Postgres' schemas. Since the backend can modifiy rows (but not clients if read-only), clients can't replay the modifications if they don't have a primary key.
- ⚠️ Any table which enters a publication must be registered with `zebridge_enable()`.

**Client read-only tables**: tables can be public (everyone can read every row) or private (rows are scoped by tenant, RLS scope).

When you declare a read-only table "public", you **must** add a value to the field `public_reason`, so being public is intentional and there is no tenant column. The migration running `zebridge_enable()` that attaches the table to the publication is for example:

```sql
PERFORM * FROM public.zebridge_enable(
  'public.users'::regclass,
  public_reason => 'no tenant column, readable by every consumer', -- ❗️needed
  publication => 'my_pub', -- or env var subsitution BRIDGE_CDC_PUBLICATION
  dry_run => false
);
```

A read-only table is made "private" by adding a tenant column. The tenant_id is assigned by the DBA when registering the user and it is propagated to the client via the client library automatically on-the-fly.
You declare the field name in the 'tenant_col' of `zebridge_enable()`. The migration running `zebridge_enable()` that makes this table writable and attached to a publication is:

```sql
PERFORM * FROM public.zebridge_enable(
  'public.counter_tenant'::regclass,
  tenant_col => 'tenant_id',
  publication => 'my_pub', -- or env var subsitution BRIDGE_CDC_PUBLICATION
  dry_run => false
);
```

**Client writable tables**: The first general rule is that the table needs a **primary key** (can be composite) of type `uuid`. This is because clients mint this key so you will have ID collision.

Furthermore, a writable table needs:

- as said, a `tenant_id`() column for scoping,
- a version column (`updated_at`, or `modified_at` or `last_modified`...) of type `timestamp`**Z**,
- a tombstone column (`deleted_at` or `removed_at`) of type `timestamp`**Z** when you want soft-deletes,
- optionally, a tiebreak column, `last_writer`, of type TEXT.


```sql
CREATE TABLE IF NOT EXISTS test_types (
    uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), -- ✅ PK with UUID
    tenant_id text NOT NULL, -- ✅ RLS scope
    ...
    created_at timestamp with time zone NOT NULL, -- or inserted_at..

    modified_at timestamp with time zone NOT NULL -- ✅ version with timestampz (or `updated_at`)
    deleted_at timestamp with time zone, -- ✅ tombstone for soft-delete with timestampz
    last_writer varchar, -- ✅ tiebreak: which principal wrote
);

PERFORM * FROM zb_enable(
    'public.test_types'::regclass,
    writable => true,
    tenant_col => 'tenant_id',
    version_col => 'modified_at',
    tombstone_col => 'deleted_at',
    tiebreak_col => 'last_writer',
    publication => 'my_pub',
    dry_run => false
);
```
#### Read-only table

**A "Bad"** `read-only` table: 

The primary key (PK) is missing:

```sql
CREATE TABLE IF NOT EXISTS users (
    -- ❗️ no PK
    name varchar NOT NULL,
    email varchar,
    inserted_at timestamp NOT NULL,
);
```

The migration to add a primary key:

```diff
+ ALTER TABLE users 
+ ADD COLUMN id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY;
```

> since it is READ-ONLY, it can be a simple `bigint`, but necessarily `uuid` in the WRITABLE case.

**A "Good"** `read-only` table ✚ 🔔 the magic  PG function `zb_enable()` to attach this table to the 'publication' of your choice.

``` sql
CREATE TABLE IF NOT EXISTS users (
    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY, -- ✅
    name varchar NOT NULL,
    email varchar,
    inserted_at timestamp NOT NULL,
);

-- ❇️ the magic function that declares this table 'public' 
-- ⚠️ needs a 'public_reason' field, NOT NULL, meaning deliberate)
-- declare the publication to which this table will be added

SELECT zb_enable(
    'public.users'::regclass,
    public_reason => 'no tenant column, readable by every consumer', -- ❗️needed
    publication => 'my_pub',
    dry_run => false
);
``` 

#### Writable table

The following example has several defects. 
```sql
CREATE TABLE IF NOT EXISTS test_types (
    uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), -- ✅ PK with UUID for writable as set by client
    -- ❗️no tenant_id
    temperature double precision,
    ...
    inserted_at timestamp NOT NULL,
    modified_at timestamp  NOT NULL -- ❗️the 'version' but no time zone
     -- ❗️ no tiebreak
     -- ❗️ no tombstone
);
```

This example fails to be accepted for the following reasons: 

- it has a 'version' timestamp (`modified_at`, `updated_at`...) but the _type_ is wrong, 
- not a `timestamp`**Z**, 
- has no `tenant_id` (RLS scope), 
- has no 'tombstone' (`deleted_at`), 
- has no 'tiebreak' (`last_writer`):

The migration:

```diff
+ ALTER TABLE test_types 
+ ADD COLUMN tenant_id text NOT NULL,
+ ALTER COLUMN modifed_at TYPE timestamp with time zone USING updated_at AT TIME ZONE 'UTC';
+ ADD COLUMN deleted_at timestamp with time zone;
+ ADD COLUMN last_writer text,
```

So the "good" writable ✚ 🔔 the magic PG function `zb_enable()` where we declare which columns will play which role, and link it to the 'pulication' and declare it 'writable'.

```sql
CREATE TABLE IF NOT EXISTS test_types (
    uid uuid PRIMARY KEY DEFAULT gen_random_uuid(), -- ✅ PK with UUID for writable as set by client
    tenant_id text NOT NULL,
    temperature double precision,
    ...
    inserted_at timestamp with time zone NOT NULL,
    modified_at timestamp with time zone NOT NULL -- ✅ version
    deleted_at timestamp with time zone, -- ✅ tombstone for soft-delete
    last_writer varchar, -- ✅ tiebreak
);

-- ❇️ the magic function to map field identifers and attach the table to a publication:
-- the tenant_id
-- that it's 'writable'
-- the SYNC RULES correspondance (tombstone_col, version_col, tiebreak_col)
 -- declare the publication to which this table will be added

PERFORM * FROM zb_enable(
    'public.test_types'::regclass,
    writable => true,
    tenant_col => 'tenant_id',
    version_col => 'modifed_at',
    tombstone_col => 'deleted_at',
    tiebreak_col => 'last_writer',
    publication => 'my_pub',
    dry_run => false
);
```

A **Writable with wrong PK type**:

```sql
CREATE TABLE IF NOT EXISTS test_types (
    -- ❗️ PK without UUID for writable
    id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    ...
);

SELECT zb_enable(...);
```

* If the table is **empty**, the change of the pirmary key type from 'bigint' -> 'uuid' to simple:

```diff
+ ALTER TABLE users 
+ DROP CONSTRAINT users_pkey,
+ DROP COLUMN id,
+ ADD COLUMN uid uuid PRIMARY KEY DEFAULT gen_random_uuid();
```

* If the table is **already populated**, then we need a bit more efforts: add a new column with type 'uuid', populate it with UUIDs for every existing rows, drop the old pk column, and promote the new one as primary key, in this order.

```sql
-- 1. Add the new UUID column (without primary key or NOT NULL yet)
ALTER TABLE users ADD COLUMN new_id uuid DEFAULT gen_random_uuid();
-- 2. Populate existing rows with a UUID
UPDATE users SET new_id = gen_random_uuid();
-- 3. Make the new column NOT NULL
ALTER TABLE users ALTER COLUMN new_id SET NOT NULL;
-- 4. Drop the old primary key constraint and the old id column
ALTER TABLE users DROP CONSTRAINT users_pkey;
ALTER TABLE users DROP COLUMN id;
-- 5. Rename new_id to id
ALTER TABLE users RENAME COLUMN new_id TO id;
-- 6. Add the primary key constraint to the new id column
ALTER TABLE users ADD PRIMARY KEY (id);
```

## Suspended tables

A table that breaks a rule is **suspended**, not the bridge: its events are dropped and counted, its clients receive a suspension notice instead of a schema, and everything else keeps flowing.

`bridge_refused_tables` on `/metrics` says how many, the log says which and why, and `SELECT * FROM zebridge_suspensions` lists them.

| reason | what happened | how it lifts |
| --- | --- | --- |
| `no_primary_key` | the table has no primary key | add one; the DDL trigger lifts it live |
| `no_cdc_subject` | the table is published but has no catalogue row | `zebridge_enable(...)`; lifts live |
| `no_tenant_column` | the catalogue names a tenant column the table lacks | add the column or fix the catalogue row |
| `tenant_not_in_replica_identity` | a DELETE could not be routed to its tenant | a unique index on `(tenant, pk)` and `REPLICA IDENTITY USING INDEX` on it; `zebridge_enable` does this |
| `unsupported_column_type` | a column's type cannot be decoded | change or drop the column |
| `row_too_large` | a row exceeded the event buffer | lifts at the first write that fits after a 30 s cooldown; a restart re-measures the widest row and keeps the suspension while it still exceeds `BASE_BUF` |
| `too_many_columns` | a migration grew the table past `MAX_COLUMNS` | drop columns (lifts live), or restart: the boot re-detects |

Rows written while a table was suspended never reached any replica. A lift after such drops, live or across a restart, bumps the table's seed epoch, so every replica re-seeds from a fresh full. Nothing to do by hand.

## Restart rules

One rule covers almost everything:

> **The catalogue governs it → a migration. Data governs it → live. Neither restarts the bridge:** `zebridge_catalogue` rides the publication, so its rows reach the bridge through the WAL like DDL and it reloads its routing on the spot.

| change | what's needed |
| --- | --- |
| ✚ new table <br> (public or tenant-scoped) | ❗️ `zebridge_enable(...)` migration, <br> **no restart** — the bridge sees the catalogue row in the WAL, reloads its rules, reconciles CDC_PUBLIC's subjects, lifts the table's refusal and publishes its schema. No env edit, no stream edit by hand. |
| changed _rule_ on an existing table (version / tombstone / tiebreak / tenant column) | ❗️re-run `zebridge_enable`, <br> **no restart** — same path (the write path re-reads the catalogue on the same signal; the sweeper still re-reads on its own restart) |
| ✚ new tenant | INSERT INTO `zebridge_user_tenants` row; <br> **no restart** — create its streams,  the INSERT propagates live to `$KV.tenants`,NATS grants need a SIGHUP (reload, not restart) until the JWT signing key covers them |
| ✚ new user on an existing tenant | conf grant + SIGHUP only — with the JWT operator model, not even that |
| generations on/off, tenant growth, invites, enrollment | **nothing** — the producer and the mint read the database per tick/request |
| `DROP TABLE` | nothing for the bridge — the DDL trigger tombstones the schema and reaps the guard |
| `ADD` / `RENAME` / `DROP COLUMN`, an index, a foreign key | **nothing** — the DDL trigger publishes the new descriptor, every replica applies it live, rows kept ([MIGRATIONS.md](MIGRATIONS.md)) |
| a column filled by an expression default (`DEFAULT now()`) | `SELECT zebridge_reseed('t')`, **no restart** — the catalogue's `seed_epoch` rides the WAL; the producer builds a full, every replica re-seeds |
| primary key re-typed or re-shaped, a column's type changed | **nothing** — the DDL trigger bumps the seed epoch itself (foreign-key closure included); then re-run `zebridge_enable` for the table, since the old key took the replica identity with it |
| a migration grows a table past `MAX_COLUMNS` | the table is **suspended** (`too_many_columns`), the bridge stays up; **restart** to re-detect (or set `MAX_COLUMNS`) — the boot sees the suspension it inherited and re-seeds the table, so the rows written meanwhile reach every replica |
| | |
| change `BASE_BUF` / `RING_BUFFER_COUNT` <br> / other bridge env | **bridge restart needed** (the bridge re-registers its row-width budget and re-bakes the guards at boot). A table's descriptor rides the event buffer too, about 150 bytes per column: `2^BASE_BUF` bounds how wide a table can be described |
| change `MAX_COLUMNS` | **bridge restart needed** |

What makes this safe is that `zebridge_enable()` is the gate: its own preflight (the tombstone gate, the tenant column's existence, the width guard, the publication check) returns `preflight ERROR` rows and writes **no catalogue row** for a table that fails.
The running bridge never sees a rule it should refuse. What it does see it treats the way boot does — a table it cannot route (or that lost its row) is refused on the spot and its clients get a suspension, never a bare subject that blocks the publisher.
`zebridge_enable()` prints the bridge side as its `T3 bridge LIVE` step; `T4 nats conf` is the one step that stays outside the database.

### Checking a table / database against the bridge's rules

> **TODO**: clean picture between `zebridge_enable(dry_run => true)` complementary to `zbdoctor`, and a TODO? `psql -f diagnose.sql`

After a migration, before (or without) a bridge, ask the database itself — the same
questions the bridge's preflight asks, from the same source of truth:

```sql
SELECT * FROM zebridge_check('orders', 'writable');
SELECT * FROM zebridge_check('orders', 'writable', 'updated_at', 'deleted_at', 'last_writer', 'tenant_id');
SELECT * FROM zebridge_check_all('{"users": "read_only",
  "orders": {"mode": "writable", "version": "updated_at", "tombstone": "deleted_at",
             "tiebreak": "last_writer", "tenant": "tenant_id"},
  "audit":  {"mode": "writable", "physical_deletes": true}}')   -- accepted, so a WARNING not an ERROR
WHERE status = 'ERROR';           -- an empty result is a clean bill
```

`intent` is what you _mean_ the table to be; the check compares it with the grants (who holds INSERT+UPDATE) and with the catalogue row.
Naming the columns makes it a pre-migration check: a declared name that disagrees with the catalogue is a finding, and a table with no row yet is checked against the names you gave.

`zebridge_check_all()` treats every catalogue table you did not name as an ERROR — the ones people forget — unless you pass `partial => true`.

`scripts/zbdoctor.py --intent intent.json` runs the same map and adds the live gates (bridge, streams, KV, chains).

## Troubleshooting

Start from what you see. Each row names the check and the rule behind it.

| you see | check | usually |
| --- | --- | --- |
| a table is missing on every client | `SELECT * FROM zebridge_suspensions`; the log line naming the table | suspended: see [Suspended tables](#suspended-tables). Or never enabled: `zebridge_enable` |
| rows are missing on every client | `bridge_refused_events_dropped_total` on `/metrics`; the table's `seed_epoch` in `zebridge_catalogue` | the table was suspended for a while; the lift re-seeds. If the epoch did not move, `zebridge_reseed('t')` |
| rows are missing on one client | `bridge_fleet_client_lag_events` for that client; its own log for `held`, `predates`, `waiting for the producer's full` | it is behind, or waiting for the next full after a re-seed; both resolve by themselves |
| a client back from a pause says `chain predates` | `bridge_cdc_window_seconds` for its stream against 2 × cadence; `bridge_cdc_window_short` | the stream's window is shorter than the chain's reach: raise `CDC_MAX_AGE_SECONDS`, or a byte/message valve is ending the window first; the client re-seeds at the next generation by itself |
| old rows hold NULL in a new column, PostgreSQL does not | the column's default in PostgreSQL | an expression default; `zebridge_reseed('t')` |
| a client still shows the old shape after a migration | `nats kv get schemas <table>`; the client's log for `SCHEMA` | the descriptor did not publish (a suspension), or the client failed the ALTER and logged why |
| a write is rejected with `stale` | the client's log: `rebased` or `edit LOST` | last-writer-wins on the version; the client resends an edit whose columns the winner did not touch, and drops one on a contested column, by design |
| a write is rejected with `row_deleted` | the tombstone on the row | the row was deleted; the client's optimistic copy is reverted |
| writes never get a verdict | the client's outbox count; `nats consumer info MUTATIONS <principal>` | the client is offline, or the principal's grant does not cover `mutation.<principal>.>` |
| the client waits 90 s at connect | the client's log for `no generation chain yet` | a followed table with no chain: enable it, or remove a stale key from the `schemas` bucket |
| the bridge refuses to start | its first error line | a held slot, a missing `DATABASE_READER_URL`, a publication that does not exist; see [Restart rules](#restart-rules) |
| `bridge_wal_confirmed_lag_bytes` keeps rising | `bridge_queue_usage_percent`, `bridge_connected` | NATS is not draining, or the bridge lost PostgreSQL; see [Monitoring](#monitoring--telemetry) |
| retained WAL grows for a slot nobody reads | `bridge_replication_slot_active` | an abandoned instance: `SELECT pg_drop_replication_slot('<slot>')` |

## ZeBridge CLI

```txt
  --slot <NAME>     Replication slot (created if absent). No default —
                    required here or as BRIDGE_CDC_SLOT.
  --pub <NAME>      Postgres PUBLICATION to stream. No default —
                    required here or as BRIDGE_CDC_PUBLICATION.
  --port <PORT>     HTTP telemetry port (default: 9090)

  --gen-nkey      Mint the bridge<->NATS nkey pair (seed to stdout, once)
  --diagnose      Pre-run doctor: report everything boot would decide, write nothing
  --init-nats [dev|operator]  Generate the whole NATS stack, no nsc (--force overwrites)
  --revoke <principal>  Revoke: mapping + unused invites, three-clock narration.
                  Needs ADMIN_DATABASE_URL for the invocation (never stored in env)

  --help, -h        Show this help message
```

## The consumer side

**Two rules**: 

* you do not talk to NATS: the library does all of it. 
* you talk to the replica via the library primitives.

The client libray comes in two flavours: TypeScript (for any JavaScript engine) and a native dynamic Zig library with a C ABI `libzb` (mobile Flutter, Python/PHP/Elixir... services).

* **`zb-client-ts`** — a self-contained TypeScript package that runs **as-is** in any JS runtime: browsers, Node, Electron, Deno, Bun. No wasm and no native library needed — a JavaScript host just uses this.
* **`libzb`** — a native library with a C ABI for mobile apps, desktop apps and microservices (FFI-compatible).

💡 One big difference: The TypeScript client drives itself, the C ABI library is driven by its host.

### The TypeScript API

The consumer app uses one websocket connection ot NATS, one storage (persisted or in-memory, storage defaults to SQLite, or declared PGlite)
Developers handle their own OAuth onboarding strategy. With the credentials, the dev builds a `zb = new ZeBridge()` object and calls `zb.connect()`. If the dev needs to query the replica, he uses `zb.query(sql)`. When they want to mutate the replica, they use `mutate(table, key, values)`. You get reactivity by implementing `onChange(table, cb)` and the callback takes a granular event for smart rendering.


**Constructor**: describe your infrastructure and the user in `new ZeBridge()` and `connect()`.

```js
const zb = new ZeBridge({
  natsUrl: 'my-domain',
  engine: 'sqlite',
  durable: true,
  principal: "alice",
  creds: credsText,   // the .creds file's text (JWT + seed), from your own onboarding
})
await zb.connect();
```

Once you call `connect()`, it subscribes, receives, applies and fires your callbacks on its own: you do nothing. 

`durable` defaults to true: the replica is a stable per-principal file that survives a reload, which is what an outbox needs — a write queued while the socket was down must still be there after the page comes back. `durable: false` gives a fresh replica per load, the shape a dev loop wants and nothing else. `engine` defaults to SQLite; `'pglite'` loads PostgreSQL-in-the-browser on demand, and a SQLite consumer never downloads it.

**Query**: `query(sql)` — read your local database directly. Any SQL: joins, aggregates, offline. The replica _is_ the API.

```js
const q = "SELECT count, note, updated_at, last_writer FROM app_orders ORDER BY updated_at DESC LIMIT 3;"
const resp = await zb.query(q);
```


**Mutation**: `mutate(table, op, key, values)` — one write, three verbs (insert, update, delete), resolved last-write-wins. This is the only way to change data.

<details><summary>An example of a mutation query</summary>

The UPDATE query:

```sql
UPDATE orders
SET status = 'Expired'
WHERE id IN (
    SELECT status 
    FROM orders 
    WHERE order_date < '2023-01-01' AND status = 'Pending'
);
```

is written as:

```js
// the SELECT half runs against the local replica, works offline
const orders = await zb.query(
  `SELECT id FROM orders WHERE order_date < '2023-01-01' AND status = 'Pending'`
);

for (const row of orders) {
  await zb.mutate(
    'orders',               // table
    'UPDATE',               // op: 'INSERT' | 'UPDATE' | 'DELETE'
    { id: row.id },         // key: the PRIMARY KEY column(s) 
                            // — composite is fine: { org_id, id }
    { status: 'Expired' },  // values: ONLY the changed columns
  );
}
```
</details>
<br>


**Reactivity**: the `onChange(table, cb)` doorbell. When a change feed touches a table, it rings for granular reactivity.

* For single events (CDC): `onChange()` provides the exact `ev.data` row, allowing the developer to patch their UI arrays in memory instantly without querying the database at all.
* For bulk operations (Seeding): `onChange()` fires with `ev = undefined`, signaling to the developer that a massive operation just finished and they should execute a `SELECT *` against the local database.

A change event arrives from NATS as:

```js
{
  operation: 'INSERT' | 'UPDATE' | 'DELETE',
  data: Record<string, any>, // the row's columns as sent
}
```

💡 Two things to know when writing a handler. The same row rings twice for your own writes: once for the optimistic apply, once for the CDC echo, so an **INSERT handler must upsert**, never append blindly. And on a table that keeps tombstones, a delete arrives as an UPDATE whose tombstone column is set: treat that as the delete it is.

```js
```

You define the callback tailored to your table which mimics the mutations and action the signals accordingly to have granular rendering.
The example in _App.tsx_ uses `SolidJS` and looks like:

```js
const [counters, setCounters] = createSignal();

zb.onChange("counter_public", (ev) => {
  // seeding return ev = undefined, need a UI refresh
  if (!ev) {
    void refresh(['app_orders']); return;
  }


  const pk = ev.data.uid; // Or your composite key
  if (ev.data.uid !== counterUid('counter_public')) return;
  setCounters((prev) => ({
    ...prev,
    counter_public: {
      value: ev.data.value !== undefined ? ev.data.value : prev.counter_public?.value ?? 0,
      version: ev.data.updated_at !== undefined ? String(ev.data.updated_at) : prev.counter_public?.version ?? '',
      writer: ev.data.last_writer !== undefined ? String(ev.data.last_writer) : prev.counter_public?.writer ?? ''
    }
  }));
});
```

That is the whole contract for an app author. A callback to implement by table, with either page refresh or granular reactivity on CDCs.

### The C ABI library

`libzb` does nothing on its own. It moves only when the host calls it. Seven verbs make the card:

| verb | what it does | returns |
| --- | --- | --- |
| `zb_client_open(opts_json)` | opens the replica and the socket | a handle, `0` on failure |
| `zb_client_sync(h)` | the first step after open: resolves the tenant, applies the schemas, seeds the tables, drains the streams | `{"tenant": …, "first": bool}` |
| `zb_client_poll(h, wait_ms)` | waits up to `wait_ms` for CDC, applies what arrived, retries what was held, collects verdicts | `{"applied", "settled", "changed_tables", "seeded"}` |
| `zb_client_flush(h, wait_ms)` | sends the outbox and waits up to `wait_ms` for verdicts | `{"sent", "settled", "verdicts": {…}}` |
| `zb_client_query(h, sql, params_json)` | a read against the replica | `{"columns": […], "rows": [[…], …]}` |
| `zb_client_mutate(h, table, op, key_json, values_json)` | one write: optimistic locally, sent at once | `{"msgId": …}` |
| `zb_client_close(h)` | closes the socket and the replica | `0` |

Beside them: `zb_client_mutate_at` (a write with the caller's own version stamp), `zb_client_wipe` (the explicit wipe: close and delete the replica files), `zb_grammar_hash` and `zb_grammar_json` (what this build of the library speaks), `zb_client_live` and `zb_abi_version`.

**How data crosses.** Everything is a C string of JSON, in and out, so a binding is three declarations in any language with an FFI. Two ownership rules make it safe:

* Strings you pass in are read during the call and never kept: free your own copies as soon as the call returns.
* Every string the card returns is allocated by the library and is yours until you hand it back with `zb_free(p)`. Read it, decode it, free it — in that order, every time. A binding that forgets `zb_free` leaks one JSON document per call; the Dart, Swift and C++ bindings in `examples/05-mobile` free in the same line they decode.

A failure comes back as `{"error": "<name>"}` on the same channel, and `open` returns `0`: check both before anything else. A query the replica refuses adds `"detail"` with SQLite's own words (`no such table: test_types`: a table this client does not follow).

**Who holds the handle.** The client is single-threaded by contract: the host owns the thread, and only one thread ever calls into a handle. `zb_client_poll` BLOCKS that thread for up to `wait_ms` when nothing arrives, so the loop cannot live on a UI thread. The honest shape is one worker that owns the handle and everything that touches it — poll, flush, query, mutate, close — while the UI talks to it over messages. In Flutter that is an isolate; on iOS a background queue; in React Native a native thread behind a promise.

The Flutter example does exactly this (`examples/05-mobile/flutter/lib/src/data/zebridge_worker.dart`):

```dart
// UI side: the worker owns the handle; every call is a message with an answer.
final zb = await ZeBridgeWorker.spawn({
  "url": "nats://127.0.0.1:4222",       // plain NATS over TCP: libzb has no websocket
  "credsPath": "/path/to/alice.creds",  // the operator-mode broker takes nothing else
  "dbPath": "/a/writable/place/zb.sqlite3",
  "principal": "alice",
  "tables": ["counter_public", "counter_tenant", "app_users", "app_orders"],
  "clientId": "flutter-client",
});
print(zb.tenant);                       // resolved by the worker's first sync

// One report per poll that changed something: re-read the tables it names.
zb.reports.listen((report) {
  if (report.error != null) return showError(report.error);
  refresh([...report.changedTables, ...report.seeded]);
});

final rows = await zb.query("SELECT item, count FROM app_orders WHERE deleted_at IS NULL");
await zb.mutate('app_orders', 'UPDATE', {'user_id': uid, 'item': 'laptop'}, {'count': 2});

// App lifecycle: nothing polls while the app is in the background.
@override
void didChangeAppLifecycleState(AppLifecycleState state) {
  state == AppLifecycleState.resumed ? zb.resume() : zb.pause();
}
```

and the worker isolate is a loop over the card using `zb.sync`, `zb.poll`, `zb.flush`, `zb.close`, and the data as the key `changedTables`.

```dart
// Worker isolate: the only place the handle is ever used.
ZeBridge.init();                          // the FFI lookups are per isolate
final zb = ZeBridge(options);
final info = zb.sync();                   // tenant, schemas, seed, drain
toUi.send({'type': 'ready', 'tenant': info['tenant']});

while (!closing) {
  if (paused) { await Future.delayed(const Duration(milliseconds: 200)); continue; }
  final report = zb.poll(100);            // blocks up to 100 ms on the broker
  if (report.changedTables.isNotEmpty || report.seeded.isNotEmpty) toUi.send(report);
  zb.flush(0);                            // the outbox, every turn
  await Future.delayed(Duration.zero);    // the command port runs here: query, mutate…
}
zb.close();
```

The bound on a click is one poll's `wait_ms`: commands are served between two polls. When the app resumes, the next `poll` catches up on everything it missed and the next `flush` sends what was written meanwhile — the outbox is what makes the pause harmless. A Python service is the same loop without the isolate (`scripts/scenarios/clients.py`), and a Swift app puts it on a background queue (`examples/05-mobile/ios`).

<details><summary>or in React Native</summary>

```js
import { ZeBridge } from 'react-native-zebridge';
import { DeviceEventEmitter } from 'react-native';

const zb = new ZeBridge({ url: '...', dbPath: '...' });

// Run a continuous async loop
const startPolling = async () => {
  while (true) {
    // Await pauses the loop without freezing the UI!
    // Under the hood, C++ is running zb_client_poll on a background thread.
    const report = await zb.poll(1000);

    if (report.changedTables.length > 0) {
        // Granular UI updates
        DeviceEventEmitter.emit('zb_changes', report.changedTables);
    }
    
    if (report.seeded.length > 0) {
        // Bulk reload for specific tables
        DeviceEventEmitter.emit('zb_seeds', report.seeded);
    }
    
    if (report.settled > 0) {
        // Pending writes succeeded
        DeviceEventEmitter.emit('zb_settled', report.settled);
    }

    await zb.flush(0);
  }
};

startPolling();
```
</details>
<br>

💡 **One consequence to note when testing your client implementation**: because `libzb` sends a mutation to the outbox the moment `mutate()` returns, testing how your UI reacts to a "late arrival" (simulating a slow network) should be done by explicitly stamping the mutation into the future via `zb_client_mutate_at`, rather than trying to manually delay the flush.

### Code examples

* [App.tsx](/examples/06-web-consumer/src/App.tsx) (browser, live),
* [Flutter](/flutter) example, 
*  Node, Go, Python and Elixir microservices.

### User onboarding

**Authorization lives where the data does — no gatekeeper, no DSL.**

* NATS grants (a scoped JWT signing key) decide which subjects a principal may touch;
* PostgreSQL RLS and the tenant guard decide which rows it may read and write. 

There is no sync-rules language to author and no separate authorization service to run and keep in sync — the two systems that already hold the data hold the rules, and the principal is a subject token the broker vouches for, never a claim in a payload.
It watches the schema, seeds the local database (from the generation chain), follows the change feed, applies rows last-write-wins, and sends your writes.

It owns the local SQLite, so a write can only go through the library.



**Getting a consumer connected** is enrollment: the app authenticates to your backend (or the bridge's mint endpoint), receives a JWT credential, and connects. The principal comes back _inside_ the credential — the consumer never types it. The same model works for every consumer type.

See [Credentials & trust boundaries](#credentials--trust-boundaries).

### Understanding the LWW rules

We have two users in the same tenant. Mary went offline, and Bob is online and work on the **same row**.

|                 what happened             |                         result                          |
|-----------------------------------------------|---------------------------------------------------------|
| Mary is off, Bob edits online, then Mary deletes, then reconnects | delete wins: her stamp is newer than the row's |
| Mary is off, Bob deletes online, then Mary edits, then reconnects | delete wins: the tombstone is terminal, her newer stamp does not revive it   |
| Mary is off, deletes, then bob edits online, she reconnects  | edit wins: her delete reaches the server with a stamp older |


So the delete is terminal once it has landed, and it wins against anything older than itself.

The one case it loses is when it arrives late with an older stamp than an edit that already landed, which is last-writer-wins treating a delete like any other write. This is the only one where a deleted row comes back, with the loser told why.

|                 what happened                 |                         result                          |
|-----------------------------------------------|---------------------------------------------------------|
| two edits on different columns, any older | both land, the later-arriving one rebased if it was stamped earlier |
| two edits on the same column         | the later stamp wins, the loser logs edit LOST                   |
| an edit changing a key column        | refused before it leaves the client, KeyChange; rename is delete + create |

### Local database writes are owned

`query()` is read-only on both clients: libzb answers it on a second SQLite connection opened read-only, the TypeScript client does the same on Node and refuses by statement shape where it has one handle. The application cannot reach the outbox, the stream positions or the shape record through the API; writes go through `mutate()`.

**The library owns the write path — reads are open, writes go through `mutate()`**:  every write gets the outbox, the version stamp and the LWW echo. A write that skips the library is a _bug_ you should not be able to make by accident.

**How** that is enforced depends on the local engine, and here is how we approach it:

* **Browser SQLite (one OPFS connection)**: Enforced. the library owns the single connection and hands the app a **read-only** handle — a direct write is simply unreachable.  today.
* **Mobile and microservice SQLite**: Enforced. SQLite is the only mobile engine, and there the library does not own the connection the same way — so the lock moves into the schema: an **initial migration** makes the app-facing tables read-only (views + triggers) and routes writes through the library's own path. ➡ Enforced by the schema, not the handle.
* **PGlite:** a supported engine (`?engine=pglite` in examples/06-web-consumer; adapter at `zb-client-ts/pglite`, dialect seam in `zb-client-ts/src/dialect.ts`). The library owns PGlite's single in-memory connection exactly as it owns the OPFS one, so the same handle-level lock applies; the schema-migration path on PGlite is not yet driven.
* **Local Postgres (microservice):** the same choice as PGlite, ➡ schema-enforced.

The rule is the same everywhere; the _mechanism_ that guarantees it is engine-specific. It is why the library — not a set of naming conventions — is the API.



## Safety & Guarantees

#### Schemas Postgres -> SQLite

It is quite forgiving, because of the limited native SQLite types:

* Integers/Booleans -> INTEGER
* Floats (real, float4, float8) -> REAL
* Everything else (including NUMERIC and unknown types) safely falls back to TEXT.

#### Internal WAL decoder and refused types

The internal WAL decoder Supported Types are:

* Standard integers, floats, and booleans.
* numeric / decimal
* text, varchar, char
*  date, timestamp, timestamptz
*  uuid
* json, jsonb
* Arrays of any of the above (e.g. text[], int4[])
* Custom ENUM types: (ZeBridge safely passes these through as TEXT because Postgres natively sends enums as their text label).

**Refused Types**: If you try to replicate a table with exotic base types like hstore, xml, macaddr, geometric types (box, circle), or money, ZeBridge will **refuse** the table: see [Suspend Table](#suspended-table).

**Why?**: When PostgreSQL streams data in binary mode (pgoutput), an unknown base type arrives as raw, unformatted bytes. If ZeBridge just guessed and passed it as a UTF-8 string to your SQLite edge database,it would corrupt your data. Instead, it fails closed and refuses to replicate the table 🔔 until the column is either dropped or cast to a supported type.

#### At-Least-Once Delivery

The full mechanism — bridge ACKs Postgres only after JetStream confirms, what happens if the bridge crashes, what happens if NATS crashes — is covered in [Bridge ACK Flow and NATS outages](#bridge-ack-flow-and-nats-outages). The guarantee no data loss between Postgres and NATS, because the ACK to Postgres only happens after JetStream has durably persisted the message, and JetStream's Msg-ID deduplication absorbs any retry.

#### Zero-Consumer Protection & Storage Bounds

**What happens if PostgreSQL emits CDC events when no NATS clients are connected?** Nothing accumulates unbounded on either side. The bridge keeps ACKing PostgreSQL as normal — that only depends on JetStream, not on a consumer being present — so PostgreSQL's WAL stays bounded regardless. On the NATS side, the `CDC` stream's own retention policy (`--max-age`, `--max-bytes` — see [The NATS streams and buckets](#the-nats-streams-and-buckets)) purges old events even with zero subscribers. A client that reconnects after being offline re-seeds from the latest **generation chain** and resumes from `CDC`.

#### Idempotent Delivery

The Message ID pattern is`{lsn}-{table}-{operation}`, eg `25cb3c8-users-insert`.

**NATS JetStream deduplication:**

* Duplicate Msg-IDs are rejected
* Ensures exactly-once semantics even with retries

#### Durability

**PostgreSQL side:**

* Logical replication slot preserves WAL
* `max_slot_wal_keep_size=10GB` prevents unbounded growth

**NATS JetStream side:**

* File storage (`.storage=file`) survives restarts
* Durable consumers track position across restarts

**Consumer side:**

* Durable consumer name persists progress
* Survives consumer restarts

#### Schema Consistency

Each chain manifest carries its cutoff — `cutoff_lsn`, and `cutoff_seq`, the CDC stream sequence captured before the build — so a consumer knows exactly which events its seed already contains and discards them (the library handles this — [the consumer side](#the-consumer-side--use-the-library)).

Event ordering follows from [one WAL-reading thread per bridge](#design-overview): PostgreSQL hands the bridge a strict total order — transactions in **commit** order, and each transaction's changes in execution order — and the bridge reads it sequentially.

⚠️ **Order is preserved WITHIN a table, not ACROSS tables.** A flush is grouped by subject before publishing, which collapses interleaving: when a run begins mid-pair, a child's batch can be published before the batch carrying its parents. Most consumers never notice — rows are applied by primary key, last-write-wins, idempotent — but a consumer holding a constraint that spans tables (a foreign key) must be order-tolerant, which is why the client library `libzb` holds a row whose parent has not arrived and retries it after the next batch.

⚠️ **Do not sort by `lsn` to "restore" order.** LSNs are not monotonic in delivery order: a transaction that begins earlier but commits later arrives later carrying a _lower_ LSN (measured: `lsn=8700486880` delivered before `lsn=8700486488`). Delivery order is the truth; `lsn` is a legacy watermark, nothing more — the commit-ordered `cutoff_seq` is the gate.

### Graceful Shutdown

**Shutdown sequence:**

1. Signal handler (SIGINT/SIGTERM) sets stop flag
2. Main thread finishes processing current WAL message
3. Batch publisher drains internal queue
4. Bridge sends final ACK to PostgreSQL (last confirmed LSN)
5. All threads join cleanly

```sh
CTRL-C

systemctl stop zebridge | pkill zebridge | docker stop zebridge
```

**Guarantees:**

* No in-flight events lost
* No in-flight mutations lost
* PostgreSQL knows exact resume point
* Clean restart from last ACK'd LSN

---

## Authentication between PG, ZB, NATS and the Consumer

Four separate keys, four separate boundaries.

| boundary | credential | who holds it |
| --- | --- | --- |
| create the roles (once) | PostgreSQL **superuser** | the DBA / the init step — never the bridge at runtime |
| bridge → PG | the DBA sets two **role passwords** (`bridge_reader`, `bridge_writer`) | the bridge, in `.env.bridge` |
| bridge → NATS | the DBA generates **nkey** (NATS has the public key in `nats.conf`) | the bridge holds the seed, the NATS-server holds the public key. Plain TCP on the colocated hop, by design |
| consumer → NATS | a **JWT** (scoped signing key) + nkey seed | every consumer, from enrollment |

### Revoke a principal

Source: <https://docs.nats.io/learn/security/decentralized-auth#revoking-a-user>

One action: the DBA can revoke immediately a user's read and write access by removing the principal via the CLI:

```sh
ADMIN_DATABASE_URL=.... bridge --revoke <principal>
```

That closes writes now, and it publishes a `revoked` verdict on the principal's own channel: clients built on the libraries hang up on it at once, stay hung up on reconnect, and answer `Revoked` to every call. The rows on the device stay until the application calls `wipe()` (`zb_client_wipe` in libzb), which is explicit on purpose. A revoked principal is dead for good: an invite for that name is refused, so the operator creates a new one. That hang-up is cooperative, though: code that is not our library can keep reading with the same token until it expires. To close the token itself now, add the operator key and the server's config: the command rebuilds the account JWT's revocations map, re-signs it and splices it in place; a reload of the server (or `nats auth account push` where the server runs the full resolver) drops the principal's live sessions and refuses the token. Revocation pins the key, not the name: re-enrolling the same principal mints a new one. Proven mid-seed on both clients by `revoke_midseed` in the battery.

```sh
OPERATOR_SEED=SO... ZB_ACCOUNT_PUB=A... ADMIN_DATABASE_URL=.... bridge --revoke <principal> --conf /path/to/nats-server.conf
```

### Authenticate ZeBridge with Postgres

**DBA creates two USER profils in .env.bridge for the init scripts**:  each script _init.core.template.sql_ and _init.write.template.sql_ creates the two ZeBridge `USER` profils.

```sh
# .env.admin — the DBA sets the role names and passwords; 
# the templates interpolate them
POSTGRES_READER_USER=bridge_reader      # created by init.core.template.sql
POSTGRES_READER_PASSWORD=reader_password_changeme

POSTGRES_WRITER_USER=bridge_writer      # created by init.write.template.sql
POSTGRES_WRITER_PASSWORD=writer_password_changeme
```

### Authenticate ZeBridge with NATS

**DBA mints the nkey pair for ZeBridge ↔ NATS**: mint the nkey pair a DBA installs between NATS and the bridge by using the bridge as a CLI:

```sh
bridge --gen-nkey >> .env.bridge
```

appends to .env.bridge:

```diff
+ NATS_BRIDGE_NKEY_PUB=UDZXDNW7BZUWYV3Y3WVV2NRG5ERSXZOPNQDVMVNSSMUC4OBGXRO3UTJ4
+ NATS_BRIDGE_NKEY_SEED=SUAPVJBFH7MWPQA4SJQTSP3QGXSZMWAWDOPKUIUAUALFS66X2DBIXQGVME
```

Set the public key `NATS_BRIDGE_NKEY_PUB=UD...` in the NATS config to start the NATS server.

```json
authorization {
  users: [
    { nkey: $NATS_BRIDGE_NKEY_PUB }
  ]
}
```

After subsittution, the DBA can start the server:

```sh
export NATS_BRIDGE_NKEY_PUB=UD...
envsubst < nats-server.conf.template > nats-server.conf

nats-server -js -m 8222 -c nats-server.conf
```

### Client Authentication - Operator mode

ZeBridge **completely decouples** your application's authentication (passwords, OAuth, session cookies) from the data-sync authentication (NATS). It does not know or care how you authenticate your users.
It only handles the minting of **NATS JWTs**, which act as cryptographically secure database credentials for your edge clients.

Nats proposes an [operator mode](https://docs.nats.io/learn/security/decentralized-auth#revoking-a-user).

ZeBridge proposes this with its CLI for a dev operator:

```sh
bridge --init-nats
```

A client never gets a password to connect to NATS. It gets a **`.creds` file** (in memory or on disk), which holds two things:
* a **user JWT** — public. It says "this public key belongs to `omar`, tenant `kilo`", and it is cryptographically signed by the Account's scoped signing key.
* a **user seed** — private. The client's own key. It never leaves the device.

**Step by step:**

1. **App Authentication (Your Backend):** Your web/mobile backend authenticates the user however you prefer (passwords, biometrics, Google OAuth).
2. **The Invite (Your Backend):** Once authenticated, your backend executes a query to authorize that user's device: `INSERT INTO zebridge_invites (code, tenant_id, expires_at)`. It hands this secure random `code` down to the client.
3. **The NKey (Edge Client):** The client app locally generates a cryptographic **NKey pair** (a public key and a private seed). *The private seed never leaves the device.*
4. **The Handshake (Edge Client to ZeBridge):** The client makes an HTTP request to the bridge's endpoint, providing the invite code and its *public* key:
   ```txt
   GET /enroll?code=<invite>&user_pubkey=U...
   ```
5. **The Minting (ZeBridge):** 
   * ZeBridge redeems the invite in PostgreSQL (stamps `used_at`) and permanently maps the user identity: `INSERT INTO zebridge_user_tenants (principal, tenant_id)`.
   * ZeBridge then acts as a **Delegated Signer**. Because you provided it with a NATS Scoped Signing Key via the `ZB_SIGNING_SEED` environment variable, it mints a NATS 2.0 JWT embedding the client's public key, restricts their subjects to their specific `tenant_id`, signs it, and returns `{"jwt":"..."}` to the client.
6. **The Credential Assembly (Edge Client):** The client app takes the JWT it received from the bridge and combines it with the private seed it already generated in step 3 to create the standard `.creds` file format. (Browser: memory/sessionStorage. Mobile: secure keychain).
7. **Connection to NATS:** The client connects to NATS presenting this `.creds` format. The NATS server sends a cryptographic challenge (a nonce). The client signs the nonce with its private seed. The NATS server verifies the signature, verifies the JWT was officially signed by the `ZB_SIGNING_SEED`, and grants access. **No secret ever crosses the wire.**
8. **Expiration:** Minted JWTs live 24 h (`enroll_jwt_ttl_seconds`). After that, the client quietly asks your backend for a new invite code and enrolls again.

The consumer boundary is one model for **every** consumer type — webapp, mobile, or microservice. The JWT and its verification are identical everywhere; only the transport (WebSocket for the browser, TLS-TCP for native) and the credential storage differ.

**Who holds what:**

| who | holds | can |
| --- | --- | --- |
| the client | its own seed + its JWT | be itself |
| ZeBridge | the account scoped signing seed (`ZB_SIGNING_SEED`) | mint client JWTs for others |
| the NATS server | the operator JWT + account public key (`ZB_ACCOUNT_PUB`) | trust what the bridge signed |

**Permissions are not in the JWT.** They come from the signing key's role template, which expands `{{name()}}` and `{{tag(tenant)}}` at connect time. That is why the JWT carries `tenant:kilo` as a tag, and why **onboarding a tenant needs no NATS config change**.

**⚠️ In local dev it is simpler — there is no enrollment.** `scripts/native/jwt-bootstrap.sh` pre-mints the fixed principals with `nsc` and writes their creds to disk:

```sh
scripts/native/creds/{alice,bob,mary,nina,omar,bridge,zbdoctor}.creds
```

Anything that reads `NATS_CREDS` just points at one:

```sh
NATS_CREDS=scripts/native/creds/zbdoctor.creds python3 scripts/zbdoctor.py
NATS_CREDS=scripts/native/creds/omar.creds     python3 scripts/scenarios/mutate.py
```

Same dance at connect time (step 6 is identical) — only steps 1–4 are replaced by "someone minted it for you ahead of time".

⚠️ **The `/enroll` endpoint is off unless all three are present**: `ZB_SIGNING_SEED` (the account signing seed — the mint authority), `ZB_ACCOUNT_PUB` (the account public key, which goes into every JWT it issues), and `DATABASE_WRITER_URL` (redeeming an invite is a write). Miss one and the bridge starts normally and answers `{"error":"enrollment not configured"}` — check the boot log for `🎟️ enrollment endpoint armed`.

---

## Architecture & Internals

>[!IMPORTANT] One bridge, one slot, one port

### Data flow overview

**One instance = one slot = sequential.** Once the publication is created, a bridge runs one replication slot and processes the WAL in order.

With Postgres replication set to 'logical', we use a log-based Change Data Capture (CDC) with the native  `pgoutput` (v1) logical decoding plugin to stream WAL changes in _binary_ format.

We use `REPLICA IDENTITY DEFAULT` to limit the volume, thus the speed of the emitted data by `pgoutput`. The price is, on every table, a _primary key_.

**The message format**:

* CDC events and chain rows travel as MessagePack — compact, type-safe, fast (it keeps the int/float/binary distinctions JSON loses).
* Schemas travel as JSON in two shapes (PostgreSQL and SQLite), so a client builds its local tables in either. Every write carries a message id, for idempotent at-least-once delivery.
* Snapshots (deltas) travel as Zstd compressed chunks to the consumer.

#### The main loop PG/ZB/NATS

1. **Read the WAL.** One thread follows PostgreSQL's logical replication stream (`pgoutput`), in order.
2. **Decode into a fixed buffer.** Each change is decoded into a pre-allocated slot — memory is bounded at startup, not grown per event.
3. **Batch to NATS.** Decoded events are published to JetStream in batches of 5000 events or 500 ms or 256 KB
4. **Acknowledge.** Only after JetStream confirms does the bridge ACK that position to Postgres (no data loss).
5. **Reclaim**: Postgres can then reclaim WAL. If the bridge crashes, Postgres keeps the unpublished WAL — nothing is lost.

```txt
PostgreSQL WAL → Bridge → NATS JetStream
              ↑            ↓
              └─── ACK after JetStream confirms
```

Egress (Postgres → NATS) is a **push**: the bridge publishes as changes happen.

**Backpressure**: NATS slow/full → Bridge can't get JetStream ACK → Bridge stops ACK'ing PostgreSQL → WAL accumulates.

#### The NATS/Consumer loop

Bootstrap and ingress (consumer ↔ NATS) are **pull**: the consumer pulls CDC and chain objects at its own pace and pushes its writes on its own subject. The consumer controls replay.

The NATS ACK flow remains outside of the bridge scope:

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

#### Seeding & Edge Optimization (Generations)

ZeBridge does not hold massive WAL logs or long CDC queues for disconnected clients. It takes a radically different approach optimized for storage, edge bandwidth, and database load: **Short CDC, Long Deltas**.

**The Flow:**
1. **Short CDC Stream:** The live JetStream `cdc.*` queue is kept intentionally short (e.g., retaining only the last 15 minutes of events). This prevents NATS from bloating with millions of single-row historical events.
2. **Generation Chains (The Fallback):** The bridge periodically captures bulk snapshots (**Fulls**) and incremental changes (**Deltas**) per tenant/table, and stores them directly in the NATS Object Store. A tiny JSON Manifest in NATS KV tracks this rolling window.
3. **Catch-Up:** When a client goes offline and misses the CDC window, it doesn't do a full wipe on reconnection. The client reads the Manifest, discovers the missing Deltas, and cherry-picks only what it needs. It bulk-upserts these Deltas into local SQLite (vastly outperforming single-row replays) and gracefully resumes tailing the live CDC stream. The manifest's `cutoff_seq` acts as the precise splice point.

**The main benefits**:
* **No connection storm on Postgres.** An on-demand dump is served _per request_, queued against the database. A generation is built once by a background job and pushed to NATS object storage. A fleet of 10,000 edge clients reconnecting simultaneously hits Object Storage (which fans out cheaply), completely shielding PostgreSQL.
* **One copy, partitioned.** The chain slices the database by tenant and table, so NATS holds a single partitioned copy of the database, not a fresh full dump per consumer request.
* **High Edge Compression (Zstd Dictionaries):** To optimize edge bandwidth, ZeBridge trains a **Zstd Dictionary** on every Full generation capped at 8 MB. It then uses this specific dictionary to compress subsequent Deltas in that era. This allows tiny 50-row JSON Deltas to compress at high ratios (often saving 75%+ bandwidth on mobile networks), saving battery and data for your edge users.


### Memory Management

The ring buffer is pre-allocated once at startup, in three parts: a fixed-size event slab, a data slab for row bytes, and a columns slab for column descriptors. Decoding a WAL message writes column values directly into that pre-allocated space — there is no per-column heap allocation on the hot path, and nothing to free afterward. The SPSC queue between the two threads carries only slot indices, not owned data; a slot is returned to the free pool once its batch is published, and the next event reuses the same memory.

**Arena allocator** (a separate, smaller one, for the encode/publish step):

* Reused once per flush, reset (not freed) between flushes.
* Backs the MessagePack value trees built for one batch.
* Avoids one malloc per column per event.

### Replication Slot Management

**On startup:**

1. Bridge creates replication slot (if not exists)
2. Gets current LSN to skip historical data
3. Starts streaming from current LSN

**During operation:**

* Bridge sends status updates every 100ms or 1MB of data, whichever comes first
* PostgreSQL prunes WAL up to last ACK'd LSN

**On shutdown:**

* Bridge sends final ACK with last confirmed LSN
* Postgres' replication slot preserves position for restart

There is **no automatic slot cleanup** because an instance can be stopped/restart on-the-fly, as a normal process.

> [!WARNING] If you stop definitely an instance (with say `--slot my_slot`), do not forget to discard the slot, otherwise Postgres will not recycle the WAL, worse, it will grow indefinitely from the last position.
 
The DBA can run:

```sql
# psql>
SELECT pg_drop_replication_slot('my_slot');
```

### Reconnection Handling

**PostgreSQL reconnection:**

* Connection lost → Bridge waits 5 seconds
* Gets latest LSN
* Reconnects and resumes streaming
* Metrics track reconnection count

**NATS reconnection:**

* Automatic (handled by `nats.zig`)
* Max attempts: -1 (infinite)
* Wait between attempts: 2 seconds
* Flush timeout: 10 seconds

---

## Setup & Deployment

ZeBridge runs in three contexts:

* **the test suite** runs Postgres and NATS natively on the host — fast to iterate, and driven by the test scenarios. For development only.
* **the docker compose evaluation** — trying ZeBridge out — is best as a `docker compose` stack: all the infrstructure in one code file, Postgres, NATS, NATS-exporter, Prometheus, Grafana, bridge_sweeper, ZeBridge and the reverse proxy HAProxy.
* **Production** is your own topology, and here compose is not a recommendation to avoid any overhead. Postgres may be a managed or remote instance and the system will inevitably suffer from latency. NATS may be remote too, although for performance, the bridge should sit to the `nats-server` and communicate by _plain text_, over TCP.

For example, when all three run on host, during a spike (800 k writes/s), CPU usage is largely dominated by Postgres with around 70% of the host CPU, delivering 300 kCDC/s whilst ZeBridge uses ~20-25% and NATS ~5-10%.

 So what actually matters is **colocation**: the bridge should sit next to `nats-server` — their hop is plain TCP for speed, so it must not cross a network.

The strong setup puts **Postgres + zebridge + nats-server + Prometheus scrapper + nats-exporter + bridge_sweeper + HAProxy** together behind one boundary, one domain. Consumers connect directly to NATS via WSS and the reverse proxy fronts Prometheus (for a cloud Grafana) and ZeBridge's HTTP surface over TLS — for the consumer **JWT enrollment dance** (`/enroll`) — with a domain and whatever auth you put in front.

The bridge holds no certificates of its own, and HAProxy should terminate the SSL (or sligntly less secure, the DNS Cloudflare).


### Docker compose setup

You have the files [grammar.json, .env.docker, Dockerfile, docker-compose.full.yml], at the folders _/telemetry_ and _/proxy_ aside.

You must be a bit patient the first time you run:

```sh
docker compose -f docker-compose.full.yml --env-file .env.docker up -d
```

> The NATS setup is a bit acrobatic. The image "nats-box" already contains `jq` (to substitute _grammar.json_).
> Postgres is setup with `wal_level=logical`, and then we run a on-shot PG server initialization (it does not contain ``gettext-base` for `envsubst` so the image brings it in).

You now have access to a Grafana dashboard at <http://localhost:3000> (admin|admin) and query the bridge at <http://localhost:8090/status>, and <http://localhost:8090/metrics>.

It remains to run a client. `App.tsx` is the best candidate, via a `pnpm dev --port 5173`.

### Host setup

You have `zstd`, `libpq` available on your host.

We suppose the DBA credentials are:

  ```sh
  export ADMIN_DATABASE_URL=postgres://admin:s3cret@l127.0.0.1:5432/my_db

  #or, in ~/.pgpass, 127.0.0.1:5432:my_db:admin:s3cret, and
  ```

You have a copy the files _.env.bridge, .env.admin, grammar.json_  next to the ZeBridge binary, and have compiled the Zig binary: `zig build -Doptimize=ReleaseFast`.
For ease, you have _./zig-tou/bin/zebridge_ in your path.

You have generated the keychain for NATS and ZeBridge:

  ```sh
  zebridge --gen-nkey >> .env.bridge
  ```

Next, Postgres is readu and you have enabled PG Logical Replication.

<details><summary>postgresql.conf</summary>

  ```sh
  wal_level = logical
  max_replication_slots = 10 #<- also the max number of ZeBridge instances
  max_wal_senders = 10
  max_slot_wal_keep_size = 10GB
  wal_sender_timeout = 300s  # 5 minutes
  ```
</details>
<br>

The SQL and config templates carry `${VAR}` placeholders, so they are rendered with `envsubst` from the admin environment first.

```sh
envsubst < init.core.template.sql  | psql -d "$ADMIN_DATABASE_URL" -v ON_ERROR_STOP=1 -f -
envsubst < init.write.template.sql | psql -d "$ADMIN_DATABASE_URL" -v ON_ERROR_STOP=1 -f -
# read/write only
# The templates create no publication. Make it by name — this is the only supported
# way, because it also attaches the three tables every bridge needs in its own
# publication (zebridge_ddl_events, zebridge_gc_watermark, zebridge_user_tenants)

unset ADMIN_DATABASE_URL
```

* **Read-only setup** — run `init.core.sql`. It creates the reader role, the publication, the catalogue, the schema/DDL triggers and the read-side guards. The bridge now streams changes and consumers read them; nothing can be written from the edge. This is the base, and for many uses it is the whole thing.

A read-only deployment never touches `init.write.sql`; a read/write one runs both, in order.

* **Read/write setup** — also run `init.write.sql`. It adds the writer role, the per-table write guards (version stamping, soft-delete, tenant guard), RLS scoping and the enrollment table. Now a consumer can push writes, resolved last-write-wins.

* **create the publication**, by name. The templates create none — the name used to be substituted into them, which made it a second spelling of the bridge's own `--pub` with nothing checking that the two agreed:

```sh
psql "$ADMIN_URL" -c "SELECT * FROM zebridge_create_publication('my_pub')"
```

⚠️ Do not hand-write `CREATE PUBLICATION`. Three tables have to be in **every** publication a bridge attaches to — `zebridge_ddl_events` (schema changes), `zebridge_gc_watermark` (the offline window), `zebridge_user_tenants` (tenant resolution) — and a publication missing them still boots a bridge, still carries user rows, and still passes every health check.

➡ `zebridge_create_publication` attaches them; nothing else will.

**Declare your tables** — the important step. Run your own migrations to create the application tables, and call `zebridge_enable(...)` for each table you want replicated. One call writes the `zebridge_catalogue` row, installs the guards, scopes RLS and adds the table to the publication, atomically:

```sql
SELECT zebridge_enable('public.notes', 
  writable => true,
  version_col => 'updated_at',
  tenant_col => 'tenant_id',
  -- named, never defaulted: this argument decides which feed carries the table.
  -- Add create_publication => true to make the publication in the same call.
  publication => 'my_pub',
  dry_run => false
);
```

* **Database diagnose**: the conformance of the database with regards to the targets (public/private with tenants, read-only or writable, user-tenants enrollment), once the two previous steps are up.

**~~Place the wire grammar.~~** ~~Copy grammar.json where the bridge and the NATS setup can read it, it holds the static names both sides share.~~

**Configure and start NATS with JetStream.** Render the server config from its template, then start it:

```sh
envsubst < nats-server.conf.template > nats-server.conf
nats-server -js -c nats-server.conf
```

**Operator/JWT** world (consumers): run `scripts/native/jwt-bootstrap.sh` once to build the operator, accounts and scoped signing keys, and start NATS with the operator config it emits instead.

**Configure the bridge.** Fill in `.env.bridge`: the connection strings (`DATABASE_READER_URL`, `DATABASE_WRITER_URL`, `NATS_URL`), the credential (`NATS_CREDS` for JWT, or `NATS_BRIDGE_NKEY_SEED`), and `BASE_BUF` if the default is too small for your widest table.

**Start the bridge.**

```sh
set -a && source .env.bridge && set +a
./bridge --slot my_slot --pub my_pub --port 27434
```

At boot it reads the catalogue, reconciles the CDC streams, publishes every table's schema, and begins streaming.

#### Verify the wiring

— `python3 scripts/zbdoctor.py`. One command, one verdict (exit 0 green / 1 red, `--json` for CI). It checks the bridge's own health (`/health`, `/status`), the PostgreSQL posture (the `zebridge_audit_*()` functions, read _through_ the catalogue so a declared-public table is not flagged for being unscoped), the NATS topology (CDC streams, KV buckets), and the property that actually matters after boot: that a **fresh client** can resolve its tenant, read every schema, and seed from a chain whose objects are really there. Its last gate delegates to [`scripts/scenarios/check.py`](scripts/scenarios/check.py) for declared-vs-actual drift. Stdlib-only: it runs wherever `psql`, `nats` and python3 do.

??

```sh
# "how is my database conforming?"
PGPASSWORD=s3cret psql -h localhost -U admin -d my_db -p 5432 \
  -v ON_ERROR=1 \
  -v schemas=... \
  -1 \
  -f diagnose.sql
```

**"What would enabling the table 'users' in the publication 'my_pub' do?"**:

  ```sh
  PGPASSWORD=s3cret psql -h localhost -p 5432 -U admin -d my_db -c \
    "SELECT zebridge_enable('public.users'::regclass, publication => 'my_pub', dry_run => true);"
  ```

  <details><summary>Result of the query</summary>  

  | step | status | detail |
  | -- | -- | -- |
  | width guard | would | zebridge_install_width_guard('users') — a row the change feed cannot carry is refused at write time: edge writes get a rejected verdict (SQLSTATE 23514), psql gets an ordinary ERROR. No-op on tables without unbounded columns. |
  | catalogue | would | zebridge_catalogue[users]: tenant_col=NULL(public) version_col=updated_at tombstone=- tiebreak=- generations=t — the bridge reads this at boot (env rules become overrides) and the generation producer per tick |
  | publication | already | my_pub already carries users |
  | T3 bridge | LIVE | nothing to do — the catalogue row this wrote reaches a running bridge through the WAL; it reconciles CDC_PUBLIC's subject filter, lifts the table's refusal and publishes its schema on the spot (NOTES §10bj). A bridge started later reads the same row at boot. |
  | T4 nats conf | MANUAL | grant subscribe on cdc.users.> — and init.snap.users.> to match, because a client must not be able to dump what it cannot subscribe to |
  | summary | DRY RUN | nothing was applied — re-run with dry_run => false |

  </details>
  <br>

  **Is the table 'orders' "writable?**:

  ```sh
  PGPASSWORD=changeme psql -h localhost -p 5432 -U postgres -d postgres -c \
  "SELECT * FROM zebridge_check('orders', 'writable');"
  ```

  <details><summary>Result of the check of the table 'orders' as 'writable'</summary>

  | check_name | status | detail |
  | -- | -- | -- |
  | catalogue | ok | orders: public (FK-ordering demo: child of users, PROTOCOL.md 4); version_col=updated_at tombstone_col=∅ tiebreak_col=∅ generations=t |
  | writers | ERROR | declared writable, but no login role holds INSERT+UPDATE on it: every edge write will be refused. zebridge_enable(..., writable => true) grants the writer role |
  | primary key | ok | (uid) |
  | replica identity | ok | DEFAULT (the primary key) |
  | publication | ok | published by {my_pub} |
  | version column | ok | updated_at timestamptz NOT NULL |
  | tombstone | ERROR | writable without a tombstone_col: DELETEs are physical, inexpressible in a generation delta, and hard-deleted rows resurrect on every fresh seed. Add deleted_at timestamptz and declare it (or accept it explicitly: allow_physical_deletes => true, and say so in the intent) |
  | tiebreak | NOTE | none: two writes with the same version are refused rather than resolved |
  | width guard | NOTE | bounded (widest possible row 0 ≤ 4096): statically safe, no trigger needed |
  | row width | ok | widest possible row 0 bytes within the 4096 budget |
  | generations | ok | a chain exists: fresh clients seed from it |
  | summary | ERROR | orders: 2 error(s), 0 warning(s) against intent writable |

  </details>
  <br>

> The `zebridge_enable(dry_run => false)` is the function that connects a table to the publication when the various checkups are all green.

### NATS streams and buckets

ZeBridge authenticates to the NATS server using nkeys.

ZeBridge uses two stream families and several KV buckets (schemas, tenants, generations) plus per-tenant object stores for the bidirectional flow ZeBridge ↔ NATS ↔ consumer.
The naming is **shared** and declared in [grammar.json](grammar.json).

A ZeBridge instance is started with one config. The DBA starts the NATS server with its own config. `grammar.json` is the static wire grammar shared between the two: stream names, subject prefixes, and KV bucket names, declared once (`streams`, `subjects`, `kv`, `cdc_streams`, `open_tenant`, `generations`). Which tables replicate, and how, lives in the database: one `zebridge_catalogue` row per table, written by `zebridge_enable(...)`. `nats-init` creates only the MUTATIONS stream and the KV buckets; the bridge creates and reconciles the CDC stream family itself at boot, from the catalogue. Both sides authenticate with nkeys.

**Three data flows**:

1. **Bootstrap** (READ): the consumer takes each table's _schema_ (from the `schemas` KV) and seeds it from the **generation chain** (objects + a manifest in the `generations` KV): PG → ZeBridge → NATS/JS.
2. **Real-time CDC** (CDC stream, READ): the consumer receives INSERT/UPDATE/DELETE events as they happen: PG → ZeBridge → NATS/JS.
3. **Real-time ingress** (MUTATIONS stream, WRITE): the consumer updates its local storage and sends the intended change to NATS/JS → ZeBridge → PostgreSQL.

| Stream | Purpose | Retention (default) | Consumer Pattern | Role |
| -------- | ------------------- | ------------- | ----------------------- | -- |
| **CDC** | Real-time egress changes | 2 x GENERATION_CADENCE ~10min | Continuous subscription | READ |
| **MUTATIONS** | Real-time ingress changes | 7 days <br>max client no-show time | Continuous subscription | WRITE |

Besides the streams, the bridge maintains the seeding buckets: the **`generations` KV** holds one chain manifest per `<tenant>.<table>`, and a per-tenant **`gen-<tenant>` object store** holds the full and delta objects the manifest points to. The producer provisions the object stores at runtime, the same way the bridge provisions per-tenant CDC streams.

The CDC stream family is owned by the bridge: at boot it creates any missing `CDC_<TENANT>` stream with file storage, limits retention, s2 compression, an 8-day max-age and a 1 GB max-bytes cap — deliberately modest, because JetStream `max_bytes` is a reservation against the server's storage budget. It also sets `CDC_PUBLIC`'s subjects authoritatively to `cdc.<tbl>.>` for every catalogue-public table plus `cdc.<open_tenant>.>`. MUTATIONS keeps the limits `nats-init` gives it.

⚠️ **Retention is a correctness parameter.** A client offline past CDC's window re-seeds from the latest generation chain, so the chain — not the stream — bounds how long a consumer may be away.

Two couplings matter: the product `GENERATION_CHAIN_DEPTH × GENERATION_CADENCE_SECONDS` must stay under the sweeper's `GC_THRESHOLD_MS` (a tombstone must never be reaped before the delta that ships it — the bridge states the number at boot), and the CDC streams' age (`CDC_MAX_AGE_SECONDS`, default three cadences) must exceed two cadences, so a fresh chain always overlaps the stream it splices into (the manifest's `cutoff_seq` is the splice point). The bridge checks it at boot; the fleet monitor watches the window each stream really holds (`bridge_cdc_window_seconds`, `bridge_cdc_window_short`), because a size valve (`CDC_MAX_BYTES`, `CDC_MAX_MSGS`) can end the window before the age does.

Consumers use these streams to interact with NATS; the exact names are declared in `grammar.json`.

### The memory setting

The fixed memory used by ZeBridge has three dimensions: the slot size, `BASE_BUF` and the number of slots, `RING_BUFFER_COUNT`, and `MAX_COLUMNS` (auto-detected).

NATS messages default to `max_payload=1M`, which is already quite large. It can be safely extend fo 8MB.

Depending on the size of the published tables you wish to track, the maximum row size, the maximum number of rows per transaction, and the CDC emission rate you want to buffer during possible NATS reconnections (e.g. buffer 100 to 50,000 evt/s during 1s)

**Row width**: ZeBridge will suspend a table whose rows are wider than the NATS message limit (<1 MB). The NATS cap also means a consumer cannot push a large row to NATS. As explained, above this limit, we are in the domain of Object storage for large blobs, and URLs should be saved in the database instead.

**CDC**: The `RING_BUFFER_COUNT` is designed to buffer the received events during potential NATS jitters or outages. Its count depends naturally upon the emitting rate.
The `BASE_BUF` is the max payload size, capped at 1MB.
The `MAX_COLUMNS` is the maximum number of possible columns per table. Unset (the normal case), it is **auto-detected at boot** from the widest table in the publication, rounded up for migration headroom — not a fixed compile-time guess. Set it explicitly only to override that.

➡ It caps the event size, suspends a table and drives the total memory used.

Ceiling is NATS/JS, the host capactiy, not ZeBridge.

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
* NATS has the MUTATIONS stream and the KV buckets (the bridge creates the CDC streams itself at boot).


**Principal authentication** (the end user of a consumer app):

The principal is authenticated by the consumer app, and carried through NATS's JWT/operator model so the bridge can pass it to Postgres for RLS policies.

|  |  subscribe  |   publish  | needs an account |
|--|--|--|--|
| read-only consumer  | cdc.>, KV.schemas.>, KV.generations.>, the gen-`<tenant>` objects | —       | no |
| read-write consumer | the same | + mutation.`<principal>`.> | yes |

```txt
# user public key:
NATS_BRIDGE_NKEY_PUB=UDXU4RCSJNZOIQHZNWXHXORDPRTGNJAHAHFRGZNEEJCPQTT2M7NLCNF4
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

### Running the Bridge

A bridge instance is one slot, one port. You declare which slot and which publication to use, and refuses to start without both.

The slot is created by the bridge if it does not exist; the publication is not — ❗️ the publication must already exist, and the bridge stops at boot if it does not (see `zebridge_enable`).

⚠️ If you stop definitely an instance, purge the slot! [Replication Slot Management](#replication-slot-management).


The bridge accepts runtime env var configuration:

* a memory budget: `BASE_BUF` (default 2^12 = 4 KB) and `RING_BUFFER_COUNT` (default 32_768), sized to the tables this instance handles — see [Sizing BASE_BUF and RING_BUFFER_COUNT](#sizing-base_buf-and-ring_buffer_count). `MAX_COLUMNS` is usually left unset and auto-detected.
* a unique `--slot` — the WAL pointer PostgreSQL keeps for this instance. Each running instance needs its own.
* a unique `--port` for its telemetry webserver. Each running instance needs its own.
* the mandatory `NATS_BRIDGE_NKEY_SEED` env var — the private half of the public nkey the NATS server was given.
* `DATABASE_READER_URL`, `DATABASE_WRITER_URL`, `NATS_URL` — the connection strings.

For example, one instance on the publication `my_pub` (created by the DBA) with the slot `my_slot` (with `bridge` in the PATH):

```sh
BASE_BUF=10 \
RING_BUFFER_COUNT=4096 \
NATS_URL=nats://127.0.0.1:4222 \  # default value
BRIDGE_PORT=9090 \                # default port
NATS_BRIDGE_NKEY_SEED=SU... \            # mandatory
DATABASE_READER_URL=postgres://bridge_reader:bridge_password_changeme@127.0.0.1:55432/postgres \
DATABASE_WRITER_URL=postgres://bridge_writer:writer_password_changeme@127.0.0.1:55432/postgres \
bridge --slot my_slot --pub my_pub --top grammar.json
```

The flags win over the environment, so `.env.bridge` can carry the usual pair and a one-off run can still point at another publication.

A [TODO]: details...

---

## Configuration

All configuration constants are centralized in `src/config.zig` and `grammar.json`. Per-table replication rules (tenant column, LWW columns, tombstone) live in `zebridge_catalogue`.

### Key Settings


**Generation configuration:**

* Cadence: `GENERATION_CADENCE_SECONDS` (chain depth × cadence must stay under the sweeper's `GC_THRESHOLD_MS`)
* CDC retention: `CDC_MAX_AGE_SECONDS` (default 3 × cadence, must exceed 2 ×), `CDC_MAX_BYTES` and `CDC_MAX_MSGS` (disk valves; `bridge_cdc_window_short` says when one ends the window before the age does). Applied to existing streams at boot.

Changing the cadence changes the CDC retention with it: the age defaults to three cadences and follows `GENERATION_CADENCE_SECONDS` unless you set it yourself, in which case keep it above two cadences. That is the contract a returning client relies on, so it is checked twice: `bridge --diagnose` reports a violation as a finding before anything runs, and boot warns. Read the doctor rather than wait for the warning to pass by in the log under load.
* Manifests: `generations` KV, keyed `{tenant}.{table}`
* Objects: per-tenant `gen-{tenant}` object stores

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

**Fleet observability**:  `FLEET_POLL_SECONDS`, `FLEET_TTL_SECONDS`, and `SLOT_INVENTORY_SECONDS` and a  per-client `heartbeatMs` option.

See `src/config.zig` for all tunables.

### ⚠️  Sizing the ring

These values are not independent, and getting them wrong has a visible consequence.

⚠️ In particular, **`BASE_BUF` is a one-way door**: lowering it after rows have already been processed, meaning **already stored**, thus can be used freely by consumers, means the next write that touches such a row can 🚦 suspend the table, impacting every client.

❗️ preflight check this at boot and refuses to let it pass silently (PROTOCOL.md §9).

The bridge pre-allocates the ring at startup, in **three parts**:

```txt
ring = ( 2^BASE_BUF  +  sizeof(CDCEvent)  +  MAX_COLUMNS × sizeof(ColumnView) )  ×  RING_BUFFER_COUNT
         ^ data:          ^ metadata:          ^ columns:                          ^ number of events
           max bytes        fixed, 328 B         8 B × MAX_COLUMNS — resolved        buffered ahead
           for ONE row      per event            at boot, not a compile constant     of NATS
```

`sizeof(CDCEvent)` is small and fixed regardless of table shape — `columns` is a _slice_ into a separate slab, not an inline array, so this term no longer grows with the widest table you might ever replicate.

Each knob answers a different question:

* **`BASE_BUF`** (log2 bytes, range 10–20) is _how large a single row may be_. Size it to your widest row: a `jsonb` document, a long `text` column, a big array.
* **`RING_BUFFER_COUNT`** (range 1024–1048576, **clamped** to the nearest bound if you go outside it — an out-of-range value used to fall back to the _default_, so asking for 64 got you 65536) is _how many events can queue while NATS is unreachable_. 65536 slots ≈ 1 second at 60K events/s. It is also useful for long transactions, like an `ON DELETE CASCADE`. Below that, a NATS blip starts back-pressuring the WAL reader sooner.
* **`MAX_COLUMNS`** is _how many columns one event may carry_ — normally left to auto-detection; override it only to widen the ceiling ahead of a migration or to pin the value across instances. It is resolved **per instance, at boot**:

  * Unset (the default): **auto-detected** from the widest table actually in the publication, rounded up to the next multiple of 8 for migration headroom (a table with 6 columns → `MAX_COLUMNS=8`; see the "MAX_COLUMNS=…" line at boot).
  * `MAX_COLUMNS=<N>`: an explicit override, clamped to 8–1600, that skips auto-detection — set it if you replicate a genuinely wide table, or want to fix the value across instances rather than let each one detect its own.

🚦 A table past the resolved `MAX_COLUMNS` is refused with `TooManyColumns` — loudly, with its events dropped and its clients told why — rather than truncated.

🔔 Changing theses values needs a **restart**. See [Restart Rules](#restart-rules)

Examples:

* default settings: payload 4 kB, 32 k evt/s, small tables (< 8 cols)

```txt
defaults
12 / 32_768, MAX_COLUMNS=8   =  134 MB data +  10 MB meta +  2 MB cols 
=  148 MB  ← 4 KB/row 
```

* You can increase the slot size if you expect large payloads (64 kB) across larger tables (< 32 cols) and more events (64 k evt/s):

```txt
16 / 65_536, MAX_COLUMNS=32   = 4.3 GB data +  21 MB meta +  8 MB cols 
= 4.3 GB  ← 64 KB/row
```

* if you expect many small events (< 2 kB), and possibly long cascade transactions, or losts of events:

```txt
11 / 131_072, MAX_COLUMNS=8  =  268 MB data +  43 MB meta + 8 MB cols 
=  320 MB  ← 2 kB/row, ~1s at 130K evt/s
```

* if you expect possibly large payload (1 MB) with low event count
```txt
20 /  256, MAX_COLUMNS=128 = 268 MB data + 84 MB meta +  < 1 MB cols
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

✅ On a healthy start you get the same arithmetic as a fact:

```txt
info(bridge): MAX_COLUMNS=8 (auto-detected: widest monitored table has 6 columns, rounded up to a multiple of 8)
info(bridge): NATS max_payload: 1024 KB (server-advertised) → CDC per-event buffer: 16 KB (BASE_BUF=14, ceiling 20)
info(bridge): Event ring: 1048 MB of a 16384 MB limit (6%) — 1024 MB data + 20 MB metadata + 4 MB columns
```

#### What happens when a row does not fit

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

Raising `max_payload` in `nats-server.conf` is possible but affects every client and every subject on that server. JetStream's memory use scales with it — so for genuinely large values, prefer **keeping the blob out of the replicated table** and replicating a reference to it (URL object storage).

---

## Requirements, Dependencies, Licenses & Sources

**External dependencies**, via `build.zig.zon`:

* [zig-msgpack](https://github.com/zigcc/zig-msgpack) - MessagePack encoding. License MIT

* [nats.zig](https://github.com/lalinsky/nats.zig) by Lalinsky, License Apache 2. **Currently vendored (in `nats.zig`)**:


**System dependencies**:

* `libpq` ≧ 14  at build time (pipeline mode). License MIT
* `libzstd`, License BSD 3-Clause

**Requirements**:
* `PostgreSQL` 14+/16+ (for standby read replica)
* `Nats/JetStream` 2.10+

Uses `pgoutput` v1 binary mode.

**Sources**:

* PGLITE: <https://github.com/electric-sql/pglite>
* OPFS: <https://webkit.org/blog/12257/the-file-system-access-api-with-origin-private-file-system/>
* SQLite-WASM: <https://sqlite.org/wasm/doc/trunk/index.md>
* SQLite-WASM/persistence: <https://sqlite.org/wasm/doc/trunk/persistence.md>

---

## Roadmap

**Current — v0.14:**

* [x] **The catalogue is the config.** One `zebridge_catalogue` row per table, written by `zebridge_enable(...)`, is the single source the bridge, sweeper and producer read; env vars are optional overrides. `grammar.json` holds only the static wire names; the bridge reconciles its NATS streams itself at boot.
* [x] **Operator/JWT auth for consumers**, with a scoped signing key holding the grant template — a new consumer is one minted JWT + one mapping row, no server-config edit. An enrollment/mint endpoint on the bridge issues credentials; the bridge signs user JWTs in pure Zig.
* [x] **The clients library**, C-ABI `libzb`, JS `zb-client-ts`.
* [x] Read-only or read/write, separable (`init.core.sql` / `init.write.sql`);
* [x] Tenant-scoped reads via RLS, one-or-N bridges per tenant;
* [x] last-write-wins ingress with tombstone + tiebreak.
* [x] Delta **generations** (full+delta chains in object storage) for fast seeding of hot tables.
* [x] Telemetry (`/metrics`, `/status`, `/health`), slots, fleet
* [ ] Diagnostic, per-instance memory sizing, preflight schema analysis
* [ ] Test suite: chaos, adversarial-input, stress, spike, sustained flood, and memory-leak scenarios (both build modes).

**Next:**

* [x] **`libzb` native** — one sans-I/O core, now carved in TypeScript (`zb-client-ts/src/core.ts`) with a language-neutral conformance suite (`zb-client-ts/fixtures/core-fixtures.json`), to be ported to Zig and compiled native (`.so`/`.dylib`/`.dll`, C ABI) for FFI hosts — mobile (Swift/Kotlin/Dart), native microservices, Windows (.NET/C++). The port is correct when it passes the same fixtures. JavaScript hosts need none of it: `zb-client-ts` already _is_ the core.
* [ ] **Auth callout** — the login rides the NATS connect (`$SYS.REQ.USER.AUTH`), so even the mint endpoint disappears; the bridge is the responder.
* [x] Split READ (CDC + bootstrap) onto a **standby replica** (PG ≧ 16) from WRITE on the primary, with chain building on the replica. Point `DATABASE_READER_URL` at a hot standby; the bridge detects it (`pg_is_in_recovery()`) at boot. The CDC slot, preflight, the catalogue and the chain snapshots stay on the standby, so the decode work leaves the primary. `DATABASE_WRITER_URL` becomes required: a standby refuses every write, so the bridge's own bookkeeping (width budget, refusal mirror, `zebridge_generations`) goes over the writer instead. Set `hot_standby_feedback=on` on the standby, or the primary can vacuum away rows the standby's slot still needs and invalidate it (the bridge warns). Replay lag is added to CDC latency.
* [x] TLS on the NATS↔leaf and PG↔bridge links for cross-network deployments.
* [ ] **Windows Server for the bridge daemon** — Zig targets `x86_64-windows`, but the daemon has Unix-isms to port first: POSIX signal handling for graceful shutdown (→ `SetConsoleCtrlHandler`) and the `poll()` WAL loop (→ `WSAPoll`), then the scenario suite re-run on Windows. Colocation with NATS is usually Linux, so this is for Windows-only shops. **Separate from the consumer**, which already runs on Windows today — native `libzb.dll` via P/Invoke, or `libzb.js` in a Node/Electron service, no wasm needed.
* [ ] **Prove the write-path lock on every local engine** — enforced today for browser SQLite and PGlite (single owned connection, both); still to implement/verify the schema-migration lock (views + triggers) for mobile/microservice SQLite and local Postgres.
* [x] **A post-boot wiring checker** — `python3 scripts/zbdoctor.py` (add `--json` for CI). Five gates, one verdict: the bridge is alive (`/health`, `/status`), PostgreSQL is wired (the `zebridge_audit_*()` functions read _through_ the catalogue, so a declared-public table is not flagged for being unscoped), NATS carries the topology (CDC streams, KV buckets), a fresh client can seed and follow (schemas published, tenants resolvable, every `(tenant, table)` chain present _and_ its full object actually fetchable), and no declared-vs-actual drift (delegates to `check.py`). Stdlib-only — it runs on a box with `psql`, `nats` and python3.
* [ ] `bridge_sweeper` on completion?
