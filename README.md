# Sync PostgreSQL locally

<p align="center"><img  width="355" height="233" alt="Screenshot 2025-12-26 at 02 37 57" src="https://github.com/user-attachments/assets/b3701ef4-2d58-497a-be21-52ad1b970644" /></p>

![Zig support](https://img.shields.io/badge/Zig-0.16.0-color?logo=zig&color=%23f3ab20)

**What is it?**:  An opinionated, bidirectional bridge to synchronize a single PostgreSQL(14+) database with a _local replica_ (SQLite, PGlite) via the message broker NATS/JetStream (2.10+).

**One bridge with two components** and a consumer builds on the client library.

```mermaid
flowchart LR
     subgraph VPN["VPN"]
        PG[("Postgres")]
        subgraph Localhost ["VPS localhost"]
            Bridge(("ZeBridge <br> daemon"))
            NATS[("NATS")]
        end
        PG <--> Bridge
        Bridge <-->  NATS
    end

    NATS <--> |WSS| Lib


    subgraph Edge["Consumer"]
        Lib(("libzb(.js)"))
        SQL[("SQLite<br>PGLite")]
        App["mobile<br>browser<br>service"]
        SQL -->App
        App <-->Lib
        Lib <-->SQL
    end

    style Bridge fill:#f59e0b,stroke:#d97706,color:#000
    style Lib fill:#fbbf24,stroke:#f59e0b,color:#000
    style NATS fill:#10b981,stroke:#059669,color:#000
```

**How does it work?**: two parts, a daemon and a client library.

* the background executable `ZeBridge` (ZB) is connected to PostgreSQL (PG) and NATS/JetStream (NATS) and streams PG changes onto NATS and applies writes coming back. This is lightweight process, can be started / stopped on the fly.
* the client library `libzb` handles the incoming data (seeds and CDCs) from NATS into the local database, and pushes optimistic consumer writes to NATS, echoed back after conflict resolution. The library comes in two flavours: a JavaScript library and a dynamic linked library (FFI).

**Consumers**: can be used by browsers (OPFS & sqlite-wasm | PGlite), mobile apps (native SQLite) and backend services (PGlite, native SQLite, PG).

**Local_DB**: standard SQLite, PGlite or PostgreSQL.

**Design**: This tool is built to serve a large number of small to medium consumers via the NATS message broker. It aims to be light (3.5 MB executable), fast, quick startup while safe.

In order to favour mobile usage, we use aggressive delta compression when reseeding. This reduces the need for lengthy CDC catchups.

⚠️ ZB moves rows, not files. This means it is NOT designed for very large tables or tables containing large objects. NATS forbids large payloads (> 1 MB by default), and on the other side, the memory consumption of ZeBridge would explode.
> Large payloads belong in object storage: database tables should only contain metadata or a reference (e.g., a bucket URL) to the blob.

**Opinionated**: the current ZB version makes decisions for you that other sync engines leave you to: conflict resolution, via LWW.

This imposes constraints -mostly mechanical- on the database schemas but buys guarantees.

On the consumer side, we expect a standard SQLite engine. The client can read freely the local database, but writes **must** go through the library. How it is enforced depends upon the local engine.

**Configuration**: the most important configuration used by ZeBridge concerns its **fixed-size buffer**. 
➡ Total buffer size can be set anything from 16MB to 4GB+, depending upon of the published tables you wish to track.

**Monitoring**: ZB exports metrics in the format for the Prometheus format and logs (Loki) for Grafana dashboards.
ZB internally monitors the payload size and quarantines trespassing tables that exceed NATS limits and buffer limits. Conversely, it also prevents sending large payloads to PG as the echoed change would fail to pass.

**Sweeper**: because consumers apply _soft-deletion_, we have a garbage collector which runs as a cron job. The executable `bridge_sweeper` prunes rows marked for deletion, and these are echoed via CDCs to consumers who applied soft-deletion.

**Status**: Dev stage. Chaos tested but not battle tested.

## Table of Contents

* [Overview](#overview)
* [Opinionated](#opinionated)
* [The consumer side — use the library](#the-consumer-side--use-the-library)
* [Architecture & Internals](#architecture--internals)
  * [Design overview](#design-overview)
  * [Inside](#inside)
  * [Safety & Guarantees](#safety--guarantees)
* [Deployment & Setup](#deployment--setup)
  * [Server setup side](#server-setup-side)
  * [Quick review of PG & NATS setup](#quick-review-of-pg--nats-setup)
  * [Running the Bridge](#running-the-bridge)
  * [Local Build Instructions](#local-build-instructions)
  * [Notes on nkeys](#notes-on-nkeys)
* [Configuration & Tuning](#configuration--tuning)
  * [Configuration](#configuration)
  * [Restart rules](#restart-rules)
* [Monitoring & Telemetry](#monitoring--telemetry)
* [Testing](#testing)
* [Requirements, Dependencies & Licenses](#requirements-dependencies--licenses)
* [Roadmap](#roadmap)

---

## Overview

The daemon is connected to PostgreSQL with all the migrations up, and NATS up with JetStream enabled and configured. A consumer connects to the NATS server, or to a [leaf node](https://docs.nats.io/learn/topologies/leaf-nodes#what-a-leaf-node-is) per tenant and his local database via the library.

| artifact | what it is | who uses it |
| -- | -- | -- |
| zebridge | executable (daemon) | running  next to PG and  NATS |
| libzb | native library, C ABI | FFI Consumers: mobile apps, desktop apps, microservices via FFI |
| zb-client-ts | npm package (self-contained TypeScript) | JS Consumers: browsers, Node, Electron, Deno, Bun |

`ZeBridge` projects PG into NATS and back. It defines a protocol—a set of rules and workflows—for a consumer to connect to NATS and to the local database.

The C-ABI `libzb` library for FFI users - and the `zb-client-ts` library for JavaScript-based consumers - implements this protocol.

It abstracts away the complex choreography required to manage NATS streams, KV buckets, data decompression and deserialization, and local state tables for reconnection.

The client library shrinks this orchestration down to three primitives: `query()` to read, `mutate()` to propagate local optimistic writes back to the Postgres server, and `onChange()`.

**An example of a deployed system**:

```mermaid
graph TD
    %% Styling and Definitions
    classDef external fill:#f9f,stroke:#333,stroke-width:2px;
    classDef proxy fill:#bbf,stroke:#333,stroke-width:2px;
    classDef internal fill:#dfd,stroke:#333,stroke-width:1px;
    classDef secure fill:#fdd,stroke:#333,stroke-width:1px;
    classDef telemetry fill:#fff2cc,stroke:#d6b656,stroke-width:1px;
    classDef bridge fill:##bdf3ff,stroke:#ac0100,stroke-with:2px;

    %% External Clients
    User([mobile / browser <br> ---libzb --- <br>SQLite / PGlite]):::external
    LocalDB[(localDB <br>PG/lite<br>SQLite)]:::secure
    LocalDB@{shape: lin-cyl}
    RemoteLeaf[NATS Leaf Node]:::internal
    Consumer([Service <br> --libbz--]):::external

    %% Cloudflare Edge
    subgraph Cloudflare_Network  [Cloudflare Edge Proxy]
        CF([https://my-domain]):::proxy
        CF@{ shape: cloud}
    end

    %% VPS Boundary
    subgraph VPS [Your VPS Server]
        HA([HAProxy<br>:443]):::proxy
        
        %% Internal Apps
        Bridge[[ZeBridge-1<br> :27434]]:::bridge
        Bridge@{shape: st-rect}
        PG[(Postgres Master <br> :5432)]:::secure
        PGREP[(PG StandBy<br>Replica<br>:5433)]:::secure
        NATS[NATS Server<br> :4222]:::internal
        NATS@{shape: data-store}
        
        %% Telemetry & Monitoring Stack
        Grafana([Grafana <br> :3000]):::telemetry
        Prom[(Prometheus<br> :9090)]:::telemetry
        NatsExp([NATS Exporter<br>Port :7777]):::telemetry
        Sweeper[[Sweeper]]:::bridge
    end

    subgraph Consumer Service
      Consumer
      LocalDB
      RemoteLeaf
    end
    %% External Connections to Cloudflare
    User <==>|HTTPS <br> WSS| CF
    RemoteLeaf <==>|Outbound Connect| CF
    Consumer <==>|Connects Local| RemoteLeaf
    Consumer -->LocalDB

    %% Cloudflare to HAProxy Subdomain Routing
    CF -.->|grafana.my-domain| HA
    CF -.->|bridge.my-domain| HA
    CF <==>|nats.my-domain| HA

    %% HAProxy Internal Layer 7 Routing
    HA -.->|localhost:3000| Grafana
    HA -.->|localhost:27434/enroll| Bridge
    HA <==>|wss://localhost:4222| NATS

    %% Internal Component Dependencies
    Bridge ==>|W| PG
    PGREP ==>|R|Bridge
    Bridge <==>|Pub / Sub <br> TCP| NATS
    
    %% Telemetry Data Flow
    Prom -.->|Scrapes| Bridge
    Prom -.->|Scrapes| NatsExp
    NatsExp -.->|Monitors| NATS
    Grafana -.->|Queries| Prom
```

## Opinionated

ZeBridge makes choices that a general sync engine usually leaves to you. These choices act as constraints—though mostly mechanical—and that is the point: each one buys a specific guarantee.

Here they are, so you can judge the fit before adopting it.

**Every consumer is an identity in a tenant.** A consumer connects as a _principal_ — a stable, unique name — that belongs to exactly one tenant. Reads are scoped to that tenant by PostgreSQL RLS; writes are confined to that principal by NATS subject grants. There is _no anonymous consumer_ and no cross-tenant read.
➡ _Enrollment_ is the only way in: a JWT minted under a scoped signing key, plus a new `zebridge_user_tenants` row — one identity, written once, in two projections.

**Tables must qualify — the schema carries the contract**: becauses tables are either public or tenant scoped, read-only or writable with a LWW conflict resolution policy, we impose some constraints:

* A replicated table needs a **primary key**. Because a client mints its own keys offline, a _writable_ table's key must be **client-generable** — a `uuid`, not a `bigserial` that the database hands out (an edge write to a sequence key would collide with the server's next insert, so the bridge refuses it).
* A non public table needs a `tenant_id` column.
* A writable table needs a **version column**, a `timestamptz` — ⚠️ never a naive `timestamp`. The timestamp guard refuses one at `CREATE`/`ALTER`, because "newer" must be an absolute instant, otherwise last-write-wins is meaningless.
* A writable table needs a **tombstone** column (a delete becomes a soft-delete so an offline client cannot resurrect a removed row) and an optional **tiebreak** column (resolves equal versions instead of refusing both).
* **Payload size limited**: because NATS caps the message payload with an already generous default 1MB, and because the bridge runs on a fixed buffer. A row too wide for the change feed is refused _at write time_, both from the edge and from `psql`. The table should only hold the metadata.

* We have tools to **diagnose** the database and tables; run them against your already migrated database to check if it they are ready and get feedback. Once the bridge is up, you can also check the design choices against the running setup with a simple diagnostic tool.

**Writes are resolved, not merely accepted — last-write-wins.** The writes use three verbs (INSERT, DELETE, UPDATE), resolved via **last-write-wins** (LWW) using Hybrid-Logical-Clock (HLC) logic.

A write carries the version the client holds; the bridge applies it **only if it is newer** than what Postgres has, and rejects a stale one.

This is a deliberate design choice, otherwise you observe whatever results.

ZeBridge arbitrates at ingest, so a slow or offline client cannot silently clobber a newer edit, and a stale queued write cannot undo a delete.

The cost is that LWW is the only resolution offered today.

> A future build may add a neutral "write-through, observe the echo" mode for apps that prefer the Electric/PowerSync shape; until then LWW is enforced, and the version column is how you get it right.

**The library owns the write path — reads are open, writes go through `mutate()`.** The consumer reads its local database freely — any SQL, joins, aggregates, offline — but changes only through the library, so every write gets the outbox, the version stamp and the LWW echo. A write that skips the library is a _bug_ you should not be able to make by accident. _How_ that is enforced depends on the local engine, and here is how we approach it:

* **Browser SQLite (one OPFS connection)**: Enforced. the library owns the single connection and hands the app a **read-only** handle — a direct write is simply unreachable.  today.
* **Mobile and microservice SQLite**: Enforced. SQLite is the only mobile engine, and there the library does not own the connection the same way — so the lock moves into the schema: an **initial migration** makes the app-facing tables read-only (views + triggers) and routes writes through the library's own path. ➡ Enforced by the schema, not the handle.
* **PGlite:** a supported engine (`?engine=pglite` in web-consumer; adapter at `zb-client-ts/pglite`, dialect seam in `zb-client-ts/src/dialect.ts`). The library owns PGlite's single in-memory connection exactly as it owns the OPFS one, so the same handle-level lock applies; the schema-migration path on PGlite is not yet driven.
* **Local Postgres (microservice):** the same choice as PGlite, ➡ schema-enforced.

The rule is the same everywhere; the _mechanism_ that guarantees it is engine-specific. It is why the library — not a set of naming conventions — is the API.

**Authorization lives where the data does — no gatekeeper, no DSL.** NATS grants (a scoped JWT signing key) decide which subjects a principal may touch; PostgreSQL RLS and the tenant guard decide which rows it may read and write. There is no sync-rules language to author and no separate authorization service to run and keep in sync — the two systems that already hold the data hold the rules, and the principal is a subject token the broker vouches for, never a claim in a payload.

**The daemon is stateless; state has one home each.** The bridge holds no certificates and does no NATS-side lookup — everything it needs (the catalogue, the rules, the tenants) comes from Postgres, and it never reads its own output back from NATS. What lives in the process is a small, self-invalidating cache, nothing durable. So a bridge is cheap to start, cheap to restart, cheap to colocate, and never itself a source of truth: ZeBridge's state is in Postgres, the consumer's is its local replica plus its NATS stream position, and neither side is the other's cache.

These opinions aim in one direction: many small consumers that read freely, write safely, and never cross the tenant line. That is what the two components are for — `zebridge` next to Postgres, `libzb` next to the consumer — and together they make the guarantees above. It is a bridge with two ends you build on, not a bare pipe.

## The consumer side — use the library

You do not talk to NATS by hand. The library does all of it: it watches the schema, seeds the local database (from the generation chain), follows the change feed, applies rows last-write-wins, and sends your writes. It owns the local SQLite, so a write can only go through the library.

Two builds of the same core:

* **`libzb.js`** — a self-contained TypeScript package that runs **as-is** in any JS runtime: browsers, Node, Electron, Deno, Bun. No wasm and no native library needed — a JavaScript host just uses this.
* **`libzb`** — a native library with a C ABI for mobile apps, desktop apps and microservices (via FFI).

The API is small — three verbs and a doorbell:

* **`query(sql)`** — read your local database directly. Any SQL: joins, aggregates, offline. The replica _is_ the API.
* **`mutate(table, key, values)`** — one write, three verbs (insert, update, delete), resolved last-write-wins. This is the only way to change data.
* **`onChange(table, cb)`** — a doorbell. When the change feed touches a table, re-run your query.

<details><summary>An example of a mutation query</summary>

```sql
UPDATE orders
SET status = 'Expired';
WHERE id IN (
    SELECT status 
    FROM orders 
    WHERE order_date < '2023-01-01' AND status = 'Pending'
);
```

becomes:

```js
// the SELECT half runs against the local replica — free, works offline
const orders = await zb.query(
  `SELECT id FROM orders WHERE order_date < '2023-01-01' AND status = 'Pending'`
);

for (const row of orders) {
  await zb.mutate(
    'orders',               // table
    'UPDATE',               // op: 'INSERT' | 'UPDATE' | 'DELETE'
    { id: row.id },         // key: the PRIMARY KEY column(s) — composite is fine: { org_id, id }
    { status: 'Expired' },  // values: ONLY the changed columns
  );
}
```

</details>
<br>

That is the whole contract for an app author. The wire format — the NATS subjects, the KV buckets, the chain layout, the tenant scoping — is the library's own business, not yours. It is written down in [PROTOCOL.md](PROTOCOL.md) for one reason: someone writing a **new** language binding. Building an app, you never read it.

**Getting a consumer connected** is enrollment: the app authenticates to your backend (or the bridge's mint endpoint), receives a JWT credential, and connects. The principal comes back _inside_ the credential — the consumer never types it. The same model works for every consumer type; see [Credentials & trust boundaries](#credentials--trust-boundaries).

**Examples [TODO]**: [App.tsx](/web-consumer/src/App.tsx) (browser, live), a [Flutter](/flutter) example, and planned Node, Go, Python and Elixir microservices.

## Safety & Guarantees

### At-Least-Once Delivery

The full mechanism — bridge ACKs PostgreSQL only after JetStream confirms, what happens if the bridge crashes, what happens if NATS crashes — is covered in [Bridge ACK Flow and NATS outages](#bridge-ack-flow-and-nats-outages). The guarantee in one line: **no data loss between Postgres and NATS**, because the ACK to PostgreSQL only happens after JetStream has durably persisted the message, and JetStream's Msg-ID deduplication absorbs any retry.

### Zero-Consumer Protection & Storage Bounds

**What happens if PostgreSQL emits CDC events when no NATS clients are connected?** Nothing accumulates unbounded on either side. The bridge keeps ACKing PostgreSQL as normal — that only depends on JetStream, not on a consumer being present — so PostgreSQL's WAL stays bounded regardless. On the NATS side, the `CDC` stream's own retention policy (`--max-age`, `--max-bytes` — see [The NATS streams and buckets](#the-nats-streams-and-buckets)) purges old events even with zero subscribers. A client that reconnects after being offline re-seeds from the latest **generation chain** and resumes from `CDC`.

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

Each chain manifest carries its cutoff — `cutoff_lsn`, and `cutoff_seq`, the CDC stream sequence captured before the build — so a consumer knows exactly which events its seed already contains and discards them (the library handles this — [the consumer side](#the-consumer-side--use-the-library)).

Event ordering follows from [one WAL-reading thread per bridge](#design-overview): PostgreSQL hands the bridge a strict total order — transactions in **commit** order, and each transaction's changes in execution order — and the bridge reads it sequentially.

⚠️ **Order is preserved WITHIN a table, not ACROSS tables.** A flush is grouped by subject before publishing, which collapses interleaving: when a run begins mid-pair, a child's batch can be published before the batch carrying its parents. Measured. Most consumers never notice — rows are applied by primary key, last-write-wins, idempotent — but a consumer holding a constraint that spans tables (a foreign key) must be order-tolerant, which is why `libzb` holds a row whose parent has not arrived and retries it after the next batch.

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

systemctl stop zebridge | pkill ./bridge | docker stop bridge
```

**Guarantees:**

* No in-flight events lost
* PostgreSQL knows exact resume point
* Clean restart from last ACK'd LSN

---

## Authentication between the parties

Four separate keys, four separate boundaries.

| boundary | credential | who holds it |
| --- | --- | --- |
| create the roles (once) | PostgreSQL **superuser** | the DBA / the init step — never the bridge at runtime |
| bridge → PG | the two **role passwords** (`bridge_reader`, `bridge_writer`) | the bridge, in `.env.bridge` |
| bridge → NATS | **nkey** (NATS has the public key in `nats.conf`) | the bridge holds the seed. Plain TCP on the colocated hop, by design |
| consumer → NATS | a **JWT** (scoped signing key) + nkey seed | every consumer, from enrollment |

### ZeBridge with Postgres

**DBA creates two USER profils in .env.bridge for the init scripts**:  each script _init.core.template.sql_ and _init.write.template.sql_ creates the two ZeBridge `USER` profils.

```sh
# .env.admin — the DBA sets the role names and passwords; 
# the templates interpolate them
POSTGRES_READER_USER=bridge_reader      # created by init.core.template.sql
POSTGRES_READER_PASSWORD=reader_password_changeme

POSTGRES_WRITER_USER=bridge_writer      # created by init.write.template.sql
POSTGRES_WRITER_PASSWORD=writer_password_changeme
```

### ZeBridge with NATS

**DBA mints the nkey pair for ZeBridge ↔ NATS**: mint the nkey pair a DBA installs between NATS and the bridge by using the bridge as a CLI:

```sh
./bridge --gen-nkey >> .env.bridge
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

Now that the two PG USER profils are created and the NKEY keypair in place, the DBA can run the database migrations and `zebridge_enable()` the tables on the publication.

ZeBridge can now be started with the credentials:

```sh
set -a && source .env.bridge && set +a && \
DATABASE_READER_URL=postgres://bridge_reader:reader_password_changeme@127.0.0.1:5432/postgres \
DATABASE_WRITER_URL=postgres://bridge_writer:writer_password_changeme@127.0.0.1:5432/postgres \
NATS_BRIDGE_NKEY_SEED=SU... \
./bridge --pub my_pub --slot my_slot --port 27434
```

### Client to NATS dance

A client never gets a password. It gets a **`.creds` file**, which holds two things:

* a **user JWT** — public. It says "this public key is `omar`, tenant `kilo`", and it is signed by the account.
* a **user seed** — private. The client's own key. It never leaves the device.

The DBA will create an invit token and send it to the client.

Once the client authenticates (any mean, biometric, Google account...), the client will connect to the domain serving the proxied bridge endpoint <https://my-user-bridge:27434/enroll> and send his invit and the NKEY_PUBLIC he generated. ZeBridge will return a JWT that the client will use to connect to NATS.

**Step by step**:

1. **The client makes its own keypair.** `nkeys.createUser()` in the browser, or the equivalent on mobile. It keeps the seed.
2. **The DBA hands out an invite.** One row in `zebridge_invites`: a code, the principal it will become, and its tenant.
3. **The client asks the bridge for a JWT.** It sends the invite code and its **public** key only:

   ```txt
   GET /enroll?code=<invite>&user_pubkey=U...
   ```

4. **The bridge mints it**: in one transaction it redeems the invite (stamps `used_at`, writes the `principal → tenant` row), then signs a user JWT for that public key with the account signing key (`ZB_SIGNING_SEED`). It answers `{"jwt":"..."}` — nothing else.
5. **The client assembles the creds file** from the JWT it received and the seed it already had. Browser: `sessionStorage`. Mobile: the keychain. Service: a file or a secret mount.
6. **It connects to NATS**: the NATS server sends a random nonce. The client sends its JWT plus a signature of that nonce made with its seed. The server checks the JWT against the account key it trusts, takes the public key out of the JWT, and verifies the signature. **No secret crosses the wire.**
7. **It expires.** Minted JWTs live 24 h (`enroll_jwt_ttl_seconds`). After that the client enrolls again.

The consumer boundary is one model for **every** consumer type — webapp, mobile, or microservice.

The JWT and its check are the same everywhere; only the transport (WebSocket for the browser, TLS-TCP for native), where the credential is stored, and the client runtime differ.

The permissions live once on the signing key's template (`mutation.{{name()}}.>`, `cdc.{{tag(tenant)}}.>`); a new consumer is one minted JWT plus one `zebridge_user_tenants` row, no server-config edit.

On the PostgreSQL side, RLS and the tenant guard bound the rows a principal may read and write. See [SECURITY.md](SECURITY.md) for depth.

**Who holds what:**

| who | holds | can |
| --- | --- | --- |
| the client | its own seed + its JWT | be itself |
| ZeBridge | the account signing seed (`ZB_SIGNING_SEED`) | mint JWTs for others |
| the NATS server | the operator JWT | trust what the account signed |

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

## Architecture & Internals

### Design overview

ZeBridge projects PostgreSQL onto NATS and never reads its own output back. **Postgres is the source of truth for the bridge; NATS is the source of truth for consumers.** A consumer's state only ever arrives through the change feed.

**The main loop, four steps:**

1. **Read the WAL.** One thread follows PostgreSQL's logical replication stream (`pgoutput`), in order.
2. **Decode into a fixed buffer.** Each change is decoded into a pre-allocated slot — memory is bounded at startup, not grown per event.
3. **Batch to NATS.** Decoded events are published to JetStream in batches.
4. **Acknowledge.** Only after JetStream confirms does the bridge ACK that position to Postgres, which can then reclaim WAL. If the bridge crashes, Postgres keeps the unpublished WAL — nothing is lost.

**Two sides, two patterns.** Egress (Postgres → NATS) is a **push**: the bridge publishes as changes happen. Bootstrap and ingress (consumer ↔ NATS) are **pull**: the consumer pulls CDC and chain objects at its own pace and pushes its writes on its own subject. The consumer controls replay.

**Seeding: generation chains.** A fresh or fallen-behind consumer needs a starting point: a **generation chain**. The bridge periodically builds a _full_ plus a series of _deltas_ per table, stored as objects with a small manifest in KV. The consumer reads the manifest, applies the full, then only the deltas newer than its watermark — cheap and incremental, so a returning consumer usually just needs the latest delta.

A brand-new table has no chain until the next cadence tick; the client waits for it (a bounded wait) instead of asking for a bespoke dump.

The manifest's `cutoff_seq` — the CDC stream sequence captured before the build — is the splice point: the consumer lands on it and follows CDC from there.

**Why generations** (and why there is no snapshot-on-demand):

* **No connection storm on Postgres.** An on-demand dump is served _per request_, one at a time, from Postgres — a fleet reconnecting at once would queue against the database. A generation is built once by a background job (the producer, on a cadence) and pushed to object storage; any number of consumers then pull the same almost-fresh chain from storage, never touching Postgres. The storm hits object storage, which fans out cheaply.
* **One copy, partitioned — not N dumps.** The chain slices the database by tenant and table, so storage holds a single partitioned copy, not a fresh full dump per consumer request. The data lives once.
* **CDC stays small.** Because a consumer always seeds from a recent generation and only needs CDC back to the last cutoff, the CDC stream retains at most about two generation windows — bounding its size instead of letting it grow with the offline window.

**The message format.** CDC events and chain rows travel as **MessagePack** — compact, type-safe, fast (it keeps the int/float/binary distinctions JSON loses). Schemas travel as JSON in two shapes (PostgreSQL and SQLite), so a client builds its local tables in either. Every write carries a message id, for idempotent at-least-once delivery.

**One instance = one slot = sequential.** A bridge runs one replication slot and processes it in order. Scale by running more instances on more slots/publications, each with its own buffer — not by making one instance bigger.

### Bridge ACK Flow and NATS outages

```txt
PostgreSQL WAL → Bridge → NATS JetStream
              ↑            ↓
              └─── ACK after JetStream confirms
```

1. Bridge receives WAL event from PostgreSQL
2. Bridge buffers in the ring (NATS outage)
3. Bridge publishes to NATS JetStream (async), batch: 5000 events OR 500ms OR 256KB
4. **JetStream confirms** message is durably persisted (file storage)
5. Bridge ACKs that LSN to PostgreSQL
6. PostgreSQL can safely prune WAL up to that LSN

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

Seeding — reading the manifest, applying the chain, the two-clock bookkeeping around its cutoff — is the library's job, not the app's ([the consumer side](#the-consumer-side--use-the-library)). This section is about the bridge's own guarantees.

---

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

* Automatic (handled by `nats.zig`)
* Max attempts: -1 (infinite)
* Wait between attempts: 2 seconds
* Flush timeout: 10 seconds

## Local PG, Daemon, NATS Deployment & Setup

ZeBridge runs in three contexts:

* **the test suite** runs Postgres and NATS natively on the host — fast to iterate, and driven by the test scenarios. For development only.
* **the docker compose evaluation** — trying ZeBridge out — is best as a `docker compose` stack: it mimics a production setup: Postgres via TCP, NATS via TCP, NATS-exporter (telemetry), Prometheus (TSDB pulling from ZeBridge), Grafana, bridge_sweeper, ZeBridge and the reverse proxy HAProxy in one file.  so a fresh environment comes up identically every time (fully Infrastructure-as-Code).

  NATS-exporter, Prometheus, Grafana and ZeBridge are behind HAProxy.
  You can access a prebuilt Grafana dashboard for a nice monitoring.
  Rate limiting is delegated to the HAProxy.
  NATS goes through HAProxy so serve consumers over WS (normally over WSS).

* **Production** is your own topology, and here compose is not a recommendation to avoid any overhead. Postgres may be a managed or remote instance and the system will inevitably suffer from latency. NATS may be remote too, although for performance, the bridge should sit to the `nats-server` and communicate by _plain text_, over TCP.

  For example, when all three run on host, during a spike (800 k writes/s), CPU usage is largely dominated by Postgres with around 70% of the host CPU, delivering 300 kCDC/s whilst ZeBridge uses ~20-25% and NATS ~5-10%.
  * So what actually matters is **colocation**: the bridge should sit next to `nats-server` — their hop is plain TCP for speed, so it must not cross a network
  * the strong setup puts **Postgres + zebridge + nats-server + Prometheus scrapper + Grafana + nats-exporter+bridge_sweeper + HAProxy** together behind one boundary, one domain. Consumers connect directly to NATS via WSS and the reverse proxy fronts the NATS-exporter, Prometheus and ZeBridge's HTTP surface over TLS — for **Grafana** and the consumer **JWT enrollment dance** (`/enroll`) — with a domain and whatever auth you put in front. The bridge holds no certificates of its own, and HAProxy should terminate the SSL (or sligntly less secure, the DNS Cloudflare).


### Docker compose setup

You have the files [grammar.json, .env.docker, Dockerfile, docker-compose.full.yml], at the folders _/telemetry_ and _/proxy_ aside.

You must be a bit patient the first time you run:

```sh
docker compose -f docker-compose.full.yml --env-file .env.docker up -d
```

The bridge is 30MB. The sweeper is 185kB

> The NATS setup is a bit acrobatic. The image "nats-box" already contains `jq` (to substitute _grammar.json_).
> Postgres is setup with `wal_level=logical`, and then we run a on-shot PG server initialization (it does not contain ``gettext-base` for `envsubst` so the image brings it in).

You now have access to a Grafana dashboard at <http://localhost:3000> (admin|admin) and query the bridge at <http://localhost:8090/status>, <http://localhost:8090/metrics>.

It remains to run a client.

### Host setup

You have `zstd`, `libpq` available on your host.

We suppose the DBA credentials are:

  ```sh
  export ADMIN_DATABASE_URL=postgres://admin:s3cret@l127.0.0.1:5432/my_db

  #or, in ~/.pgpass, 127.0.0.1:5432:my_db:admin:s3cret, and
  ```

You have a copy the files _.env.bridge, .env.admin, grammar.json_  next to the ZeBridge binary,

You have generated the keychain for NATS and ZeBridge:

  ```sh
  ./bridge --gen-nkey >> .env.bridge
  ```

You have enabled PG Logical Replication. On host, in `postgresql.conf`:

  ```sh
  wal_level = logical
  max_replication_slots = 10 #<- also the max number of ZeBridge instances
  max_wal_senders = 10
  max_slot_wal_keep_size = 10GB
  wal_sender_timeout = 300s  # 5 minutes
  ```

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

**Place the wire grammar.** ❗️ Copy `grammar.json` where the bridge and the NATS setup can read it — it holds the static names both sides share.

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

Two couplings matter: the product `GENERATION_CHAIN_DEPTH × GENERATION_CADENCE_SECONDS` must stay under the sweeper's `GC_THRESHOLD_MS` (a tombstone must never be reaped before the delta that ships it — the bridge states the number at boot), and CDC retention should comfortably exceed one cadence, so a fresh chain always overlaps the stream it splices into (the manifest's `cutoff_seq` is the splice point).

Consumers use these streams to interact with NATS; the exact names are declared in `grammar.json`.

### The memory setting

The fixed memory used by ZeBridge has three dimensions: the slot size, `BASE_BUF` and the number of slots, `RING_BUFFER_COUNT`, and `MAX_COLUMNS` (auto-detected).

NATS messages default to `max_payload=1M`, which is already quite large.

Depending on the size of the published tables you wish to track, the maximum row size, the maximum number of rows per transaction, and the CDC emission rate you want to buffer during possible NATS reconnections (e.g. buffer 100 to 50,000 evt/s during 1s)

**Row width**: ZeBridge will suspend a table whose rows are wider than the NATS message limit (<1 MB). The NATS cap also means a consumer cannot push a large row to NATS. Above this limit, we are in the domain of Object storage for large blobs, and URLs should be saved in the database instead.

**CDC**: ZeBridge is designed to be fast, with a **fixed memory** defined at runtime.

The `RING_BUFFER_COUNT` is designed to buffer the received events during potential NATS jitters or outages. Its count depends naturally upon the emitting rate.
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

The bridge is then started with:

* a memory budget: `BASE_BUF` (default 2^12 = 4 KB) and `RING_BUFFER_COUNT` (default 32_768), sized to the tables this instance handles — see [Sizing BASE_BUF and RING_BUFFER_COUNT](#sizing-base_buf-and-ring_buffer_count). `MAX_COLUMNS` is usually left unset and auto-detected.
* a unique `--slot` — the WAL pointer PostgreSQL keeps for this instance. Each running instance needs its own.
* a unique `--port` for its telemetry webserver. Each running instance needs its own.
* `--top`, defaulting to `./grammar.json` — the stream/bucket names shared between ZeBridge, NATS and consumers.
* the mandatory `NATS_BRIDGE_NKEY_SEED` env var — the private half of the public nkey the NATS server was given.
* `DATABASE_READER_URL`, `DATABASE_WRITER_URL`, `NATS_URL` — the connection strings.

For example, one instance on the publication `my_pub` (created by the DBA) with the slot `my_slot`:

```sh
BASE_BUF=10 \
RING_BUFFER_COUNT=4096 \
NATS_URL=nats://127.0.0.1:4222 \  # default value
BRIDGE_PORT=9090 \                # default port
NATS_BRIDGE_NKEY_SEED=SU... \            # mandatory
DATABASE_READER_URL=postgres://bridge_reader:bridge_password_changeme@127.0.0.1:55432/postgres \
DATABASE_WRITER_URL=postgres://bridge_writer:writer_password_changeme@127.0.0.1:55432/postgres \
./bridge --slot my_slot --pub my_pub --top grammar.json
```

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

The bridge is told which slot and which publication to use, and refuses to start without both.

```bash
./bridge --slot my_slot --pub my_pub          # flags
BRIDGE_CDC_SLOT=my_slot BRIDGE_CDC_PUBLICATION=my_pub bridge   # or the environment
```

The flags win over the environment, so `.env.bridge` can carry the usual pair and a one-off run can still point at another publication.

The slot is created by the bridge if it does not exist; the publication is not — it must already exist, and the bridge stops at boot if it does not (see `zebridge_enable`).

### Command-Line Options

```txt
  --slot <NAME>     Replication slot (created if absent). No default —
                    required here or as BRIDGE_CDC_SLOT.
  --pub <NAME>      Postgres PUBLICATION to stream. No default —
                    required here or as BRIDGE_CDC_PUBLICATION.
  --port <PORT>     HTTP telemetry port (default: 9090)
  --help, -h        Show this help message
```

## Configuration & Tuning

### Configuration

All configuration constants are centralized in `src/config.zig` and `grammar.json`. Per-table replication rules (tenant column, LWW columns, tombstone) live in `zebridge_catalogue`.

### Key Settings

**bridge-sweeper configuartion**:

**Generation configuration:**

* Cadence: `GENERATION_CADENCE_SECONDS` (chain depth × cadence must stay under the sweeper's `GC_THRESHOLD_MS`)
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

See `src/config.zig` for all tunables.

### Sizing `BASE_BUF` and `RING_BUFFER_COUNT`

⚠️ Read this one

These values are not independent, and getting them wrong has a visible consequence rather than a silent one.

The bridge pre-allocates the ring at startup, in **three parts**:

```txt
ring = ( 2^BASE_BUF  +  sizeof(CDCEvent)  +  MAX_COLUMNS × sizeof(ColumnView) )  ×  RING_BUFFER_COUNT
         ^ data:          ^ metadata:          ^ columns:                          ^ number of events
           max bytes        fixed, 328 B         8 B × MAX_COLUMNS — resolved        buffered ahead
           for ONE row      per event            at boot, not a compile constant     of NATS
```

Examples:

* default settings:

```txt
defaults
12 / 65_536, MAX_COLUMNS=8   =  256 MB data +  20 MB meta +  4 MB cols 
=  280 MB  ← 4 KB rows
```

* You can increase the slot size if you expect large payloads:

```txt
14 / 65_536, MAX_COLUMNS=8   = 1024 MB data +  20 MB meta +  4 MB cols 
= 1048 MB
```

* if you expect many small events:

```txt
11 / 262_144, MAX_COLUMNS=8  =  512 MB data +  82 MB meta + 16 MB cols 
=  610 MB  ← many small events, ~1s at 200K evt/s
```

```txt
20 /  1024, MAX_COLUMNS=128 = 1024 MB data + <1 MB meta +  1 MB cols
= 1025 MB  ← 1 MB rows, wide table, minimum ring
```

`sizeof(CDCEvent)` is small and fixed regardless of table shape — `columns` is a _slice_ into a separate slab, not an inline array, so this term no longer grows with the widest table you might ever replicate.
`MAX_COLUMNS` is what used to be a single fixed 128 baked into every deployment; it is now resolved **per instance, at boot**:

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
info(bridge): NATS max_payload: 1024 KB (server-advertised) → CDC per-event buffer: 16 KB (BASE_BUF=14, ceiling 20)
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
re-seed from the next generation chain.

⚠️ **This is why the metrics endpoint matters.** The event that overflows may arrive years after deployment — someone pastes a large JSON document into a text column — so this is not something you can verify once at install time.
🔔 Alert on `bridge_refused_tables > 0` (Prometheus) or on `SUSPENDING` in the logs (Loki).

#### The ceiling is NATS, not the bridge

`BASE_BUF=20` is 1 MB, which is also **nats-server's default** `max_payload`. A message also carries a subject, headers and MessagePack framing, so a row sized right up to the limit is still rejected at publish time. The bridge reads the server's advertised `max_payload` from its INFO line at connect and tells you where you stand:

```txt
info(bridge): NATS max_payload: 1024 KB (server-advertised) → CDC per-event buffer: 16 KB (BASE_BUF=14, ceiling 20)
```

and warns if the two cannot coexist.
Raising `max_payload` in `nats-server.conf` is possible but affects every client and every subject on that server. JetStream's memory use scales with it — so for genuinely large values, prefer **keeping the blob out of the replicated table** and replicating a reference to it (URL object storage).

---

## ZeBridge Restart rules

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
| | |
| change `BASE_BUF` / `RING_BUFFER_COUNT` <br> / other bridge env | **bridge restart needed** (the bridge re-registers its row-width budget and re-bakes the guards at boot) |
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

## Testing

## An example of a measured throughput

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
```

### Monitoring Replication Slot

```bash
docker exec -it postgres psql -U postgres -c "
  SELECT slot_name, active,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) as lag
  FROM pg_replication_slots
  WHERE slot_name = 'my_slot';
"
```

### Clear the WAL

```sh
docker exec -it postgres psql -U postgres -c "CHECKPOINT;"
```

---

## Requirements, Dependencies, Licenses & Sources

**Managed via `build.zig.zon`**:

* [zig-msgpack](https://github.com/zigcc/zig-msgpack) - MessagePack encoding. License MIT

**Currently vendored (in `/`)**:

* [nats.zig](https://github.com/lalinsky/nats.zig). License Apache 2.

**System**:

* [libpq](https://www.postgresql.org/docs/current/libpq.html) ≧ 14  at build time (pipeline mode). License MIT

* `PostgreSQL` ≧ 14 (`pgoutput` **binary** mode),
* `Nats/JetStream` 2.10+

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
* **The client library**, C-ABI `libzb`, JS `libzb.js` (or the library `zb-client-ts`).
* Read-only or read/write, separable (`init.core.sql` / `init.write.sql`); tenant-scoped reads via RLS, one-or-N bridges per tenant; last-write-wins ingress with tombstone + tiebreak.
* Delta **generations** (full+delta chains in object storage) for fast seeding of hot tables.
* Per-instance memory sizing, preflight schema analysis, a PG connection budget enforced in the roles.
* Telemetry (`/metrics`, `/status`, `/health`)
* robustness suite: chaos, adversarial-input, and memory-leak scenarios (both build modes).

**Next:**

* [x] **`libzb` native** — one sans-I/O core, now carved in TypeScript (`zb-client-ts/src/core.ts`) with a language-neutral conformance suite (`zb-client-ts/fixtures/core-fixtures.json`), to be ported to Zig and compiled native (`.so`/`.dylib`/`.dll`, C ABI) for FFI hosts — mobile (Swift/Kotlin/Dart), native microservices, Windows (.NET/C++). The port is correct when it passes the same fixtures. JavaScript hosts need none of it: `zb-client-ts` already _is_ the core.
* [ ] **Auth callout** — the login rides the NATS connect (`$SYS.REQ.USER.AUTH`), so even the mint endpoint disappears; the bridge is the responder.
* [ ] Split READ (CDC + bootstrap) onto a **standby replica** (PG ≧ 16) from WRITE on the primary, with chain building on the replica.
* [ ] TLS on the NATS↔leaf and PG↔bridge links for cross-network deployments.
* [ ] **Windows Server for the bridge daemon** — Zig targets `x86_64-windows`, but the daemon has Unix-isms to port first: POSIX signal handling for graceful shutdown (→ `SetConsoleCtrlHandler`) and the `poll()` WAL loop (→ `WSAPoll`), then the scenario suite re-run on Windows. Colocation with NATS is usually Linux, so this is for Windows-only shops. **Separate from the consumer**, which already runs on Windows today — native `libzb.dll` via P/Invoke, or `libzb.js` in a Node/Electron service, no wasm needed.
* [ ] **Prove the write-path lock on every local engine** — enforced today for browser SQLite and PGlite (single owned connection, both); still to implement/verify the schema-migration lock (views + triggers) for mobile/microservice SQLite and local Postgres.
* [x] **Retire the snapshot path.** Done end to end (2026-08-27, NOTES §10n–§10p): `libzb` seeds from generation chains only — bounded wait for a chain not yet built, per-stream scoped re-seeding, and a chain-full can never destroy newer data; the producer publishes explicit empty manifests for known tenants; and the bridge-side serve path (snapshot listener, COPY-binary encoder, REQUESTS/`SNAP_RET`, the INIT stream family, `$KV.snapshots`) is deleted. One fewer moving part, and Postgres is never queried per consumer.
* [x] **A post-boot wiring checker** — `python3 scripts/zbdoctor.py` (add `--json` for CI). Five gates, one verdict: the bridge is alive (`/health`, `/status`), PostgreSQL is wired (the `zebridge_audit_*()` functions read _through_ the catalogue, so a declared-public table is not flagged for being unscoped), NATS carries the topology (CDC streams, KV buckets), a fresh client can seed and follow (schemas published, tenants resolvable, every `(tenant, table)` chain present _and_ its full object actually fetchable), and no declared-vs-actual drift (delegates to `check.py`). Stdlib-only — it runs on a box with `psql`, `nats` and python3.
* [ ] `bridge_sweeper` on completion?
