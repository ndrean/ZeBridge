# ZeBridge compared with PowerSync

The measurements behind the README's [Compared with PowerSync](README.md#compared-with-powersync-and-electricsql): the same workload, run by both systems against one PostgreSQL database. The architecture and what each asks you to run are in the README; how to run all of it again is in [the harness](scripts/compare/powersync/README.md).

A separate [direct Postgres versus libzb benchmark](scripts/compare/postgres/README.md)
measures one fresh SQLite database at a time, including local schema creation and
row loading, with a prepared NATS generation on the ZeBridge path.

## Local cold seed: ZeBridge, PowerSync, and Electric

To compare client seeding at the same scale without a cloud database path, I ran 10 clients per system against one local PostgreSQL 17 container in OrbStack. Each client started with an empty SQLite replica and the timer stopped after all 50,000 rows were present locally. The ZeBridge runner verified its replica; Electric checked both its TanStack collection and persisted SQLite rows; PowerSync's Node SDK waited for its first sync and reached 50,000 rows, then all ten SQLite files were independently checked. Every replica matched SHA-256 `f23eb95715da0c1c44811f949d27bb4bae36a8357b16244b67514f226ed574b7`.

| System and client path | Cold local seed median, rounds | Slowest client, rounds |
| --- | ---: | ---: |
| ZeBridge 0.2.11 / libzb / SQLite | **3.076 s / 2.959 s** | 3.094 s / 2.963 s |
| PowerSync Service 1.27.2 / Node SDK 1.3.0 / SQLite | 12.76 s / 12.99 s | 13.00 s / 13.14 s |
| Electric / TanStack DB / Node SQLite | 12.052 s / **11.481 s** | 18.548 s / 17.636 s |

Each system ran alone while Postgres and the fixture stayed up. PowerSync's two displayed rounds had a one-client readiness seed before timing the ten clients; a run immediately after restarting the service, without that stream warm-up, took 14.76 s median / 15.04 s slowest and logged initial connection retries. Electric's first round created its server-side shape; the second reused that shape. ZeBridge's generation chain was ready before timing. These are cold client stores with warmed server-side state, except Electric's first round. Across the readiness-warmed rounds, ZeBridge's median was about **4.2–4.4× lower than PowerSync** and **3.7–4.1× lower than Electric**.

This measures each product's tested SQLite client path, including transfer and local materialization. It is a seed benchmark, not a general throughput result. The clients and services shared an 8-core Mac host; OrbStack exposed about 4 GB to Docker. Electric's image was `latest` at digest `sha256:efb6fa43859d67cb8c73439e0c8bc0f7a3daa467500fb06f2a924bcb2070c139`. The local PowerSync fixture used a fixed `tenant_id = 'cmp'` stream predicate; this run does not compare per-user authorization. Tenant authorization differs between the deployed paths and should be evaluated separately.

## Writes: the same machine, then 29 ms away

ZeBridge 0.3.2 (admission and the bridge) and PowerSync service 1.28.3 (Sync Streams, its bucket storage in PostgreSQL on the host, five API processes, the Node SDK with no delay between uploads, writes through PostgREST), one at a time, with their clients, on one Linux machine (arm64, 9 cores, 7 GB). The same table rules for both: ZeBridge's triggers and tenant policy; PostgREST writes as ZeBridge's write role, so the policy applies (a mapped principal writes, an unmapped one is refused). Two databases: PostgreSQL 17 on the same machine (0.0 ms), then a new Supabase project in Dublin, 29 ms away. Measured 2026-11-13; three runs for each 1- and 10-client row, two for each crowd row below 5,000/s.

A ZeBridge write counts when its verdict returns, after admission, the receipt and the commit; a PowerSync write when PostgREST has committed it and the SDK has completed the upload. Clients with their own replicas, writing as fast as they can (ZeBridge 4 ingress lanes, PostgREST's default pool of 10):

| | ZeBridge, same machine | PowerSync, same machine | ZeBridge, 29 ms | PowerSync, 29 ms |
| --- | --- | --- | --- | --- |
| 1 client × 500 writes | 1,819–2,088/s, median 27–33 ms | **2,370–2,618/s, 6–7 ms** | 336–387/s, 0.58–0.75 s | **774–821/s, 0.36–0.39 s** |
| 10 clients × 1,000 writes | **6,971–8,578/s**, 0.18–0.30 s | 6,094–6,494/s, **30–36 ms** | 2,054–2,210/s, 2.2 s | **5,568–6,911/s, 0.35–0.71 s** |
| A write reaching another client, median | **53–54 ms** | 73–74 ms | **143–156 ms** | 212 ms |

The crowd: 500 light writers sending unordered inserts (ZeBridge: one NATS connection each, publishing writes with a `client_id` and no ordering envelope, so no write waits for another; PowerSync: apps uploading to PostgREST as `uploadData` does, up to 200 rows a request) offered a fixed rate for 30 s; committed is PostgreSQL's own row count, sampled every second. ZeBridge 8 lanes, PostgREST 30 connections:

| Offered | ZeBridge, same machine | PowerSync, same machine | ZeBridge, 29 ms | PowerSync, 29 ms |
| --- | --- | --- | --- | --- |
| 500/s | 498/s, median 9 ms | 503/s, 8 ms | **511/s, 184 ms** | 418–441/s, 3.5–4.5 s |
| 1,000/s | 1,009/s, 8 ms | 1,005/s, 8 ms | **1,021/s, 0.20–0.21 s** | 878–895/s, 3.8 s |
| 2,000/s | 2,012/s, 8 ms | 2,040/s, 8 ms | **1,932–2,001/s, 0.31–0.46 s** | 1,714–1,718/s, 4.0 s |
| 5,000/s | **5,000/s, 10 ms** | 4,942/s, 485 ms | 2,179/s, 17.5 s | **4,325/s, 4.2 s** |
| 10,000/s | **9,900/s, 16 ms** | 9,792/s, 415 ms | not run | not run |

Every write was committed on both sides. At 29 ms and 5,000/s offered, ZeBridge queued what it could not take: the run was given a 90-second drain, and the final PostgreSQL count was exact (209,985 rows, the 2,000/s and 5,000/s runs together). Its median covers the 120,477 verdicts received within the measurement; the other 29,512 writes were committed during the drain. At 29 ms, PostgREST answered `504` (pool timeout) 60 to 121 times a run, and the apps retried until each write landed.

**How to read it.**

- **The workload decides which comes out ahead.** At 29 ms, with 500 light writers offering 1,000 writes/s, ZeBridge's median verdict was 0.20–0.21 s against 3.8 s for PostgREST's acknowledgement; on the same setup, ten real clients draining bursts got 2,054–2,210 writes/s through ZeBridge and 5,568–6,911/s through PowerSync.
- **On the same machine the aggregate throughput is level:** both commit what is offered up to 10,000 writes/s (9,900 and 9,792/s at 10,000/s offered). The confirmations are not: from 5,000/s ZeBridge's median is 10–16 ms, PowerSync's acknowledgements 415–485 ms.
- **At 29 ms, for unordered inserts, ZeBridge's ceiling was about 2,000 writes/s with 8 lanes**, and below it ZeBridge confirmed 10 to 20 times sooner (0.2–0.5 s against about 4 s). All eight lanes were busy, each averaging about 4 ms a write, which gives roughly 2,000 writes/s across the bridge; database round-trip time was the main constraint in this run (SQL execution took about 85 ms of a lane's ~540 ms per batch; PostgreSQL waited on the bridge, with no lock contention). Ordered writes, the SDKs' default, also park and release and were not measured here. **Put the bridge as close to PostgreSQL as you can.**
- **PowerSync wins bursts from few clients at a distance**, 2 to 3 times: its SDK uploads up to 200 rows per request, one multi-row `INSERT`, where each ZeBridge write is judged on its own, with its receipt, its tenant's authorization and its verdict.
- **A write reaches other clients sooner with ZeBridge**, at either distance.

**What it does not measure.** PowerSync was given a ready backend (PostgREST) with the same rules, so these figures compare database and transport cost. They do not measure the work of building and running that backend, which is the structural difference between the two. PowerSync's write model is the familiar one: the app writes local SQL and your existing API applies it. ZeBridge's outbox and verdicts mean less code, but a model to learn. The evidence (raw outputs, the bridge's counters, `pg_stat_statements`) is kept with the harness's runs.

## Delivery to 500 clients

500 light clients starting cold, on one server of 6 vCPU and 12 GB, 23.6 ms from a Supabase database. A ZeBridge reader fetches the chain from NATS and follows CDC without decompressing or applying; a PowerSync reader opens the sync stream as the SDK does and decodes only its control messages. PowerSync runs its production layout (one replication process, one API process per 100 connections); ZeBridge runs one bridge. Measured 2026-11-02, three runs each.

| Sync, 500 clients starting cold | ZeBridge | PowerSync |
| --- | --- | --- |
| Time to receive the initial dataset, excluding local apply, median (three runs) | **5.3 / 5.7 / 5.3 s** | 61 / 76 / 65 s |
| Slowest client | **6.7–8.2 s** | 68–82 s |
| Sent to each client | **0.23 MB**, one compressed snapshot | 4.63 MB, 10,000 operations |
| One write reaching each of the 500, median | 0.4–0.8 s | 0.7–1.6 s |

**Delivery is where they differ most:** a ZeBridge client receives one compressed snapshot that every client shares, while each PowerSync client is sent every operation of its bucket, so the server's work grows with clients times rows.

## What a self-hosted PowerSync runs

PowerSync's documented production layout, as the crowd ran it:

| Part | What it does |
| --- | --- |
| One replication process (`start -r sync`) | reads the source database's change stream into bucket storage; only one may run |
| API processes (`start -r api`), one per 100 connections | serve the sync streams, 200 connections at most each; 1 GB and 1 vCPU each as a starting point |
| Bucket storage | a database of its own, a 3-node MongoDB replica set recommended, or PostgreSQL: the synced rows again, per bucket, and their operation log |
| A load balancer | in front of the API processes |
| A daily compact job | prunes the buckets' operation logs |
| Your backend API and your auth | apply every upload; sign the clients' JWTs |

PowerSync Cloud runs the service, its storage and the balancer for you. ZeBridge's hub, against the same clients, is four processes: NATS, the bridge, admission and the sweeper.

## Tenant access and write outcomes

Both systems can isolate tenants. In ZeBridge you designate the tenant column and map principals to tenants. NATS credentials restrict which private snapshots and change streams a device can read. On a write, the bridge sets the authenticated principal in the PostgreSQL transaction and row-level security checks the row. In PowerSync you express download access in Sync Streams using authenticated identity or membership data. Uploads go through your API, where you enforce write access, for example with Supabase RLS. [PowerSync: RLS and Sync Streams](https://docs.powersync.com/integrations/supabase/rls-and-sync-streams.md)

### What a disconnect does to a write

Suppose a phone submits an order, PostgreSQL commits it, and the reply is lost. ZeBridge commits the order and its receipt in the same transaction. The outbox resends the original write ID and payload. A matching retained receipt returns the original outcome without applying the order again. If the ID has expired and its receipt is gone, the bridge returns `indeterminate`. The client reconciles from the hub and never reissues the order under a fresh ID. [ZeBridge protocol](PROTOCOL.md#74c-reconciling-an-indeterminate-write)

PowerSync keeps an incomplete upload in its local queue and retries through the app's `uploadData()` connector. The backend can make that retry idempotent, for example by recording an operation ID with the order. The SDK leaves the endpoint and its response format to the application. [PowerSync: Writing Client Changes](https://docs.powersync.com/handling-writes/writing-client-changes.md)

### What the app learns when PostgreSQL refuses a write

If an order line violates a foreign key, ZeBridge reports `rejected` for that write ID with PostgreSQL's SQLSTATE. The client removes the entry from its send queue and reconciles the optimistic edit. Other writes receive their own verdicts. PowerSync also brings the replica back to server state once uploads complete and sync catches up. Its recommended pattern for a permanent rejection is to complete the upload and communicate the reason in the successful response or through a synced error table. The app chooses how to associate that reason with the edit. [ZeBridge protocol](PROTOCOL.md) · [PowerSync: Handling Write / Validation Errors](https://docs.powersync.com/handling-writes/handling-write-validation-errors.md)

In the write benchmark, PostgREST used the same writer role and tenant policy as ZeBridge. A ZeBridge confirmation included a durable per-write receipt. A PowerSync confirmation meant the connector completed its PostgREST upload. The throughput runs timed these two paths. They did not test a lost reply or audit isolation between tenants.
