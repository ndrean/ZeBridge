# ZeBridge compared with PowerSync

The measurements behind the README's [Compared with PowerSync](README.md#compared-with-powersync): the same workload, run by both systems against one PostgreSQL database. The architecture and what each asks you to run are in the README; how to run all of it again is in [the harness](scripts/compare/powersync/README.md).

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

## Same workload, two servers

These earlier measurements used Supabase in Dublin; they are retained as separate remote-database results and should not be compared directly with the local Compose measurement above.

Both against one Supabase database in Dublin, 10,000 rows of about 200 bytes, the same table setup for both (ZeBridge's triggers and tenant policy; PowerSync's writes went through PostgREST as ZeBridge's write role, so the policy applied). Clients on the server itself, one system running at a time. ZeBridge 0.2.10 (4 ingress lanes, rate limit off); PowerSync service 1.27.2 with its bucket storage in PostgreSQL on the same server, Sync Streams, the Node SDK 1.3.0 with no delay between uploads (`crudUploadThrottleMs: 0`; the default waits 1 s). Measured 2026-11-02: one run each, except the larger server's write rows, which give the median of three runs and their range.

| What | ZeBridge, 6 vCPU, 24.5 ms to the database | PowerSync, same | ZeBridge, 2 vCPU, 85 ms | PowerSync, same |
| --- | --- | --- | --- | --- |
| 10 clients seeding the table, empty / already seeded | **0.11 s / 0.03 s** | 5.93 s / 2.03 s | **0.35 s / 0.08 s** | 14.44 s / 6.82 s |
| One client's write reaching another, median / p95 | **120 / 133 ms** | 167 / 181 ms | **367 / 634 ms** | 487 / 1,277 ms |
| 100,000 rows inserted in one statement, until a client holds them all | **2.70 s** | 26.14 s | **3.74 s** | 33.40 s |
| Writes, 1 client | 395/s (380–401) | **708/s** (248–826) | **178/s** | 155/s |
| Writes, 10 clients | **1,691/s** (1,680–1,863) | 879/s (689–1,550) | **680/s** | 298/s |

**How to read it.** The gaps that hold on both servers are delivery to clients. Seeding is 40 to 70 times faster, because a ZeBridge client loads a compressed snapshot while a PowerSync client applies every operation in its bucket; the already-seeded column is an app restarting with its replica, the common case. The 100,000-row burst reaches a client about 10 times sooner, and one write reaches another client sooner, with a shorter tail. Writes are about equal in performance: PowerSync is faster with one client, ZeBridge with ten. The difference is in what a write gets: ZeBridge judges each write separately and returns a verdict for each, with exactly-once receipts and the tenant's authorization, where PowerSync uploads batches of up to 200 rows and leaves those guarantees to your backend. ZeBridge's write figures also repeat within about 10% from run to run; PowerSync's varied by a factor of two to three.

A ZeBridge write counts when its verdict returns, after admission, the receipt and the commit; a PowerSync write counts when the backend has committed it and the SDK has completed the upload. Most of a write's cost is the table's own rules, for both systems: the same 100,000-row insert took 4.37 s on a plain table and 10.16 s with ZeBridge's triggers (smaller server), and an earlier PowerSync run whose backend role bypassed the tenant policy reached 2,495 writes/s with 10 clients on the larger server, despite settings that worked against it (a 1 s upload delay, its bucket storage in Dublin). During each 10-client PowerSync run, 7 to 9 client sync streams broke ("illegal end of stream in BSON object") and reconnected; no write was lost.

**What it does not measure.** PowerSync was given a ready backend (PostgREST) with the same rules, so these figures compare database and transport cost. They do not measure the work of building and running that backend, which is the structural difference between the two. PowerSync's write model is the familiar one: the app writes local SQL and your existing API applies it. ZeBridge's outbox and verdicts mean less code, but a model to learn.

## A crowd of 500

The same comparison with 500 clients at once: one server of 6 vCPU and 12 GB, 23.6 ms from the same Supabase database (60 connections), with the services and the crowd on it. The clients are light: they do each system's wire work and nothing else, since 500 real clients do not fit on one host. A ZeBridge reader fetches the chain from NATS and follows CDC without decompressing or applying; a PowerSync reader opens the sync stream as the SDK does and decodes only its control messages. PowerSync runs its production layout (one replication process, one API process per 100 connections); ZeBridge runs one bridge. Measured 2026-11-02; sync three runs each, writes one.

| Sync, 500 clients starting cold | ZeBridge | PowerSync |
| --- | --- | --- |
| Time to receive the initial dataset, excluding local apply, median (three runs) | **5.3 / 5.7 / 5.3 s** | 61 / 76 / 65 s |
| Slowest client | **6.7–8.2 s** | 68–82 s |
| Sent to each client | **0.23 MB**, one compressed snapshot | 4.63 MB, 10,000 operations |
| One write reaching each of the 500, median | 0.4–0.8 s | 0.7–1.6 s |

| Writes, 500 clients, offered for 30 s | ZeBridge, 8 ingress lanes | PowerSync, PostgREST with 30 connections |
| --- | --- | --- |
| 500/s | 536/s committed, verdict median 0.34 s | 407/s, acknowledged median 7.4 s |
| 1,000/s | 990/s, median 0.69 s | 879/s, median 4.8 s |
| 2,000/s | 1,572/s, median 5.0 s | 1,702/s, median 6.4 s |

Every write was committed on both sides. Connections set the write figures, within the database's 60: with ZeBridge's default 4 lanes and PostgREST's default pool of 10, the rates at 2,000/s were 983/s and 1,353/s. Under the crowd, PostgREST timed out acquiring a connection 150 to 1,000 times a run (`504`), and the apps retried until each write landed. **So writes are equivalent at 500 clients:** ZeBridge answers sooner below its ceiling, PowerSync's batches of up to 200 rows go further above it. **Delivery is where they differ:** a ZeBridge client receives one compressed snapshot that every client shares, while each PowerSync client is sent every operation of its bucket, so the server's work grows with clients times rows.

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

## What a disconnect does to a write

With ZeBridge, a write sent again after a lost connection meets its receipt in PostgreSQL: it is applied once, and the client receives the verdict of the first attempt. A refused write gets its own verdict.

With PowerSync, the SDK keeps the write in its upload queue and calls `uploadData()` again after the reconnect; if the backend had committed it and only the reply was lost, the same operations arrive again, and PowerSync's documentation leaves it to the backend to detect them by the client's and the operation's IDs. A backend error makes the SDK retry the upload indefinitely, blocking the queue, so the documentation advises answering 2xx even for a refused write and reporting the refusal some other way, for example in a table synced back to the client.
